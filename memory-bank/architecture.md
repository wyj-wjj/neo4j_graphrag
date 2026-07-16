# 当前真实架构

> 更新时间：2026-07-14。本文件只描述仓库当前行为；目标与阶段边界以
> `phase1_env_checklist.md` 为准。

## 1. 总体状态

阶段一已实现为 Python 模块化单体与 React SPA。默认 dev/test 使用确定性 Fake，能在无网络、
无收费模型的环境完成上传、入库、检索、引用式回答、FAQ、订单、物流、退款草单和人工升级流程。
生产组合使用 MySQL、Redis、Milvus、Neo4j、百炼兼容接口和可选 Langfuse。

MySQL 是知识、权限、版本、任务、Outbox、会话与审计的唯一真理源。Milvus 和 Neo4j 是可删除、
可重建的派生索引；Redis 只保存 Checkpoint、短状态、缓存、锁和限流计数。

## 2. 运行拓扑

```text
React/Nginx
    │ REST + SSE /api/v1
FastAPI 模块化单体
    ├── LangGraph Supervisor → FAQ / KB / Order / Logistics / Refund / Escalation
    ├── Ingestion Worker → Parser/OCR → Parent/Child Chunker
    ├── GraphRAG → Dense + Graph + Keyword → RRF → MySQL ACL → Rerank → Citation
    └── Port/Adapter
         ├── MySQL 8.4 / SQLAlchemy / Alembic / aiomysql
         ├── Redis 8.2 / LangGraph Redis Checkpointer
         ├── Milvus 2.6 / COSINE + HNSW
         ├── Neo4j 5.26.28 / tenant-scoped bounded traversal
         ├── Bailian OpenAI-compatible model, Embedding, OCR, Rerank
         └── LocalObjectStore（阶段二可替换 S3）
```

## 3. 代码边界

| 路径 | 当前职责 |
| --- | --- |
| `src/graphrag/api` | FastAPI 工厂、JWT/JWKS、REST、SSE、错误映射、请求 ID 和安全响应头 |
| `src/graphrag/application` | 运行时依赖组装、会话用例和资源生命周期 |
| `src/graphrag/domain` | 冻结领域模型、错误、ID、事件、Agent State 和所有 Port |
| `src/graphrag/agents` | 规则优先 Supervisor、LangGraph 图和七类专家节点 |
| `src/graphrag/retrieval` | Query Rewrite、并发召回、RRF、ACL、Rerank、证据与引用 |
| `src/graphrag/ingestion` | 上传校验、各格式解析/OCR、结构分块、任务状态与补偿 |
| `src/graphrag/tools` | 版本化 Registry，以及 Tool 权限、Schema、超时、Trace 和审计执行器 |
| `src/graphrag/infrastructure` | SQL、Redis、Milvus、Neo4j、对象存储、百炼和 Fake Adapter |
| `src/graphrag/observability` | JSON 日志、统一脱敏、Langfuse/Noop Trace |
| `frontend` | 对话、知识/版本、任务、检索调试工作台 |
| `tests` | 单元、SQLite 集成、可选真实依赖、安全、评测和 API 验收 |
| `synthetic-data` | 与生产包隔离的合成数据、独立 Oracle、HTTP 模拟器、上传/评分/审核/清理工具与 Fixture |

领域层不导入 FastAPI、SQLAlchemy、数据库 SDK、Langfuse 或模型 SDK。外部能力全部在组合根注入。

阶段 1.5 新增 `application/context.py`，集中负责 Token 估算、`context-policy-v1` 预算、滚动摘要、
结构化 Conversation State 和 Context Manifest；Agent 与检索模块不得自行拼接无预算 Prompt。
`application/memory_governance.py` 负责用户长期记忆治理，`application/safety.py` 提供统一 Safety Port，
`application/prompts.py` 校验版本化 Prompt 内容，`application/knowledge_quality.py` 只读检测知识问题。

## 4. Port 与 Adapter

| Port | 生产 Adapter | 测试/离线 Adapter |
| --- | --- | --- |
| KnowledgeRepository | `SQLKnowledgeRepository` | `InMemoryKnowledgeRepository` |
| VectorStore | `MilvusVectorStore` | `InMemoryVectorStore` |
| GraphStore | `Neo4jGraphStore` | `InMemoryGraphStore` |
| Checkpoint/Cache/Lock/Rate | `RedisAdapter` + `AsyncRedisSaver` | 内存实现 + `InMemorySaver` |
| Chat/Embedding/Entity | `OpenAICompatibleProvider` | `FakeModelProvider` |
| Reranker/OCR | `BailianReranker` / `BailianOCR` | Fake Provider / `FakeOCR` |
| Audit | `SQLAudit` | `InMemoryAudit` |
| Trace | `LangfuseTrace` | `NoopTrace` |
| Order/Logistics/Refund | 阶段一没有真实 Adapter | `FakeBusinessServices` |
| ObjectStore | `LocalObjectStore` | 同一实现，按租户隔离 |
| EventPublisher | 进程内幂等 Publisher；事件同时写 Outbox | 进程内 Publisher |
| LongTermMemory | `SQLLongTermMemoryStore` | `InMemoryLongTermMemoryStore` |
| Safety | 阶段 1.5 确定性规则实现；真实 Guard 预留 Adapter 边界 | `RuleBasedSafety` |
| Router | `DeterministicRouter`，可替换 Embedding/模型 Router | 同一实现 |
| ResultConsolidator | `DeterministicResultConsolidator` | 同一实现 |

`EventPublisherPort`、`ObjectStorePort`、业务 Port 和审批 Port 已为阶段二 Kafka、S3、真实 API 与审批流保留。

## 5. MySQL Schema 与事务边界

首个显式 Alembic 迁移创建：

- 知识：`documents`、`document_versions`、`ingestion_tasks`、`chunks`、`chunk_acl`；
- 一致性：`outbox_events`；
- 写操作：`action_drafts`、`approval_requests`；
- 会话审计：`sessions`、`messages`、`agent_runs`、`agent_steps`、`tool_call_logs`、`audit_events`；
- FAQ：`faq_items`。

阶段 1.5 的第二个迁移为会话一致性补充以下持久化字段和唯一约束：

- `sessions` 保存单调 `revision`、最后消息序号、当前活动运行和上下文策略版本；
- `messages` 保存 `client_turn_id`、`run_id`、会话内单调 `sequence`、状态与内容 hash；
- `agent_runs` 在 Agent 执行前以 `running` 状态创建，保存输入/结果 hash、结果快照、错误码及
  State、Checkpoint、Prompt、Context Policy 和恢复来源版本；
- 同一租户、会话和 `client_turn_id` 只能有一个 Agent Run；同一轮每种消息角色只能提交一次。

聊天提交边界现在分为 `begin_turn → Agent/Tool → complete_turn | abort_turn`。`begin_turn` 在单一
MySQL 事务中占用 Session、写入用户消息和运行记录；`complete_turn` 使用 Session revision 做 CAS
语义校验，在另一事务中原子写助手消息、结果快照和运行终态。相同输入的重复 `client_turn_id`
直接回放已完成结果；不同输入复用同一 ID 会被拒绝。同一会话最多一个活动运行，不同会话可并行。
内存 Adapter 实现同一契约并以会话粒度锁保证离线测试行为一致。

第三个迁移在 `sessions` 中保存版本化 Conversation State、摘要及覆盖序号，并新增
`context_manifests`。Manifest 只保存模型/策略版本、组件目标与实际 Token、被选择的消息/证据 ID、
借用预算和丢弃原因，不保存消息、证据、手机号或 Tool 原文。每个租户的每个 Run 只能有一份最终 Manifest。

第四个迁移新增 `safe_resume_snapshots`。它按租户和草单唯一保存审批中断的安全节点、原始运行/会话/
用户归属、已完成副作用名称、下一合法动作、State/Checkpoint 版本、过期时间和恢复结果 hash；不保存
原始退款问题或 Tool 载荷。相同决定重复回调直接回放，冲突决定返回 409。

第五个迁移新增 `user_memory_settings`、`long_term_memories` 和 `generation_manifests`。长期记忆按租户、
用户、键和版本隔离，保存确认来源、敏感等级、有效期、纠正链和删除状态；删除会擦除同一键的整条版本
链原值。Generation Manifest 将 Run 绑定到 Prompt bundle/hash、Chat/Embedding/Rerank 模型、Router、
State、Context Policy 和评测集版本。Agent Run 从开始时就记录实际 Prompt bundle 版本。

文档/版本/任务/received Outbox 在同一事务写入；Chunk/ACL/chunked Outbox 在同一事务写入。
`tenant_id` 出现在所有业务查询边界。文档 hash、版本号、Chunk 序号、事件 ID 和草单幂等键有唯一约束。
迁移已在 SQLite 执行 `upgrade → schema diff → downgrade → upgrade`；真实 MySQL 流程在 CI 的
`real-integration` Job 中执行。

因 `asyncmy 0.2.11` 存在无修复版本的 SQL 注入公告，驱动已切换为 `aiomysql`，见
`docs/adr/0001-mysql-async-driver.md`。

## 6. 入库与版本生命周期

1. API 校验角色、扩展名、MIME/文件签名、大小和限流。
2. 对象以 `{tenant}/{document}/{content_hash前缀}/original.ext` 保存，路径穿越被拒绝。
3. MySQL 原子写文档/版本/任务/Outbox；API 立即返回任务 ID。
4. Worker 以 claim 方式领取任务，解析 PDF、DOCX、XLSX、PPTX、HTML、TXT、Markdown 或图片 OCR。
5. 结构分块生成 parent/child Chunk；只有 child 写入向量与图谱。
6. Milvus 与 Neo4j 并发写入，分别记录成功、失败和错误；部分成功可从已有 Chunk 恢复。
7. 两个派生索引都成功后激活新版本，并使旧版本及旧 Chunk 失效。
8. 下线先在 MySQL fail-closed，再尽力清理 Milvus/Neo4j；清理失败不恢复已下线内容。

稳定 ID、`content_hash`、版本号和独立索引状态使部分失败可检测、可重试、可重建。

## 7. GraphRAG 检索

Dense、实体图谱和关键词分支并发执行。单个派生分支故障可降级；全部召回不可用返回依赖错误。
不同召回器的原始分数不互比，使用 RRF `k=60`。候选随后必须由 MySQL 复核租户、ACL、状态、
版本和有效期；权限复核异常转换成 `authorization` 依赖错误并拒绝返回任何知识。

Rerank 后采用绝对阈值 `0.08` 与相对最佳分数 `0.80` 的较严格者，最终最多保留 5 条。
父 Chunk 也重新授权后才可扩展上下文。证据保留 Chunk、文档、版本、更新时间和来源位置。
无可靠证据返回标准拒答；有证据回答追加稳定 `[C1]` 引用。文档正文被视为数据，不能覆盖系统权限与 Tool 规则。
同名且同时有效的证据若内容不一致，会在生成前被标记为冲突并拒绝给出确定结论；管理员质量报告可定位
冲突、重复、过期和低质量 Chunk，但不会自动删除或发布业务内容。结构化回答额外保存开放问题、草单动作
和拒答原因，Safety Validator 在提交前检查企业引用、Fake 标记、重复引用和疑似密钥泄露。

Embedding 维度和版本由 Milvus Schema 与写入双重检查；不兼容 Collection 会拒绝复用。在线向量检索与
补偿核对使用 Strong consistency，保证写入确认并在 MySQL 激活后立即可见。
Neo4j 查询始终带 `tenant_id`，跳数硬限制为 1–2。

阶段 1.5 后，Query Rewrite 和生成模型均接收经过统一预算筛选的会话历史。RAG 不再以固定字符数
决定模型证据上下文；候选证据由 Context Assembler 按 Token 预算裁剪，只有实际进入模型的证据才
保留为最终 Citation。证据区域带 `UNTRUSTED_EVIDENCE_DATA_ONLY` 信任域标记。

## 8. Agent 与 Tool

LangGraph 路径为 `Supervisor → 一个专家 → END`，因此图本身无无限循环。Supervisor 先用确定性规则处理
高风险和清晰意图，多意图冲突进入澄清。KB、FAQ、Escalation 为真实阶段一能力；Order、Logistics、
Refund 明确返回 Fake 来源。

路由现实现版本化 `RouterPort`，输出严格候选、置信度、理由和降级字段；同时提供最多两个专家的
`bounded-plan-v1` 复合意图计划接口。`ResultConsolidatorPort` 与 `deterministic-consolidator-v1`
提供最多两个专家结果的确定性收敛契约：先比较业务定义的来源权威等级，同级且时间可比时选择更新证据；
同级冲突无法确定性裁决时必须澄清，不能比较模型自报置信度猜测答案。仲裁结果保存版本和
`single/authority/recency/conflict/combined` 依据；权威或时效落选结果的引用不会混入最终答案。
阶段 1.5 仍保持单专家执行，歧义复合意图进入澄清；该接口只为阶段二预留稳定领域契约，不会为展示
“多 Agent”而执行未经真实评测的自由协作。

Agent State 当前版本为 v2，以 `{tenant}:{session}` 作为 LangGraph thread，并使用
`agent-state-v2` namespace。生产使用 Redis Saver，同时保存应用级 TTL Checkpoint。Checkpoint 读取统一
经过 `StateMigrationRegistry`：固定 v1 夹具会补齐 Conversation State、Context Manifest 和审批字段后升级
到 v2；未知未来版本、损坏 JSON、缺少必填身份字段或迁移不前进均拒绝恢复。每个节点写
`agent_steps` 脱敏摘要。

Agent Run 开始时先把当前用户输入合入结构化 Conversation State，再根据 `context-policy-v1` 组装初始
上下文。默认输入预算在预留输出 Token 后按最近原始消息 30%、摘要/结构状态 20%、外部证据 40%、
固定安全/身份/当前问题 10% 分配。比例是可版本化起点；不可裁剪的安全上下文和结构化硬约束可借用
其他组件预算。无证据的 FAQ/澄清不会为了填满 40% 注入无关内容。

工作记忆包含两层：滚动叙事摘要只压缩已覆盖的较早消息；金额、地区、期限、否定和审批状态保存为
带来源消息与序号的结构化约束。已进入摘要覆盖范围的原始消息不会重复注入模型。当前 Token 估算器
对 CJK 逐字符、其他文本按四字符估算；它是无外部依赖的保守基线，后续真实模型评测可替换为模型
Tokenizer Adapter，而不改变领域状态或持久化契约。

所有实际业务/检索 Tool 调用经过 `ToolExecutor`。Registry 同时保存真实的版本化 Pydantic 输入模型与
运行时输出类型；输入和返回值均在 Agent State 之外验证，非法输出不能进入状态或用户响应。执行器为每个
依赖设置独立并发 Semaphore、总超时预算和连续故障熔断状态；只有声明为幂等且返回瞬时错误的调用可在
总预算内有限重试，写 Tool 还必须具有幂等键。取消不被转换或吞掉，会记录 `cancelled` 审计后向上传播。
超时、熔断和依赖错误使用稳定错误码，Agent 节点将其转为明确的安全停止/人工升级终态；审计只记录输入
键名、次数、依赖和错误类型，不记录业务载荷。退款流程只做
`calculate → create_draft → pending approval`，幂等键不依赖 run ID，代码中不存在真实退款 execute 路径。

退款节点现为 `refund → approval_interrupt → END`。第一次运行在草单落库后触发真实 LangGraph
`interrupt()`，API 返回 Fake 待审批结果；签名回调使用 `Command(resume=...)` 从同一 thread 恢复。
若 LangGraph/Redis Checkpoint 丢失，则只根据 MySQL 安全快照生成确定性的批准/拒绝终态，标记
`recovery_source=mysql_snapshot`，不会重跑 `calculate`、`create_draft`，更不存在真实 `execute`。

## 9. API 与前端

所有业务端点位于 `/api/v1`。实现了 liveness/readiness、dev token、会话与历史、文档上传/列表/版本/下线、
任务查询/重试、非流式聊天、SSE、管理员检索调试和带 HMAC 的 Fake 审批回调。错误统一包含
`error_code`、`message`、`request_id`、`retryable`，不返回堆栈。

长期记忆 API 提供显式确认创建、查看、纠正、禁用和删除；没有任何聊天节点会自动提取或写入长期记忆，
`LONG_TERM_MEMORY_AUTO_WRITE_ENABLED=true` 在配置加载时直接失败。当前不建立长期记忆 Redis 缓存或语义
派生索引，因此删除提交 MySQL 后不存在额外副本；未来新增派生索引必须扩展同一删除契约。管理员还可调用
只读知识质量报告接口。

聊天请求包含可由客户端提供的 `client_turn_id`；未提供时由 API 生成。响应返回该 ID。API 在调用
Orchestrator 前完成 Turn Claim，并将既有历史快照、预先分配的 `run_id` 一同传入 Agent。运行失败、
超时或取消会写确定终态并释放会话占用，不会提交半截助手消息。

SSE 不再等待完整答案后按字符切片。RAG 的流式与非流式路径都消费 `ChatModelPort.stream()`，使用同一
答案/引用聚合器；流式请求通过 `RunEventEmitter` 把 Provider delta、路由、检索、引用、状态和终态放入
同一个 `RunEvent v1` 队列，并在整个 Run 内分配唯一单调序号。会话只有在 Agent 最终结果完整形成后才
提交 assistant 消息；客户端断开或生成器取消会取消下游任务并执行 `abort_turn`。非生成型 Agent 只发送
一个完整业务结果 delta，不伪装成模型流。成功与失败路径都只发送一个 `end`。

OpenAPI 固化在 `openapi.json`，前端 DTO 由它生成。React 使用路由懒加载，提供对话流、引用和答案状态、
知识上传与版本列表、任务刷新/重试、管理员调试及全局错误边界。Nginx 关闭 SSE 缓冲并回退 SPA 路由。

## 10. Redis Key 与 TTL

| Key/前缀 | 用途 | TTL/策略 |
| --- | --- | --- |
| `agent:{tenant}:{session}:state` | 应用级序列化 Agent State | `CHECKPOINT_TTL_SECONDS` |
| `agent_checkpoint*` | LangGraph Checkpoint | 同配置，读取可刷新 |
| `agent_checkpoint_write*` | LangGraph pending writes | 同配置 |
| `rate:{tenant}:{user}:{category}` | chat/upload 限流 | 60 秒窗口 |
| 调用方提供的 cache key | 只读缓存 | 必须显式 TTL |
| 调用方提供的 lock key | 所有者租约锁 | 必须显式 lease，Lua 校验所有者释放 |

## 11. 安全与可观测性

- dev/test 才能签发 HS256 本地 Token；staging/prod 强制非对称 JWKS、真实 Adapter、非 root MySQL。
- 上传做扩展名、MIME、内容签名、大小、页数和 OCR 超时限制。
- 日志、Trace、Agent Step、Tool Log 与错误统一脱敏，不保存完整 Tool 输入。
- 每个 HTTP 请求有 request ID，每个运行有 run ID；API、Agent、召回、Rerank、模型和 Tool 在嵌套 Span 中。
- ACL 默认拒绝，deny 优先；跨租户、跨用户会话和普通用户调试均有自动化测试。
- `.env`、上传数据、缓存、浏览器产物和附件来源目录均被忽略。
- 用户输入、外部证据、已验证 Tool 输出和模型输出具有独立 Trust Domain。Prompt Injection 只会作为
  不可信数据进入既有确定性规则；Safety 审计记录策略版本和 flags，不记录原文。
- 核心 Prompt 位于 `src/graphrag/prompts`；manifest 中的 SHA-256 与内容不符时应用拒绝启动，支持通过
  版本化文件和清单回滚，不在代码多处维护隐式 Prompt。

## 12. 构建、CI 与验收

- 后端：uv 冻结安装、Ruff、mypy、pytest/coverage、Golden Set、pip-audit。
- 可靠性：CI 额外运行 `memory-reliability-v1`，覆盖 4/8/12/16 轮约束、预算、幂等、状态迁移与隔离，
  报告绑定 Prompt/模型/Router/State/Context Policy 版本并明确标记为 Fake 本地指标。
- 前端：pnpm 冻结安装、ESLint/TypeScript、Vitest、Vite build、Playwright + axe。
- 真实依赖：Compose 使用 `neo4j:5.26.28-community`，并按 Milvus 2.6.14 官方 Standalone
  组合固定 etcd 3.5.25、MinIO 2024-12-18 和 Woodpecker MQ；Milvus 与 MinIO 通过环境变量注入
  同一组非默认凭据，执行迁移与 Adapter 集成测试。阶段二 `phase2` Profile 另起隔离的 MinIO 服务和
  Volume 作为知识原文件对象存储，不复用 Milvus 内部 Bucket；真实集成 Job 增加 S3 保存、读取、租户
  前缀、幂等删除测试。
- 镜像：后端与非 root Nginx 多阶段构建，Trivy Action 固定为 `v0.36.0`，阻断已修复的 Critical 漏洞。
- Secret：gitleaks。

Compose 默认只启动基础依赖；`--profile app` 加入隔离对象存储、Bucket 引导、迁移、后端和前端。
`--profile phase2` 加入固定版本的单节点 Kafka KRaft Broker、向量 Worker 和图 Worker。该 Broker 使用本地
PLAINTEXT、单副本，只用于开发验收，不是生产 Kafka 拓扑或安全配置。

## 13. 已知限制与阶段二入口

- 当前执行容器没有 Docker CLI；GitHub CI 已通过真实 MySQL、Redis、Milvus、Neo4j 集成、镜像启动和 Trivy 扫描。
- 当前环境无法下载 Playwright Chromium；GitHub CI 已通过完整工作台 E2E 和逐页 axe 检查。
- Golden Set 使用确定性合成数据和 Fake 模型；真实百炼只做受控评测，不能把离线延迟当生产指标。
- Outbox Relay、Kafka Publisher/Consumer、Inbox/DLQ、索引 Handler 与独立 Worker 入口已实现并通过本地契约/SQL 测试；
  当前环境没有 Broker，真实 Kafka 多实例竞争、重启、Rebalance 与 DLQ 重放仍待自托管 Runner 验证。
- S3 兼容对象存储已实现；本地 Compose/CI 新增隔离 MinIO，但本窗口无法实际启动 Docker 复验。
- 合成业务 HTTP Adapter 已实现且强制校验 Fake Envelope；没有真实订单、物流、退款、企微、邮件或审批
  执行 Adapter。staging/prod 配置会因缺少真实业务契约 Adapter 而 fail-closed。
- 阶段 1.5 里程碑 A–E 已完成。当前长期记忆只提供显式用户治理，不自动写入或注入 Prompt；真实 Guard、
  字段级 KMS、长期记忆语义索引和模型 Router 需要真实数据、隐私策略与评测后再启用。仅“订单只读查询 +
  物流只读查询”的确定性双专家已开放；写意图、第三意图和不明确组合继续澄清或走单一状态机。

完成阶段 1.5 验收后，阶段二应只新增/替换 Adapter 与部署单元：Outbox Relay/Kafka/DLQ、S3、
真实业务 API、生产审批、多租户配额和生产灾备；不得让 Agent 直接连接这些外部系统。

## 14. 阶段二合成数据工厂（当前可运行基线）

根目录 `synthetic-data/` 是独立的 Python 3.12 子项目，拥有自己的 `pyproject.toml`、`uv.lock`、Schema、
Profile、CLI 和测试。生产包 `src/graphrag` 不导入该项目；生成器不读取 `.env`，不连接 MySQL、Redis、
Milvus、Neo4j、收费模型或真实业务端点。

当前实现 `graphrag-data-factory-v3`：

- 固定 `phase2-data-spec-v1`、`synthetic-commerce-v1`、根 Seed 和 UTC 基准时间，按领域派生 Seed，
  使用稳定测试 ID、Decimal 金额、稳定排序和规范 JSONL 序列化；
- 生成租户、用户/角色、权限判定、商品、订单/物流独立 Oracle、退款/审批生命周期、知识源、长会话、
  2,000 条 Silver Case、5,000 条 Security Case、500 条 Memory Case、300 条 Golden Candidate、
  故障计划、正常事件及重复/乱序/毒消息/未知 Schema 投递；
- Memory Case 固定 `context-policy-v1` 的 32,768 Token 示例窗口和 30/20/40/10 初始评测预算；Silver
  Case 绑定 Prompt、Fake 模型、Router、State、Context、Tool、Event、Oracle 与 Dataset 版本；
- 生成 Markdown、TXT、HTML、PDF、DOCX、XLSX、PPTX、PNG、JPEG 的正常、边界和损坏样本。Office
  ZIP 元数据固定并规范重打包；独立解析器验证成功样本与损坏拒绝，不调用生产 Parser 计算预期；
- 先写同目录临时树，完成文件清单、数量、SHA-256、Schema、金额、时间、关系、租户、审批、Split、
  事件重放、物理文件解析和敏感信息门禁后才原子发布；替换时只接受带合成所有权 Manifest 的目录；
- 独立 FastAPI 业务模拟器从已验证 Manifest 加载事实，配置类型不能表达生产环境且 CLI 只能绑定回环地址。
  身份 Header 不能提升角色；查询、改址/催单/退款草单、审批回调、幂等冲突、Trace、审计和受保护故障模式
  使用统一 Fake Envelope；
- 正式知识上传客户端只调用应用 `/api/v1/documents` 和任务查询 API，Token 必须来自显式文件，不读取环境。
  已对根项目真实 FastAPI 工厂的 Test/Fake 组合执行 18 个有效样本：18 个任务全部完成，生成 18 个文档、
  18 个版本和 199 个 Chunk，14 个可解析文档的 Evidence Anchor 全部进入 Chunk。该验收使用内存真理源与
  Fake 派生索引，不等于已验证 MySQL/Milvus/Neo4j；
- 行为评分核对意图、Agent、Tool 顺序、Evidence Anchor、答案状态、副作用和租户隔离；检索评分提供
  Recall@K、MRR 和 NDCG。Golden Candidate 只有在两个不同审核人对当前候选指纹均批准后才能生成
  Golden Record，生成器本身不能晋升；
- `dev-standard` 与 `failure-lab` 通过规范 JSONL 批次生产器生成；每批数据和检查点都执行 fsync。检查点
  绑定 dataset ID 与 Profile SHA-256，记录已完成文件和活动文件的记录数、字节偏移与前缀 Hash。恢复必须
  显式 `--resume`，先验证已提交前缀和完成文件，随后只截断未提交尾部；部分数据永不发布到正式目标；
- 大 Profile 验证不把百万记录加载为 Python 列表，而是在临时 SQLite 建身份、事实、Anchor、Split、事件、
  Inbox、聚合终态、投递和 DLQ 唯一性索引。验证器逐行执行 Pydantic、租户、关系、权限、金额、时间、
  事件顺序、去重、终态 Hash、文件清单/校验和和敏感模式检查，通过后才删除检查点并原子发布；
- `estimate` 提供保守资源规划。大 Profile 会依次校验 `--allow-large`、Profile/估算字节精确确认和专用
  `generated/` 路径。CLI 只开放已实测的 `dev-standard`/`failure-lab`；`staging-large` 的百万订单核心事实
  尚未磁盘化，因此继续硬停止且没有绕过开关。清理默认 dry-run，执行前使用磁盘型验证器重新验证 Manifest
  并要求完整 dataset ID；
- `fixtures/ci-small/` 当前包含 18,992 条结构化记录和 103 个文件（Manifest 管理 102 个），约 14 MB；
  其中 986 条重放期望覆盖数据集及每个事件聚合，200 条 DLQ 修复记录绑定原始 Hash、操作者和审计 ID。
  已提交历史 Fixture 仍为兼容的 v2，Manifest 摘要
  `fe860fa31c1d49e406246f2bf46b25afa7338b0bd7f20a9e3b507ccdf38e489b`。v3 将 Office ZIP 的创建/修改
  时间固定，跨秒重复生成逐字节一致；全新 v3 `ci-small` 的预期摘要为
  `a6e48bc37562ae4cc499c8d8de803a363e138cc3fd2efcde8ecdfb6175eb5ee4`。验证器只兼容 v2/v3，未知版本
  拒绝。公共契约导出为 39 份 JSON Schema；独立 CI 执行格式、Ruff、mypy strict、pytest/coverage 和
  已提交 Fixture 验证；
- `failure-lab` 已物化 1,380,226 条记录、326 个文件和 1,155,538,493 字节，实测生成+全量验证
  223.850 秒、峰值 342,696 KiB；`dev-standard` 已物化 3,372,108 条记录、1,226 个文件和
  2,842,845,571 字节，批次派生优化后实测 570.486 秒、峰值 1,447,980 KiB。大产物位于 Git 忽略的
  `synthetic-data/generated/synthetic-commerce-v1/`，指标只代表合成生成器，不是生产 SLO。
- 两个已物化大 Profile 由独立 GitHub Actions 工作流重新生成并校验固定 Manifest/Archive SHA-256，随后
  发布到不可变 Release 标签 `synthetic-commerce-v1-data-v3`。`fetch-release` 命令固定校验标签内资产、
  安全解压、执行完整磁盘验证并原子安装；发布归档为 `dev-standard` 445,969,395 字节（SHA-256
  `4bcbc5f6…a61237`）和 `failure-lab` 197,895,810 字节（SHA-256 `877e7b22…b01ffd`），大文件本身仍不进入
  Git 历史。

尚未完成：在真实 MySQL/Milvus/Neo4j 组合复验上传与 Anchor Map；真实 Kafka Broker 下的 Relay/消费者/
DLQ 修复重放验收；
`staging-large` 的磁盘型核心事实和物化；真实依赖加载和全链路 GraphRAG 评测。当前事件能力是
当前离线 Oracle 与已实现的 Kafka Adapter 都不等于 Kafka 已部署。动态订单、物流和退款事实不得写入
RAG 或长期记忆。

## 15. 阶段二首批生产能力实现（2026-07-15）

- `EventEnvelope` 区分 Schema `event_version` 与业务顺序 `aggregate_version`。Outbox 增加可用时间、租约、
  最后错误和 Broker 确认时间；同一租户/聚合只领取最早未发布事件。Kafka Key 固定为
  `tenant_id:aggregate_id`，Topic 只由受控事件类型映射，Producer 使用幂等和 `acks=all`，只有 delivery
  acknowledgement 后才完成 Outbox。
- Inbox 按 `(consumer_name,event_id)` 唯一；同 ID 不同 Hash 拒绝，处理中租约可恢复，已处理重复不产生
  副作用，较旧 `aggregate_version` 标为 stale。未知 Envelope、未知版本、永久失败和耗尽重试进入 DLQ；
  原始载荷以受限大小 Base64 保存，DLQ 修复表绑定原始/修复 Hash、操作者、原因、目标版本和审计 ID。
- Kafka Consumer 禁用自动提交和自动 offset store；processed/duplicate/stale/DLQ 才同步提交 offset，busy/
  retry 回退到原 offset。Embedding/KG Handler 只从 MySQL 重新读取版本与 child Chunk，验证租户、文档和
  聚合版本后幂等 upsert Milvus/Neo4j；外部副作用成功但 Inbox 未完成时可安全重投。
- 对象存储由 `ObjectStoreBackend` 选择 Local 或 S3。S3 Key 固定加租户物理前缀，写入租户与 SHA-256
  Metadata，读取强制复核，支持 Path Style、TLS、IAM/静态凭据、SSE-S3/KMS 和幂等删除；staging/prod
  禁止 Local 和关闭 TLS 校验。
- 合成业务 HTTP Adapter 只在 dev/test 使用 `x-synthetic-*` 身份头，严格校验 Envelope、Trace、租户、
  用户与响应 Schema；只读可有限重试，写草单不自动重试，草单仍落 MySQL。真实业务契约缺失时
  staging/prod 明确拒绝启动。
- `bounded-plan-v1` 首次接入执行链，仅开放订单+物流两个只读专家。Router 必须恰好命中两个意图且两步
  都是只读；确定性互补收敛固定顺序和最多两个结果。写意图不会进入 Compound。
