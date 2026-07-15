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

领域层不导入 FastAPI、SQLAlchemy、数据库 SDK、Langfuse 或模型 SDK。外部能力全部在组合根注入。

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

`EventPublisherPort`、`ObjectStorePort`、业务 Port 和审批 Port 已为阶段二 Kafka、S3、真实 API 与审批流保留。

## 5. MySQL Schema 与事务边界

首个显式 Alembic 迁移创建：

- 知识：`documents`、`document_versions`、`ingestion_tasks`、`chunks`、`chunk_acl`；
- 一致性：`outbox_events`；
- 写操作：`action_drafts`、`approval_requests`；
- 会话审计：`sessions`、`messages`、`agent_runs`、`agent_steps`、`tool_call_logs`、`audit_events`；
- FAQ：`faq_items`。

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

Embedding 维度和版本由 Milvus Schema 与写入双重检查；不兼容 Collection 会拒绝复用。
Neo4j 查询始终带 `tenant_id`，跳数硬限制为 1–2。

## 8. Agent 与 Tool

LangGraph 路径为 `Supervisor → 一个专家 → END`，因此图本身无无限循环。Supervisor 先用确定性规则处理
高风险和清晰意图，多意图冲突进入澄清。KB、FAQ、Escalation 为真实阶段一能力；Order、Logistics、
Refund 明确返回 Fake 来源。

Agent State 版本化、可序列化，并以 `{tenant}:{session}` 作为 LangGraph thread。生产使用 Redis Saver，
同时保存应用级 TTL Checkpoint。每个节点写 `agent_steps` 脱敏摘要。

所有实际业务/检索 Tool 调用经过 `ToolExecutor`：验证 Agent 与角色、拒绝未知/额外字段、执行超时、
生成 Span、写成功/失败审计。退款流程只做 `calculate → create_draft → pending approval`，幂等键不依赖 run ID，
代码中不存在真实退款 execute 路径。

## 9. API 与前端

所有业务端点位于 `/api/v1`。实现了 liveness/readiness、dev token、会话与历史、文档上传/列表/版本/下线、
任务查询/重试、非流式聊天、SSE、管理员检索调试和带 HMAC 的 Fake 审批回调。错误统一包含
`error_code`、`message`、`request_id`、`retryable`，不返回堆栈。

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

## 12. 构建、CI 与验收

- 后端：uv 冻结安装、Ruff、mypy、pytest/coverage、Golden Set、pip-audit。
- 前端：pnpm 冻结安装、ESLint/TypeScript、Vitest、Vite build、Playwright + axe。
- 真实依赖：Compose 使用 `neo4j:5.26.28-community`，并按 Milvus 2.6.14 官方 Standalone
  组合固定 etcd 3.5.25、MinIO 2024-12-18 和 Woodpecker MQ；Milvus 与 MinIO 通过环境变量注入
  同一组非默认凭据，执行迁移与 Adapter 集成测试。
- 镜像：后端与非 root Nginx 多阶段构建，Trivy Action 固定为 `v0.36.0`，阻断已修复的 Critical 漏洞。
- Secret：gitleaks。

Compose 默认只启动基础依赖；`--profile app` 加入迁移、后端和前端。阶段二 Kafka 不在 Compose 中。

## 13. 已知限制与阶段二入口

- 当前执行容器没有 Docker CLI，因此真实四依赖集成、镜像启动/扫描只能由已定义的 CI Job 补跑。
- 当前环境无法下载 Playwright Chromium，组件测试和生产构建已通过，浏览器 E2E/axe 由 CI 补跑。
- Golden Set 使用确定性合成数据和 Fake 模型；真实百炼只做受控评测，不能把离线延迟当生产指标。
- 阶段一事件写 Outbox，但没有 Kafka Relay；对象存储仍为本地目录。
- 没有真实订单、物流、退款、企微、邮件或审批执行 Adapter。

阶段二应只新增/替换 Adapter 与部署单元：Outbox Relay/Kafka/DLQ、S3、真实业务 API、审批恢复、
多租户配额和生产灾备；不得让 Agent 直接连接这些外部系统。
