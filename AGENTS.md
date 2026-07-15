# AGENTS.md

## 适用范围

本文件适用于仓库根目录及其全部子目录。若未来某个子目录存在更具体的 `AGENTS.md`，则子目录文件可补充或覆盖本文件中与该目录相关的规则。

## 项目概述

本项目是一个面向企业智能客服场景的多智能体 GraphRAG 系统，核心目标包括：

- 使用 LangGraph 编排 Supervisor、KB、FAQ、Escalation 等专家 Agent。
- 使用 MySQL 作为知识、状态、权限与审计的唯一真理源。
- 使用 Milvus 进行向量召回。
- 使用 Neo4j 进行实体检索和有限跳数的图谱扩展。
- 使用 Redis 保存 LangGraph Checkpoint、会话短状态、缓存和分布式锁。
- 通过阿里云百炼的 OpenAI 兼容接口调用聊天模型、Embedding、OCR 和 Rerank 模型。
- 阶段一交付模块化单体 MVP，同时预留阶段二 Kafka、对象存储、审批流、真实业务 API 和多租户能力。

## 权威文档与阅读顺序

开始设计、编码、重构或审查前，按以下顺序阅读并核对：

1. `memory-bank/architecture.md`
2. `memory-bank/phase1_env_checklist.md`（本项目的权威设计文档）
3. `memory-bank/tech-stack.md`
4. `memory-bank/implementation-plan.md`
5. `memory-bank/phase1_5-hardening-plan.md`（阶段二前的健壮性加固基线）
6. `enterprise_multi_agent_architecture.md`
7. 当前任务涉及目录中的 README、Schema、迁移和测试文件

如文档与代码不一致：

- 不要静默选择其中一方。
- 先以自动化测试和当前可运行行为确认事实。
- 明确记录不一致及处理依据。
- 完成功能时同步更新对应文档。

## 阶段一实施边界

阶段一必须实现：

- 文档上传、解析、结构感知分块和入库任务状态。
- MySQL、Milvus、Neo4j 三库协同写入和失败补偿。
- 向量召回、实体/图谱召回、融合、MySQL 权限复核和引用式回答。
- Supervisor、KB、FAQ、Escalation 四个可运行 Agent。
- Order、Logistics、Refund 的稳定 Tool/API 契约和 Fake Adapter。
- Redis Checkpoint、会话恢复、基础缓存和锁。
- Langfuse Trace、结构化日志和敏感信息脱敏。
- 单元测试、集成测试、端到端测试和最小 Golden Set。

阶段一不执行：

- 真实退款、真实改地址或其他高风险业务写操作。
- 真实企微或邮件审批。
- 生产 Kafka 集群和异步 Worker 部署。
- 无实际收益的微服务拆分。
- 生产级多活、集群化和复杂灾备。

虽然不部署上述能力，相关端口、数据契约、事件 Envelope、幂等字段和状态字段必须在阶段一预留。

## 技术基线

以 `tech-stack.md` 为完整依据，核心基线为：

- Python 3.12
- uv + `pyproject.toml` + `uv.lock`
- FastAPI + Pydantic 2
- LangGraph 1.x
- SQLAlchemy 2 + Alembic + aiomysql（asyncmy 因未修复的 CVE-2025-65896 被替换）
- MySQL 8.0+；新部署优先 8.4 LTS
- Milvus 2.6.x，Server 与 PyMilvus 匹配
- Neo4j 5.26 LTS + 官方 Python Driver
- Redis 8.2.x + `langgraph-checkpoint-redis`
- OpenAI Python SDK调用百炼兼容接口
- Langfuse 4.x
- React 19.2 + TypeScript 5.9 + Vite 8
- Ant Design 6 + TanStack Query 5
- pytest、Playwright、Testcontainers、Ruff、mypy

不得仅因某个依赖发布新版本就升级。依赖、运行时、数据库和镜像升级必须说明原因，更新锁文件，并通过相关测试和评测。

## 架构规则

### 模块化单体

阶段一采用模块化单体。推荐边界：

```text
api/
application/
domain/
agents/
retrieval/
ingestion/
tools/
infrastructure/
observability/
```

领域层不得依赖 FastAPI、SQLAlchemy、Milvus、Neo4j、Redis、Langfuse 或具体模型 SDK。

### Port 与 Adapter

以下外部能力必须通过 Port 隔离：

- `ChatModelPort`
- `EmbeddingPort`
- `RerankerPort`
- `KnowledgeRepositoryPort`
- `VectorStorePort`
- `GraphStorePort`
- `CheckpointStorePort`
- `ObjectStorePort`
- `EventPublisherPort`
- `ApprovalPort`
- `OrderServicePort`
- `LogisticsServicePort`
- `RefundServicePort`
- `AuthorizationPort`
- `AuditPort`
- `TracePort`

禁止在 Agent 节点内直接创建数据库连接、HTTP Client 或第三方 SDK Client。

### Agent 规则

- Supervisor 负责语义路由和有限仲裁，不承担权限、事务、金额或风控规则。
- 循环上限、超时、重试、权限、错误码路由等确定性逻辑必须由代码控制。
- Agent 只获得当前任务所需 Tool 子集。
- 所有 Tool 输入输出都必须使用版本化 Pydantic Schema。
- 任何不符合 Schema 的模型输出不得直接进入业务服务。
- Agent State 必须显式、可版本化、可序列化、可恢复。
- 状态字段必须明确归属和合并策略，禁止多个节点随意覆盖同一字段。

### 数据边界

- MySQL 是唯一真理源。
- Milvus 和 Neo4j 是可重建派生索引。
- Redis 只保存短期状态、Checkpoint、缓存和锁，长期审计状态需归档 MySQL。
- Agent 不直接访问数据库，只调用应用服务或 Tool。
- 所有业务数据从阶段一开始携带 `tenant_id`；单租户使用固定默认租户。
- Neo4j 扩展出来的 Chunk 必须重新经过 MySQL ACL、状态、版本和有效期校验。
- 权限依赖不可用时必须拒绝返回内容，不得降级绕过权限。

## GraphRAG 规则

- Dense 向量召回、图谱实体召回可以并发执行。
- 不直接比较不同召回器的原始分数，使用 RRF 或明确校准过的融合算法。
- 召回候选必须在 MySQL 中做最终权限和有效性复核。
- 进入生成模型的证据必须保留 `chunk_id`、文档版本、更新时间和来源。
- 知识类答案必须提供引用。
- 没有可靠证据时明确拒答，禁止使用模型参数知识补充企业事实。
- 图谱查询必须带租户范围和跳数上限，禁止无界路径查询。
- Embedding 模型或维度变化时创建新 Collection 并重建索引，禁止新旧向量混写。

## 入库与一致性规则

- 文档、Chunk、状态和 Outbox 事件在 MySQL 中建立明确事务边界。
- 每个文档、Chunk 和事件都使用稳定全局 ID。
- 使用 `content_hash`、`event_id` 和版本字段实现幂等。
- 阶段一同步写 Milvus/Neo4j 也必须记录 `vector_status`、`graph_status` 和失败原因。
- 任何部分成功都必须可检测、可重试、可重建，不允许只打印异常后丢失状态。
- 删除优先采用 MySQL 状态失效和派生索引异步清理。
- 文档重新解析或模型变更不得静默覆盖不可追溯版本。

## Tool 与写操作规则

每个 Tool 必须定义：

- 全局唯一且带版本的名称
- 输入与输出 Schema
- 权限策略
- 风险等级
- 超时和重试策略
- 结构化错误码
- 审计策略
- 写操作幂等键

阶段一的 Order、Logistics、Refund 使用 Fake Adapter，但 Fake 与未来真实 Adapter 必须实现同一 Port，并在响应中明确标记数据来源。

退款等高风险操作必须拆分为：

```text
calculate → create_draft → approval_interrupt → execute
```

Agent 只能生成草单。真实提交必须在审批通过、幂等校验和服务端风控通过后执行。

## API 规则

- 对外 API 统一使用 `/api/v1` 前缀。
- 使用 REST 处理资源与命令，使用 SSE 处理单向流式回答。
- 错误响应必须包含稳定 `error_code` 和 `request_id`，不得把内部堆栈返回客户端。
- API Schema 与 ORM Model 分开。
- OpenAPI 是前后端契约来源，前端类型从 OpenAPI 生成。
- 所有外部请求设置连接、读取和总超时。
- 重试仅用于幂等操作；禁止无条件重试写操作。
- 健康检查区分 liveness 和 readiness。

## 配置与安全

- 只提交 `.env.example`，禁止提交 `.env`、API Key、密码、Token 或真实用户数据。
- 代码和文档不得包含可用的默认密码。
- 应用不得使用 MySQL `root` 账号。
- 生产环境不得启用 Fake Client。
- 文件上传必须校验大小、MIME、扩展名和内容，并限制解析/OCR超时。
- 日志和 Trace 默认不记录完整敏感原文。
- 对手机号、地址、订单号、证件号、Token 和密钥进行脱敏。
- 任何来自文档或用户的文本都视为不可信输入，防范 Prompt Injection。
- Prompt 中的指令不能覆盖权限、工具风险和服务端策略。

若发现仓库历史或文档中存在真实凭据，应停止传播该值，报告风险并建议轮换。

## 编码规范

- 使用完整类型标注；公共 Port、领域模型和 API Schema 必须通过 mypy。
- 使用 Pydantic 2 API，不新增 Pydantic 1兼容写法。
- 使用 SQLAlchemy 2 风格语句和异步 Session。
- 时间使用带时区的 UTC `datetime`。
- 金额使用 `Decimal`。
- ID 使用 UUIDv7 或项目统一 ID 生成器。
- 禁止使用可变对象作为函数默认值。
- 禁止 `except Exception: pass`。
- 异常必须转换为领域错误或基础设施错误，并保留可追踪因果链。
- 函数和类保持单一职责；优先组合而非深层继承。
- 不为简单逻辑增加不必要抽象，但外部系统边界必须抽象。
- Prompt 保存在版本化文件或 Prompt Registry 中，不在多处硬编码。
- 模型名、阈值、Top-K、超时和功能开关必须配置化。

## 测试要求

每次修改至少运行与改动范围匹配的测试。新增重大功能必须同时新增测试。

测试层级：

- 单元测试：领域规则、分块、融合、权限、状态 Reducer、错误映射。
- 契约测试：Tool、事件、Fake/Real Adapter、OpenAPI。
- 集成测试：MySQL、Redis、Neo4j、Milvus。
- 端到端测试：文档入库、提问、检索、引用、拒答、故障降级。
- 安全测试：越权、跨租户、Prompt Injection、恶意上传。
- 评测：路由准确率、Recall@K、MRR/NDCG、Faithfulness、引用正确率、拒答率。

CI 中不得调用收费模型。使用 Fake 模型和固定测试向量；真实模型评测作为显式的受控 Job。

修复 Bug 时先添加能复现问题的测试，再实施修复。不得通过删除断言、跳过测试或放宽核心安全条件使 CI 变绿。

## 可观测性

- 每次请求生成 `request_id`，每次 Agent 运行生成 `run_id`。
- API、Supervisor、Agent、检索、Rerank、Tool 和模型调用应处于同一 Trace。
- 记录模型、Prompt、Embedding、知识和 Tool Schema 版本。
- 记录耗时、Token、候选 Chunk ID、过滤数量和最终引用。
- 失败日志必须包含结构化错误码和依赖名称，但不能暴露密钥或敏感载荷。

## 文档维护

- 代码行为、数据模型、接口、状态图或部署方式变化时，同一变更中更新文档。
- 重大架构决策使用 ADR 记录背景、选择、替代方案和影响。
- `memory-bank/architecture.md` 描述当前真实架构，不保存已经失效的理想蓝图。
- `memory-bank/phase1_env_checklist.md` 描述设计目标、用户流程、数据契约、阶段边界和量化验收门槛；不得再创建内容重复的 `design-document.md`。
- 项目状态、已知问题和下一步应保持可被其他开发者或 AI 直接理解。

## 变更纪律

- 修改前检查工作树，保护用户已有改动。
- 不修改与任务无关的文件。
- 不使用破坏性 Git 命令清除未知改动。
- 数据库 Schema 变更必须提供 Alembic Migration，不能只改 ORM。
- 事件、Tool 和公共 API 的破坏性变更必须新建版本。
- 依赖升级必须更新锁文件并说明兼容性影响。
- 完成后报告修改内容、验证方式、未覆盖风险和后续建议。

## 完成定义

功能只有同时满足以下条件才算完成：

- 实现符合架构和阶段边界。
- 关键失败路径已处理。
- 权限、幂等、超时、审计和敏感信息规则已落实。
- 相关单元、集成或端到端测试通过。
- 文档与代码一致。
- 新增配置已加入 `.env.example` 并有说明。
- 没有新增明文凭据、危险默认配置或未标记 Fake 数据。
- 其他开发者或 AI 能根据文档和测试继续维护。

重要提示

写任何代码前必须完整阅读memory-bank/architecture.md

写任何代码前必须完整阅读memory-bank/phase1_env_checklist.md，该文件即本项目的权威设计文档

每完成一个重大功能或里程碑后，必须更新memory-bank/architecture.md
