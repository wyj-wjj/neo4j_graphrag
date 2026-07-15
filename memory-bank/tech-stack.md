# Neo4j GraphRAG 多智能体客服系统技术栈

> 文档状态：阶段一实施基线
> 制定日期：2026-07-14
> 依据：[`phase1_env_checklist.md`](https://github.com/wyj-wjj/neo4j_graphrag/blob/main/phase1_env_checklist.md) 与企业级多智能体架构方案
> 目标：阶段一交付可运行 MVP，同时预留阶段二真实业务 API、Kafka、审批流、对象存储和多租户扩展点，避免大规模重构。
> 设计文档约定：`phase1_env_checklist.md` 即本项目的权威设计文档，不另建 `design-document.md`。

## 1. 核心结论

本项目采用 **Python 异步模块化单体 + LangGraph 状态图 + MySQL/Milvus/Neo4j/Redis 四类存储 + React 管理端**。

阶段一不拆微服务，但代码必须按领域和端口隔离。阶段二新增 Kafka、真实业务 API、审批服务或拆分微服务时，只增加 Adapter 和部署单元，不改 Agent、领域模型和 API 契约。

### 1.1 最终推荐组合

| 层级 | 推荐技术 | 版本策略 | 用途 |
| --- | --- | --- | --- |
| Python 运行时 | CPython | `3.12.x` | 后端、Agent、入库与评测 |
| Python 包管理 | uv | 锁定当前稳定版，提交 `uv.lock` | 环境、依赖、脚本统一管理 |
| Web API | FastAPI + Uvicorn | FastAPI `>=0.126,<1.0` | REST、SSE、OpenAPI、上传接口 |
| 数据校验 | Pydantic + pydantic-settings | `>=2.12,<3` | API、配置、Tool、事件契约 |
| Agent 编排 | LangGraph | `>=1.0,<2` | Supervisor、专家 Agent、状态流转 |
| LLM 基础接口 | OpenAI Python SDK | `>=2,<3` | 调用百炼 OpenAI 兼容接口 |
| 关系数据库 | MySQL | 现有 `8.0+` 可继续；新部署优先 `8.4 LTS` | 真理源、ACL、审计、Outbox |
| ORM/迁移 | SQLAlchemy + Alembic + aiomysql | SQLAlchemy `>=2.0,<2.2` | 异步数据访问与可回滚迁移；规避 asyncmy 未修复 SQL 注入漏洞 |
| 身份校验 | PyJWT + cryptography | PyJWT `>=2.10,<3` | JWT、JWKS、角色与租户上下文 |
| 向量数据库 | Milvus | Server `2.6.14`；PyMilvus `2.6.11` | Dense Vector 召回 |
| 图数据库 | Neo4j Community | Server `5.26.28 LTS`；Python Driver `>=6.2,<7` | 实体直达与 1～2 跳扩展 |
| 状态与缓存 | Redis | Server `8.2.x`；redis-py `>=8,<9` | Checkpoint、会话、缓存、锁 |
| LangGraph 持久化 | langgraph-checkpoint-redis | `>=0.5,<0.6` | Agent 状态持久化与恢复 |
| LLM/Embedding | 阿里云百炼 | 配置化模型名 | L2/L3、Embedding、OCR、Rerank |
| 可观测性 | Langfuse Cloud + structlog | Langfuse Python SDK `>=4,<5` | Trace、Token、检索与 Agent Span |
| 前端 | React + TypeScript + Vite | React `19.2.x`、TS `5.9.x`、Vite `8.x` | 对话、知识管理、运行追踪 |
| 前端组件 | Ant Design + Ant Design X | Ant Design `6.x`、X `2.x` | 企业管理界面与流式对话组件 |
| 前端数据层 | TanStack Query | `5.x` | 服务端状态、缓存、重试 |
| Node 工具链 | Node.js + pnpm | Node `24 LTS`、pnpm `11.x` | 前端构建与依赖锁定 |
| 测试 | pytest + Playwright + Testcontainers | 锁定稳定版本 | 单元、集成、端到端测试 |
| 部署 | Docker Compose + Nginx | Compose V2 | 本地与单机服务器部署 |

### 1.2 已确认的阶段一约束

- 交付后端、React 前端、Docker 镜像、GitHub Actions CI 和本地一键验收，但不执行真实服务器上线。
- 单租户运行时使用 `default`，所有数据与状态仍强制携带和校验 `tenant_id`；租户管理与配额属于阶段二。
- dev/test 使用本地测试身份签发器；生产只接受外部 JWT/JWKS，且不实现自有密码账号系统。
- Fake Agent 可把草单和工单写入 MySQL，但必须标记 Fake，绝不调用真实业务系统。
- 无真实数据时使用版本化的虚构中文电商语料和 Golden Set，CI 不访问收费模型。
- 上传统一保存到 `./data/uploads/`，支持 PDF、DOCX、XLSX、PPTX、HTML、TXT、Markdown、PNG、JPEG；单文件不超过 25 MB，文档不超过 200 页。
- 入库 API 立即返回持久化任务；同一部署单元内的受控任务执行器负责处理并支持重启恢复，阶段二再替换为 Kafka Worker。
- Langfuse Cloud 只接收脱敏元数据，不上传完整问题、回答或知识原文；测试环境默认关闭外发。
- 量化验收门槛以 `phase1_env_checklist.md` 为唯一来源，禁止在多份文档中维护不同数值。

## 2. 选型原则

### 2.1 选择模块化单体，不在阶段一拆微服务

阶段一运行一个 `backend` 进程，并在代码内划分：

```text
api                  HTTP/SSE 接入
application          用例编排
domain               领域模型和规则
agents               LangGraph 节点与状态
retrieval            混合召回、融合、重排
ingestion             文档解析、分块、三库写入
tools                 Tool 契约和注册表
infrastructure        MySQL/Milvus/Neo4j/Redis/LLM Adapter
observability         Trace、日志和指标接口
```

模块之间通过 Python `Protocol`、Pydantic Schema 和应用服务通信，禁止 Agent 直接访问数据库。阶段二需要拆分时，可将 `ingestion`、`retrieval`、`tool` 等模块分别包装成服务，而不改变其核心接口。

### 2.2 异步优先，但不滥用并发

- FastAPI、MySQL、Neo4j、Redis、LLM HTTP 调用统一使用 `async/await`。
- Milvus SDK 的阻塞调用放入受控线程池，并设置并发上限，不在事件循环中直接阻塞。
- 向量召回与图谱实体召回并发执行。
- MySQL 权限过滤必须在候选融合后执行；权限服务失败时 fail closed，不返回知识原文。
- 文档解析、OCR、Embedding 批处理使用后台任务接口，阶段一可进程内执行，阶段二替换成 Kafka Worker。

## 3. 后端技术栈

### 3.1 Python 3.12

选择 `Python 3.12.x`，不直接追随 Python 3.14：

- 对 FastAPI、LangGraph、Milvus、Neo4j、Redis 和文档解析生态兼容更稳。
- 支持现代类型系统、`asyncio` 与性能优化。
- 后续可以独立验证 3.13/3.14，验证通过后再升级运行时。

项目在 `pyproject.toml` 中声明：

```toml
requires-python = ">=3.12,<3.13"
```

Docker 镜像使用 `python:3.12-slim` 的固定补丁版或 digest。开发机、CI、Docker 使用同一小版本。

### 3.2 uv + pyproject.toml

使用 uv，不同时维护 Poetry、pipenv 和多套 `requirements*.txt`：

- `pyproject.toml` 保存直接依赖及兼容范围。
- `uv.lock` 保存完整精确版本并提交 Git。
- 生产和 CI 使用 `uv sync --frozen`，禁止隐式更新依赖。
- 可按 `dev`、`test`、`docs` 分依赖组。

uv 的锁文件可跨平台记录精确依赖，适合后续交给其他开发者或 AI 复现环境。

### 3.3 FastAPI + Pydantic 2

FastAPI 负责：

- `/api/v1/chat/stream`：SSE 流式回答。
- `/api/v1/documents`：文档上传、状态查询、重试。
- `/api/v1/knowledge/search`：检索调试接口，仅管理员可用。
- `/api/v1/sessions`：会话与历史。
- `/api/v1/health/live`、`/ready`：存活与依赖就绪检查。
- `/api/v1/webhooks/approvals`：阶段二审批回调占位。

优先使用 SSE，不在阶段一引入 WebSocket。智能客服主要是服务端持续输出，SSE 更容易穿过 Nginx、调试和重连；只有阶段二出现双向实时事件需求时再补 WebSocket Adapter。

所有 Tool、Agent 输出、事件和 HTTP 请求都使用 Pydantic 2 严格模型，禁止依赖 Prompt 保证 JSON 正确。

### 3.4 SQLAlchemy 2 + Alembic + aiomysql

推荐：

```text
SQLAlchemy 2.x AsyncSession
aiomysql MySQL async driver
Alembic schema migration
```

不用 SQLModel 作为核心数据层，避免 API Schema 与数据库实体过度耦合。API 模型、领域模型和 ORM 模型分开维护。

MySQL URL：

```text
mysql+aiomysql://app_user:password@127.0.0.1:3306/enterprise_agent_db
```

阶段一就建立但暂不异步消费的 `outbox_events` 表。同步入库成功或失败均记录索引状态，为阶段二 Kafka 接入保留事务边界。

## 4. Agent 与模型调用

### 4.1 LangGraph 1.x

选择 LangGraph 1.x LTS API，直接使用 `StateGraph`、conditional edges、checkpoint 和 interrupt，不使用 AutoGen/CrewAI 式自由对话协作。

阶段一运行：

- Supervisor Agent
- KB Agent
- FAQ Agent
- Escalation Agent

阶段一提供契约与 Fake Adapter：

- Order Agent
- Logistics Agent
- Refund Agent

LangGraph 只负责流程和状态，不承载以下规则：

- 权限校验
- 金额和风控判断
- 幂等判断
- 数据库事务
- 工具超时和重试策略

这些规则由应用服务和 Tool Adapter 确定性执行。

### 4.2 减少 LangChain 依赖面

- 核心编排直接依赖 `langgraph`。
- 只安装必要的 `langchain-core`，不默认引入全部 LangChain Community 集成。
- 不把数据库对象、Retriever 和业务服务绑定到 LangChain 抽象。
- Prompt、模型、Embedding、Reranker 都由项目自己的 Port 包装。

这样能降低升级影响，也方便以后迁移到其他 Agent 框架。

### 4.3 使用 OpenAI 兼容 SDK 调百炼

项目默认使用 OpenAI Python SDK 对接百炼 OpenAI 兼容地址，而不是让业务代码直接依赖 DashScope SDK：

```text
ChatModelPort
├── DashScopeOpenAIAdapter
├── GenericOpenAIAdapter
└── FakeChatModel
```

原因：

- 百炼官方支持 OpenAI 兼容接口。
- 后续更换 OpenAI、DeepSeek、其他兼容平台或本地 vLLM 时，只需替换配置或 Adapter。
- SDK 原生支持异步客户端、流式输出和结构化调用。

DashScope SDK 仅用于 OpenAI 兼容接口没有覆盖的特定多模态能力，并封装在单独 Adapter 中。

### 4.4 模型配置

模型名只能存在于配置和模型注册表，禁止散落在 Agent 代码中。

| 能力 | 阶段一默认 | 设计要求 |
| --- | --- | --- |
| L2 路由/FAQ | `qwen-turbo` | 低温度、结构化输出、超时短 |
| L3 生成/复杂判断 | `qwen-plus` | 必须基于 evidence 回答 |
| Dense Embedding | `text-embedding-v4` | 固定 `1024` 维并记录模型版本 |
| Rerank | `qwen3-rerank` | 可配置开关，Top 20 → Top 5 |
| OCR | `.env` 配置的 OCR 模型 | 仅扫描件或图片路径使用 |
| 测试 | `FakeChatModel`、`FakeEmbedding` | CI 不调用收费 API |

`text-embedding-v4` 支持多种维度；本项目固定为 1024 维。Milvus Collection 的向量维度在创建后不可随意变化，因此必须同时保存 `embedding_model`、`embedding_dimension` 和 `embedding_version`。更换模型时创建新 Collection 并重建索引，不原地混写。

## 5. 存储技术栈

### 5.1 MySQL：唯一真理源

现有 MySQL 8.0 可以继续用于阶段一；新服务器部署优先 MySQL 8.4 LTS。不要为了选型文档破坏已有本地数据。

核心原则：

- 原文、文档状态、版本、ACL、审计、入库任务状态保存在 MySQL。
- Milvus 和 Neo4j 只是可重建的派生索引。
- 表必须从阶段一开始包含 `tenant_id`，单租户环境使用 `default`。
- 应用使用独立最小权限账号，禁止使用 `root`。
- 时间统一保存 UTC，金额使用 `DECIMAL`，禁止 `float`。
- Binlog 使用 `ROW`，为阶段二 CDC 做准备。

### 5.2 Milvus 2.6.x

使用 Milvus `2.6.14` Standalone 和匹配的 PyMilvus `2.6.11`。Compose 同步采用该版本官方
Standalone 配套的 etcd `3.5.25`、MinIO `RELEASE.2024-12-18T13-15-44Z` 与 Woodpecker MQ。
不采用 `3.0-beta`，因为项目目标是稳定 MVP，不需要测试版存储架构。

Collection 建议：

```text
name: weview_content_chunks_v1
primary key: chunk_id (VARCHAR)
vector: dense_vector (FLOAT_VECTOR, dim=1024)
scalar filter: tenant_id, document_id, status, embedding_version
index: HNSW
metric: COSINE
```

Milvus 只做粗粒度租户和状态预过滤；最终权限和有效期必须回 MySQL复核。

### 5.3 Neo4j 5.26 LTS

使用 Neo4j Community `5.26 LTS`，Compose 固定到可用的 `neo4j:5.26.28-community` 镜像，不使用快速变化的 `2026.x` 当前版本作为阶段一基线。Python 使用官方 `neo4j` 包 6.2 系列，不安装已弃用的 `neo4j-driver` 包名。

使用异步 Driver，并显式指定数据库和超时。图谱至少包含：

```text
(Document)-[:HAS_CHUNK]->(Chunk)
(Chunk)-[:MENTIONS]->(Entity)
(Entity)-[r:RELATED_TO]->(Entity)
```

所有节点和关系携带 `tenant_id`、`source_chunk_id`、`extractor_version`、`confidence` 与有效期。建立唯一约束和组合索引。阶段一图扩展限制为 1～2 跳，禁止无界 Cypher。

核心检索使用官方 Neo4j Driver 和项目自己的 `GraphStorePort`。`neo4j-graphrag-python` 可用于实验或参考，但不作为核心业务依赖，以免把 Milvus、MySQL ACL 和自定义融合链路锁进单一框架。

### 5.4 Redis 8.2.x

建议把阶段一 Redis 基线升级到 `8.2.x`。原因不是普通缓存，而是官方 `langgraph-checkpoint-redis` 需要 RedisJSON 与 RediSearch；Redis 8 已将这些能力纳入发行版。

如果继续使用 Redis 8 以下版本，必须使用包含 RedisJSON 和 RediSearch 的 Redis Stack，普通 Redis 7 容器不能直接满足该 Checkpointer。

Redis 用途分开命名：

```text
agent:{tenant_id}:{session_id}:*       LangGraph checkpoint
session:{tenant_id}:{session_id}       会话短状态
cache:tool:{tenant_id}:{hash}          只读工具缓存
lock:ingest:{document_id}              入库互斥锁
rate:{tenant_id}:{user_id}:{window}    限流
```

开启 AOF，设置 `maxmemory-policy noeviction` 或按用途拆实例。关键 Checkpoint 不应与可随意淘汰的缓存共享同一个淘汰策略。

## 6. GraphRAG 检索技术栈

### 6.1 检索流水线

```text
Query Rewrite
  ├── Dense Milvus Top 20
  ├── Neo4j Entity/Graph Top 20
  └── MySQL FULLTEXT/BM25-like keyword Top 20
             ↓
          RRF 融合
             ↓
      MySQL ACL/状态/有效期复核
             ↓
     qwen3-rerank Top 20 → Top 5
             ↓
       引用式答案或明确拒答
```

阶段一先实现 Dense + Graph 两路；关键词召回接口同时定义，可先用 MySQL FULLTEXT。融合使用 RRF，避免直接比较不同检索器不可比的分数。

### 6.2 文档解析

阶段一采用轻依赖、按格式解析：

| 格式 | 推荐库 |
| --- | --- |
| PDF | PyMuPDF |
| Word | python-docx |
| Excel | openpyxl |
| PowerPoint | python-pptx |
| HTML | BeautifulSoup4 + lxml |
| 图片 | Pillow + 百炼 OCR Adapter |

不把 Unstructured/Docling 作为第一阶段硬依赖；它们可以作为 `DocumentParserPort` 的后续 Adapter。扫描 PDF 先检测文本层，只有无有效文本时才调用 OCR，降低费用。

### 6.3 分块

- 结构感知分块优先于固定字符切分。
- 检索块建议 300～600 中文字，重叠 10%～15%，最终通过评测校准。
- 使用父子块：小块检索，父块生成。
- 表格作为结构化单元，不按普通段落拆碎。
- 每个 Chunk 使用 UUIDv7，保存 `content_hash`、标题路径、页码和版本。

## 7. 阶段二扩展接口

阶段一必须定义以下 Port，但只实现本地或 Fake Adapter：

```text
ChatModelPort
EmbeddingPort
RerankerPort
KnowledgeRepositoryPort
VectorStorePort
GraphStorePort
CheckpointStorePort
ObjectStorePort
EventPublisherPort
ApprovalPort
OrderServicePort
LogisticsServicePort
RefundServicePort
AuthorizationPort
AuditPort
```

### 7.1 Kafka

阶段一：

- MySQL 同事务写业务记录和 `outbox_events`。
- `InProcessEventPublisher` 同步调用索引 Handler。
- 失败写入任务状态，可人工重试和重建。

阶段二：

- 使用 Kafka KRaft 集群。
- Python Producer/Consumer 优先 `confluent-kafka`，封装在 `KafkaEventPublisher`。
- 保持阶段一 Event Envelope 不变。
- 所有消费者基于 `event_id` 幂等并提供 DLQ。

### 7.2 对象存储

阶段一使用 `LocalObjectStore` 保存到 `./data/uploads`；阶段二增加 S3 兼容 Adapter，接入 MinIO 或 OSS。数据库只保存对象 Key、hash、MIME、大小和版本，不保存机器绝对路径。

### 7.3 审批流

阶段一实现：

- `ActionDraft`、`ApprovalRequest`、`ApprovalDecision` Schema。
- `FakeApprovalService`。
- 审批 Webhook 路由占位。
- Refund Agent 只能生成草单，不执行资金动作。

阶段二将流程拆为：

```text
calculate_refund → create_draft → approval_interrupt → submit_refund
```

LangGraph 挂起点位于草单生成后、真实提交前，不能简单使用 `interrupt_before=["refund"]`。

## 8. 可观测性

### 8.1 Langfuse 4

使用 Langfuse Python SDK 4.x。每次请求统一记录：

- `request_id`、`run_id`、`session_id`、脱敏后的 `user_id`。
- Supervisor 路由、Agent 节点、检索分支、Rerank、Tool、LLM Span。
- 模型名、Prompt 版本、知识版本、Token、TTFT、总耗时。
- 候选 `chunk_id` 与分数，不默认上传敏感原文。

Cloud 环境先做字段脱敏；若后续真实业务数据不允许传给第三方，切换自建 Langfuse。代码只依赖项目自己的 `TracePort`，不让 Langfuse SDK侵入领域层。

### 8.2 日志与指标预留

- `structlog` 输出 JSON 日志。
- OpenTelemetry 语义统一 Trace，上线时可导出到 Tempo/Jaeger。
- 阶段一不部署 Prometheus/Grafana，但代码暴露 `/metrics` 的 Adapter 位置。
- 禁止日志记录密码、API Key、完整地址、手机号、证件号和未经脱敏的对话全文。

## 9. 前端技术栈

使用：

```text
Node.js 24 LTS
pnpm 11
React 19.2
TypeScript 5.9
Vite 8
Ant Design 6
Ant Design X 2
TanStack Query 5
React Router
Vitest + Testing Library
Playwright
```

暂不使用 Next.js。该项目是登录后的管理和客服工作台，不需要 SEO 或 React Server Components；Vite SPA 部署更简单、攻击面更小，也与现有 `VITE_API_*` 配置一致。

前端只保存展示状态；会话、权限、Agent 状态和任务状态均以后端为准。API 类型通过 OpenAPI 生成，不手写两套重复 DTO。

## 10. 测试和质量工具

### 10.1 后端

- `pytest 9.x`
- `pytest-asyncio 1.x`
- `pytest-cov`
- `respx`：Mock HTTPX/模型请求
- `testcontainers`：MySQL、Redis、Neo4j 集成测试
- 独立 Compose Job：Milvus 集成测试
- `ruff`：Lint + Format
- `mypy`：核心领域、Port 和公共 API 严格类型检查

### 10.2 前端

- Vitest + React Testing Library：组件和 Hook。
- Playwright：上传文档、提问、流式回答、引用展开、失败降级等 E2E。
- ESLint 仅保留 React/TypeScript 必需规则，格式化交给 Prettier 或统一选定的 formatter，避免规则冲突。

### 10.3 安全与供应链

- `pip-audit`：Python 依赖漏洞。
- `pnpm audit`：前端依赖。
- Gitleaks：阻止密钥进入 Git。
- Trivy Action `v0.36.0`：容器镜像和文件系统扫描；因旧版内置二进制安装在当前 Runner 失败而升级。
- 依赖升级由 Renovate/Dependabot 提 PR，测试通过后合并，不自动追最新版。

## 11. Docker 与部署

阶段一 Compose 包含：

```text
backend
frontend/nginx
redis 8.2
neo4j 5.26 LTS
milvus 2.6.14 + etcd + MinIO（Milvus 内部依赖）
```

MySQL 可继续使用本机安装；通过环境变量连接。为了 CI 和新开发者一键启动，另外提供可选的 MySQL 8.4 Compose Profile。

阶段二组件放入 Compose Profile，默认不启动：

```text
kafka
kafka-ui
minio（业务文件对象存储）
worker-embedding
worker-kg
approval-mock
```

注意区分 Milvus 内部使用的 MinIO 与阶段二业务对象存储 Bucket/账号，禁止共用默认凭据。

## 12. 配置和密钥

- 仓库只提交 `.env.example`，不提交 `.env`。
- Pydantic Settings 启动时校验必填项、URL、端口、阈值和模型维度。
- `APP_ENV=dev|test|staging|prod`，生产环境禁止 Fake Adapter。
- `USE_FAKE_EXTERNAL_CLIENTS` 只能在 dev/test 为 true。
- 密钥通过环境变量注入，阶段二替换 Secret Manager/KMS。
- 当前环境清单出现了 Redis 和数据库明文密码；应删除公开文档中的密码并轮换相关凭据。

建议新增环境变量：

```text
APP_ENV
APP_NAME
LOG_LEVEL
DEFAULT_TENANT_ID
EMBEDDING_DIMENSION=1024
EMBEDDING_VERSION
RERANK_MODEL
RERANK_ENABLED
RAG_RETRIEVAL_TOP_K=20
RAG_FINAL_TOP_K=5
RAG_GRAPH_MAX_HOPS=2
CHECKPOINT_TTL_SECONDS
UPLOAD_MAX_BYTES
ALLOWED_UPLOAD_TYPES
REQUEST_TIMEOUT_SECONDS
TOOL_TIMEOUT_SECONDS
OTEL_EXPORTER_OTLP_ENDPOINT
```

## 13. 版本锁定规则

1. 本文中的版本表示允许使用的稳定系列，不代替锁文件。
2. `pyproject.toml`/`package.json` 声明兼容范围，`uv.lock`/`pnpm-lock.yaml` 保存精确版本。
3. Docker 镜像固定补丁版本；生产环境进一步固定 digest。
4. Milvus Server 与 PyMilvus 按官方兼容表成对升级。
5. Neo4j 优先 LTS，不追当前月度版本。
6. LangGraph、Langfuse 等重大版本升级必须先运行离线 Golden Set 和 E2E。
7. Embedding 模型或维度变化必须重建新 Collection，不能混用旧向量。

## 14. 明确不采用的方案

| 不采用 | 原因 |
| --- | --- |
| Python 3.14 作为首发运行时 | 新、兼容面尚需验证，3.12 更稳 |
| Milvus 3.0 Beta | 存储层不应使用 Beta 作为 MVP 基线 |
| Neo4j 2026.x 当前版 | 月度变化快，5.26 LTS 更利于长期维护 |
| 阶段一拆微服务 | 增加部署和调试成本，不能改善当前业务验证 |
| Agent 直接访问数据库 | 无法集中权限、审计、重试和事务治理 |
| 全量 LangChain Community | 依赖面大、升级耦合高 |
| 将 `neo4j-graphrag-python` 作为系统核心 | 难以覆盖三库协同、MySQL ACL 和自定义融合 |
| 普通 Redis 7 + 官方 Redis Checkpointer | 缺少 RedisJSON/RediSearch 模块 |
| Next.js | 当前无 SEO/RSC 需求，Vite SPA 更适合管理端 |
| 阶段一部署 Kafka/审批平台 | 没有真实写业务，先定义契约和 Outbox 即可 |

## 15. 阶段一完成标准

- `uv sync --frozen`、`pnpm install --frozen-lockfile` 可复现安装。
- 一条命令启动应用依赖并完成健康检查。
- 文档可上传、解析、分块并写入三库，失败可重试。
- Supervisor、KB、FAQ、Escalation 可运行。
- Order、Logistics、Refund 的 Schema、Tool 和 Fake Adapter 可测试。
- 检索候选必须经过 MySQL ACL/状态/有效期复核。
- 每个回答带 Chunk 引用；无可靠证据时明确拒答。
- Redis Checkpoint 可在进程重启后恢复会话。
- Langfuse 可查看完整节点链路，敏感字段已脱敏。
- 单元、集成、E2E、安全扫描进入 CI。
- 生产配置不能启用 Fake Client，也不能使用默认密码。

## 16. 官方参考

- [LangGraph 1.x 与版本策略](https://docs.langchain.com/oss/python/versioning)
- [LangGraph 持久化](https://docs.langchain.com/oss/python/langgraph/persistence)
- [Redis LangGraph Checkpointer 要求](https://pypi.org/project/langgraph-checkpoint-redis/)
- [FastAPI 版本说明](https://fastapi.tiangolo.com/deployment/versions/)
- [SQLAlchemy MySQL Async Dialect](https://docs.sqlalchemy.org/en/latest/dialects/mysql.html)
- [Alembic 官方文档](https://alembic.sqlalchemy.org/en/latest/)
- [Milvus 2.6 Release Notes](https://milvus.io/docs/v2.6.x/release_notes.md)
- [PyMilvus 安装与版本匹配](https://milvus.io/docs/install-pymilvus.md)
- [Neo4j 5.26 LTS 文档入口](https://neo4j.com/docs/operations-manual/current/)
- [Neo4j Python Driver](https://neo4j.com/docs/api/python-driver/current/)
- [MySQL 8.4 LTS Release Model](https://dev.mysql.com/doc/refman/8.4/en/mysql-releases.html)
- [百炼 OpenAI 兼容调用](https://help.aliyun.com/en/model-studio/first-api-call-to-qwen)
- [百炼 Embedding](https://help.aliyun.com/zh/model-studio/embedding)
- [Langfuse SDK](https://langfuse.com/docs/observability/sdk/overview)
- [uv Lock 与 Sync](https://docs.astral.sh/uv/concepts/projects/sync/)
- [React 19.2](https://react.dev/blog/2025/10/01/react-19-2)
- [Vite 8](https://vite.dev/blog/announcing-vite8)
- [Node.js Release Schedule](https://nodejs.org/en/about/previous-releases)
