# 阶段一 MVP 环境准备清单

> 基于 `enterprise_multi_agent_architecture.md` 企业级多智能体架构方案的落地方案。
> 本文档总结当前环境状态、缺失组件部署方式、以及阶段一 MVP 的边界范围。
> 本文件同时作为项目的权威设计文档；项目不再单独维护 `design-document.md`。

---

## 一、部署环境概览

> **当前阶段：本地电脑开发验证**，所有组件均在本地运行。
> 宝塔面板部署方案已标注保留，后续上服务器时可直接参考。

| 环境 | 部署方式 | 组件 |
| :--- | :----- | :--- |
| **本地电脑（当前）** | Docker | Milvus、Neo4j、Redis |
| | 本地直装 | MySQL 8.0+ |
| | 云平台 | 阿里百炼（DashScope）、Langfuse Cloud |
| **服务器（后续）** | 宝塔面板裸机 | MySQL 8.0+ |
| | Docker | Milvus、Neo4j、Redis、Kafka 等 |
| | 云平台 | 同上 |

---

## 二、各组件状态总览

| #  | 组件                   | 状态                | 部署方式            | 备注                                                                   |
| :- | :--------------------- | :------------------ | :------------------ | :--------------------------------------------------------------------- |
| 1  | **MySQL**        | ✅ 已就绪           | 本地直装（后续服务器上通过宝塔管理） | 需创建库 `enterprise_agent_db`，开启 Binlog（为阶段二准备） |
| 2  | **Milvus**       | ✅ 已就绪           | Docker              | 端口 19530；阶段一新建隔离的版本化 Collection：`weview_content_chunks_v1`，不得覆盖已有 Collection |
| 3  | **Neo4j**        | ✅ 已就绪           | Docker              | Bolt 7687，Web 7474                                                    |
| 4  | **Redis**        | ✅ 已就绪           | Docker              | 密码通过 `.env` 注入，端口 6379，AOF 持久化已开启                   |
| 5  | **阿里百炼 LLM** | ✅ 已配置           | 云 API              | L3:`qwen-plus`，L2: `qwen-turbo`，Embedding: `text-embedding-v4` |
| 6  | **Langfuse**     | ✅ 已配置           | Cloud 免费版        | 密钥已写入 `.env`                                                    |
| 7  | **Kafka**        | ⏸ 推迟到阶段二     | Docker              | 阶段一走同步模式，无需消息队列                                         |
| 8  | **审批流通知**   | ⏸ 推迟到阶段二     | 企微/SMTP           | 阶段一无写操作，无审批流需求                                           |
| 9  | **外部业务 API** | ⏸ 阶段一用 Fake    | Mock 实现           | 阶段二接入真实 Order/Logistics/Refund 系统                             |
| 10 | **对象存储**     | ⏸ 阶段一用本地磁盘 | 本地 `./data/uploads/` | 阶段二文件量大后再上 MinIO/OSS                                      |

---

## 三、需立即执行的操作

### 1. ~~启动 Redis~~ ✅ 已完成

Redis 已在 Docker 中部署并运行，密码通过 `.env` 注入，AOF 持久化已开启。

### 2. 在 MySQL 中创建数据库

**本地电脑操作**：

打开命令行或 MySQL 客户端工具（如 Navicat、DBeaver、HeidiSQL），执行：

```sql
CREATE DATABASE IF NOT EXISTS enterprise_agent_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;
```

或用 MySQL 命令行：

```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS enterprise_agent_db DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci;"
```

**后续上服务器（宝塔面板）操作**：

宝塔面板 → 数据库 → 添加数据库：
- 数据库名：`enterprise_agent_db`
- 用户名/密码：与 `.env` 中 `DATABASE_URL` 一致（禁止在文档中记录真实凭据）

### 3. 开启 MySQL Binlog（为阶段二准备）

**本地电脑操作**：

找到 MySQL 配置文件 `my.ini`（Windows 通常在 `C:\ProgramData\MySQL\MySQL Server 8.0\my.ini`），在 `[mysqld]` 段落下添加：

```ini
log-bin=mysql-bin
binlog_format=ROW
server-id=1
```

保存后，在 Windows 服务（`services.msc`）中找到 MySQL 服务并重启。

**后续上服务器（宝塔面板）操作**：

宝塔 → 软件商店 → MySQL → 设置 → 配置修改，在 `[mysqld]` 下添加相同配置，保存后重启 MySQL。

---

## 四、关键概念澄清

### 4.1 LangGraph vs Langfuse

|          | LangGraph                                           | Langfuse                                        |
| :------- | :-------------------------------------------------- | :---------------------------------------------- |
| 是什么   | Python 包，Agent 编排框架                           | 可观测性 Web 平台                               |
| 安装方式 | `pip install langgraph`                           | Docker 自建 或 Cloud 免费版                     |
| 作用     | 编排多 Agent 的执行流程（Supervisor → 专家 Agent） | 记录 LLM 调用耗时、Token、上下文，排查 Bad Case |
| 关系     | 负责**执行**                                  | 负责**记录和展示**                        |

接入方式（Langfuse Cloud，已配好密钥）：

```python
from langfuse.langchain import CallbackHandler

langfuse_handler = CallbackHandler(
    public_key="通过环境变量注入",
    secret_key="通过环境变量注入",
    host="Langfuse 部署地址"
)

# LangGraph 调用时传入
app.invoke(state, config={"callbacks": [langfuse_handler]})
```

### 4.2 为什么不需要 LLM Gateway

阿里百炼（DashScope）本身就是统一的多模型平台，支持：

- `qwen-plus`：L3 旗舰模型（复杂推理、情绪安抚、退款决策）
- `qwen-turbo`：L2 小模型（意图分类、简单寒暄、Supervisor 路由）
- `text-embedding-v4`：Embedding 模型
- `qwen-vl-ocr-2025-11-20`：OCR 模型

所有模型通过同一个 `DASHSCOPE_BASE_URL` 调用，无需额外搭建 OneAPI/LiteLLM。

### 4.3 为什么阶段一不需要 Kafka

Kafka 在架构中的职责：MySQL Outbox → Kafka → embedding-worker / kg-worker（异步同步到 Milvus/Neo4j）。

阶段一 MVP 用**同步调用**替代：

```python
def ingest_chunk(content):
    chunk_id = mysql.insert(content)          # 1. 写入 MySQL
    vector = embedding_model.encode(content)
    milvus.upsert(chunk_id, vector)            # 2. 同步写 Milvus
    entities = llm_extract(content)
    neo4j.create_subgraph(chunk_id, entities)   # 3. 同步写 Neo4j
```

阶段二入库量大时再加 Kafka 解耦，改造成本低。

### 4.4 为什么阶段一不需要审批流通知

审批流（Human-in-the-loop）的典型场景：用户申请退款 → 生成草单 → 挂起 → 主管在企微审批 → 继续执行。

阶段一对外只提供无真实副作用的能力，不执行退款、改地址等真实业务写操作，因此不需要真实审批流和企微/邮件通知。为验证阶段二契约，阶段一允许在 MySQL 中持久化明确标记为 `fake` 的改地址、催单、退款草单和人工工单；这些记录不得调用外部业务系统。

### 4.5 为什么外部业务 API 用 Fake

架构中的 Order/Logistics/Refund Agent 需要调真实业务系统 API，但阶段一没有这些系统。用 Fake 实现返回模拟数据，接口契约不变，阶段二换真实实现即可：

```python
# 阶段一：Fake 实现
class FakeOrderService:
    def query_order(self, order_id):
        return {"status": "已发货", "items": [...], "address": "..."}

# 阶段二：真实实现
class RealOrderService:
    def query_order(self, order_id):
        return requests.get(f"订单服务地址/orders/{order_id}")
```

### 4.6 为什么对象存储用本地文件系统

文档解析管道需要存储原始文件（PDF/Word/图片）。阶段一只有单机部署、文件量小，直接用本地目录：

```python
UPLOAD_DIR = "./data/uploads"
```

阶段二多实例部署、文件量大时，上 MinIO（Docker 一行命令）：

```yaml
minio:
  image: minio/minio
  ports:
    - "9000:9000"
    - "9001:9001"
  command: server /data --console-address ":9001"
```

---

## 五、多 Agent 架构设计

### 5.1 为什么选择多 Agent

- **角色专业化**：每个 Agent 只负责一个业务域，工具列表精简，Prompt 聚焦，降低误调用率
- **可扩展**：新增业务域只需新增 Agent 节点，不影响现有 Agent
- **可控边界**：Supervisor 统一调度，避免 Agent 之间耦合扩散

### 5.2 Agent 拓扑

```
                      ┌──────────────────┐
                      │  1. Supervisor    │  总调度（意图路由 / 循环控制 / 结果仲裁）
                      └────────┬─────────┘
        ┌──────────┬──────────┼──────────┬──────────┬──────────┐
        ▼          ▼          ▼          ▼          ▼          ▼
   ┌─────────┐┌─────────┐┌─────────┐┌─────────┐┌─────────┐┌─────────┐
   │2.KB     ││3.FAQ    ││4.Order  ││5.Logis- ││6.Refund ││7.Escala-│
   │ Agent   ││ Agent   ││ Agent   ││tics Agt ││ Agent   ││tion Agt │
   └─────────┘└─────────┘└─────────┘└─────────┘└─────────┘└─────────┘
   知识检索    标准问答     查单改单    物流催单    退款审批    兜底安抚
```

### 5.3 7 个 Agent 详细定义

| # | Agent | 阶段一状态 | 使用模型 | 绑定的工具 | 职责描述 |
|:--|:--|:--|:--|:--|:--|
| 1 | **Supervisor** | ✅ 全功能 | `qwen-turbo`（L2） | 无（纯路由） | LLM 意图分类与路由分发；多 Agent 结果收敛与置信度仲裁；通过 `visited_agents` 和 `iteration` 防死循环 |
| 2 | **KB Agent** | ✅ 全功能 | `qwen-plus`（L3） | `HybridRetrieve` | 三库协同 GraphRAG：Milvus 向量召回 → Neo4j 图谱多跳扩展 → MySQL 权限清洗 → LLM 生成答案 |
| 3 | **FAQ Agent** | ✅ 全功能 | `qwen-turbo`（L2） | `FAQMatcher` | 高频标准问题匹配（"怎么开发票""退货流程"等 SOP）；向量匹配 + 规则引擎快速命中，不经过重量级检索链路 |
| 4 | **Order Agent** | ⚠️ 骨架+Fake | `qwen-plus`（L3） | `QueryOrder`, `UpdateAddress` | 订单查询、状态跟踪、修改收货地址；阶段一返回 Fake 模拟数据，阶段二接入真实订单系统 API |
| 5 | **Logistics Agent** | ⚠️ 骨架+Fake | `qwen-turbo`（L2） | `QueryLogistics`, `UrgeDelivery` | 物流轨迹查询、催单；阶段一返回 Fake 模拟数据，阶段二接入真实物流系统 API |
| 6 | **Refund Agent** | ⚠️ 骨架+Fake | `qwen-plus`（L3） | `CalculateRefund`, `CreateDraft` | 退款计算、草单生成；阶段一只生成草单不做真实退款；阶段二接入审批流（Human-in-the-loop） |
| 7 | **Escalation Agent** | ✅ 全功能 | `qwen-plus`（L3） | `CreateTicket` | 情绪安抚、投诉升级、兜底处理；当其他 Agent 置信度过低或用户情绪激动时接手，决定是否转人工 |

> **阶段一实际跑通 4 个全功能 Agent：Supervisor + KB + FAQ + Escalation**
> **阶段一搭好骨架、阶段二接入真实的 3 个：Order + Logistics + Refund**

### 5.4 Agent 通信机制：共享 State + Supervisor 仲裁

Agent 之间**不直接通信**，只通过共享 `AgentState` 间接协作：

```
┌──────────────────────────────────────────────────────┐
│               共享 AgentState                         │
│  query / chat_history / evidence_context /           │
│  retrieved_chunks / confidence / draft_action ...    │
├───────────┬──────────┬──────────┬────────────────────┤
│   读/写    │  读/写    │  读/写    │  读/写              │
│   KB      │  Order   │  Refund  │  Escalation        │
│   Agent   │  Agent   │  Agent   │  Agent             │
└───────────┴──────────┴──────────┴────────────────────┘
       ▲          ▲          ▲          ▲
       └──────────┴────┬─────┴──────────┘
                       │
             ┌─────────┴─────────┐
             │    Supervisor     │  ← 唯一有权调度 Agent 的节点
             └───────────────────┘
```

**设计原则：**
- **解耦**：每个 Agent 只读写自己关心的 State 字段，不依赖其他 Agent 内部实现
- **可追溯**：State 每次变更可记录，出问题看 State 快照即定位是哪个 Agent 写错
- **可重放**：相同 State 输入 → 相同 Agent 输出，方便测试和 Debug
- **防耦合扩散**：通过 Supervisor 统一调度，新增 Agent 不影响现有 Agent

**为什么不用 Agent 直接对话模式（如 AutoGen / CrewAI）：**
- 消息传递链不可控，容易死循环，难以审计
- LangGraph 有向图结构，路径可预测，每一步有 Span 可追踪

### 5.5 多级模型降级漏斗（成本控制）

```
用户请求
  │
  ├─ L1 (0 Token): 规则/正则匹配 → FAQ Agent 直接返回
  ├─ L2 (低成本):  qwen-turbo 分类 + FAQ 向量匹配
  └─ L3 (高成本):  qwen-plus 走完整 GraphRAG 检索链路
```

> 设计目标：80% 的 FAQ 问题在 L1/L2 解决，只有 20% 复杂问题进入 L3，Token 成本降低 60%。

### 5.6 项目核心亮点（面试用）

| 亮点 | 说明 |
|:---|:---|
| **三库协同 GraphRAG** | 不是普通 RAG。Milvus 向量召回 + Neo4j 图谱多跳推理 + MySQL 权限清洗，通过 Chunk_ID 贯穿三库。解决"语义相似但逻辑相关"的漏召回问题 |
| **多级模型降级** | L1 规则 → L2 小模型 → L3 旗舰模型，按问题复杂度分级调用，大幅降低 Token 成本 |
| **写操作安全设计** | Agent 只能生成草单不能直接执行退款。致命操作走 Human-in-the-loop，LangGraph 原生 `interrupt_before` 实现状态挂起。所有写操作带 `idempotency_key` 防重复 |
| **数据一致性保障** | MySQL 为真理源（Source of Truth），Milvus/Neo4j 为派生索引。Outbox 模式保证不丢消息，最终一致性 |
| **全链路可观测** | Langfuse 追踪每次 LLM 调用（Token/耗时/Context），LangGraph 每个节点天然对应 Trace Span，Bad Case 可定位到具体环节 |

### 5.7 面试话术模板

> "我做的是一个**企业级多智能体智能客服系统**，基于 LangGraph 编排了 7 个专家 Agent。
>
> 核心差异化有四点：
> 1. **三库协同的 GraphRAG 检索**：不是简单向量检索，而是 MySQL 作真理源、Milvus 作向量引擎、Neo4j 作图谱推理，通过 Chunk_ID 贯穿三库，既保证权限安全又实现多跳推理。
> 2. **多级模型降级**：L1 规则 → L2 小模型 → L3 旗舰模型，80% 的问题在低成本层解决，Token 成本降低 60%。
> 3. **写操作安全**：退款等敏感操作走 Human-in-the-loop，Agent 只生成草单不直接执行，LangGraph 原生支持状态挂起。
> 4. **全链路可观测**：Langfuse 追踪 LLM 调用，每个节点有 Span，Bad Case 可回溯到具体环节。"

---

## 六、阶段一 MVP 边界定义

### 6.1 阶段一目标

> 用户提问 → Supervisor 路由 → 专家 Agent 处理 → 返回答案

核心是跑通 **7 Agent 骨架 + 4 个全功能 Agent（Supervisor + KB + FAQ + Escalation）**。

### 6.2 阶段一范围

| 功能                       | 阶段一 | 阶段二 |
| :------------------------- | :----- | :----- |
| 文档入库与分块             | ✅     | —     |
| 向量检索（Milvus）         | ✅     | —     |
| 图谱检索（Neo4j）          | ✅     | —     |
| MySQL 鉴权过滤             | ✅     | —     |
| 多 Agent 编排（LangGraph） | ✅     | —     |
| Supervisor 路由 + 循环控制 | ✅     | —     |
| KB / FAQ / Escalation 全功能 | ✅   | —     |
| Order / Logistics / Refund 骨架 | ✅  | —     |
| 真实外部写操作（退款/改地址） | ❌  | ✅     |
| 本地 Fake 草单与工单       | ✅     | —      |
| Human-in-the-loop 审批     | ❌     | ✅     |
| 异步数据同步（Kafka）      | ❌     | ✅     |
| 租户字段与强制隔离校验     | ✅     | —      |
| 多租户管理、配额与自助开通 | ❌     | ✅     |
| 微服务拆分                 | ❌     | ✅     |
| 高可用/灾备                | ❌     | ✅     |

### 6.3 阶段一不需要做的事情

- 不部署 Kafka
- 不接入企微/邮件通知
- Order/Logistics/Refund 使用 Fake 实现
- 不部署 MinIO（用本地磁盘）
- 不做审批流
- 不做 Prometheus/Grafana（Langfuse 够用）

---

## 七、已确认的实施决策与验收基线

以下决策是阶段一实施的固定边界，除非用户明确修改，否则 AI 开发者不得自行改变。

### 7.1 交付与运行边界

- 阶段一包含后端、React 前端、Docker 镜像、GitHub Actions CI 和本地一键验收流程。
- 阶段一只保证本地与 CI 可重复运行，不包含真实服务器上线、域名、证书或生产流量切换。
- 当前开发基线为 Windows 主机配合 WSL2、Docker Desktop 和本地 MySQL；应用与脚本优先在 Linux/WSL2 环境执行。
- 所有测试只能创建和清理名称明确带 `test` 或版本后缀的专用资源，不得删除、迁移或覆盖来源不明的数据库、Collection、图数据、Redis Key 或文件。

### 7.2 租户、身份与 Fake 能力

- 阶段一只启用一个 `default` 租户，但数据库、Milvus、Neo4j、Redis、Checkpoint、API 和审计从第一天携带并强制校验 `tenant_id`。
- 阶段一不实现租户管理后台、配额、自助开通和跨租户运营能力；这些属于阶段二。
- 后端负责验证 JWT 并构造租户、用户和角色上下文。dev/test 可提供本地测试身份签发器；生产配置必须使用外部 JWT/JWKS，且必须拒绝测试身份。
- 阶段一不实现完整注册、密码登录和找回密码系统。
- Order、Logistics、Refund 和 Escalation 可以持久化 Fake 草单或工单，但必须带 `fake` 来源、幂等键和审计信息，且不得调用真实外部系统。

### 7.3 数据、文件与外部服务

- 没有真实业务数据时，使用完全虚构、版本化的中文电商知识库，覆盖商品、发票、退换货、物流、会员、售后、权限、多跳关系和攻击样本。
- 主要交互语言为中文，英文输入允许正常处理；时间统一以 UTC 存储，前端默认按 `Asia/Shanghai` 展示并允许配置。
- 阶段一支持 PDF、DOCX、XLSX、PPTX、HTML、TXT、Markdown、PNG 和 JPEG。单文件上限 25 MB，文档最多 200 页；旧格式 DOC、XLS、PPT 不支持。
- 本地对象存储统一使用 `./data/uploads/`；数据库只保存对象 Key，不保存机器绝对路径。
- 阶段二真实业务 API 尚无权威契约时，阶段一使用供应商无关的 Order、Logistics、Refund Port；未来由 Adapter 完成真实 API 字段映射。
- Langfuse Cloud 默认只接收脱敏标识、模型、耗时、Token、候选 ID 和分数，不上传完整问题、回答或知识原文；测试环境默认关闭外发。

### 7.4 入库执行方式

- 上传 API 创建持久化任务后立即返回，不在请求中执行完整解析与索引。
- 阶段一使用同一部署单元内的受控任务执行器，从 MySQL 领取任务并限制并发；进程重启后必须能继续领取或安全重试未完成任务。
- 阶段一的单个入库任务仍按 MySQL 真理源、Milvus 向量索引、Neo4j 图谱索引的顺序协调，不部署 Kafka。
- 阶段二以 Kafka Worker 替换任务领取和事件传输时，不改变事件 Envelope、Handler、Port 和任务状态机。

### 7.5 阶段一量化验收门槛

| 指标 | 最低门槛 |
| :--- | :--- |
| Supervisor 路由准确率 | ≥ 95% |
| Recall@20 | ≥ 90% |
| MRR@20 | ≥ 0.80 |
| NDCG@10 | ≥ 0.85 |
| 引用正确率 | ≥ 98% |
| 有证据回答 Faithfulness | ≥ 95% |
| 无答案正确拒答率 | ≥ 95% |
| 越权或跨租户内容进入上下文 | 0 |
| 未审批的真实业务写操作 | 0 |

CI 必须使用确定性 Fake 模型执行硬门禁。真实云模型的延迟和可用性受公网影响，阶段一记录 P50、P95、首 Token 时间和总耗时作为观测指标，但不作为阻塞代码合并的硬 CI 门禁。

### 7.6 FAQ、分块、图谱与检索默认值

- FAQ 以 MySQL 中版本化的 `faq_items` 为真理源，保存租户、规则、标准答案、状态、有效期和版本；语义匹配使用独立的版本化 Milvus Collection，最终命中仍回 MySQL 复核。
- FAQ 精确规则命中可直接回答；语义相似度初始阈值为 0.88。低于阈值或出现冲突时转入 KB，不直接返回标准答案。
- Supervisor 结构化路由的初始置信度阈值为 0.75；低于阈值时澄清或安全升级，不随机选择业务 Agent。
- 检索小块目标长度为 450 个中文字符，硬上限 600，重叠比例 12%；父块目标范围为 1200～1800 个中文字符。表格按结构单元处理，不强行套用字符切分。
- Dense、Graph 和可选 Keyword 分支各取 Top 20，使用 `k=60` 的 RRF 融合，授权过滤后 Rerank，最终保留 Top 5 证据。
- 图谱实体使用固定的 `Entity` 节点类型，通过 `entity_type`、规范化名称和别名区分语义；实体关系统一使用 `RELATED_TO`，具体谓词保存在受控的 `predicate` 属性中，禁止让模型生成任意 Neo4j 关系类型。
- 初始实体类型允许 Product、Policy、Process、Document、Organization、Location、Time 和 Condition；无法安全归类时使用 Other 并保留来源，不武断合并同名实体。
- 以上参数必须配置化。调整阈值、块大小、RRF 或 Top-K 时，必须同步记录配置版本并重新运行 Golden Set。

### 7.7 超时、重试与并发默认值

- MySQL、Redis、Neo4j 和 Milvus 的单次普通操作超时默认 5 秒。
- L2 路由默认超时 8 秒；L3 生成首 Token 默认等待上限 15 秒、完整生成上限 60 秒；Embedding 默认 30 秒；Rerank 默认 15 秒；OCR 单页默认 60 秒。
- 只有只读请求或带稳定幂等键的操作允许自动重试，最多重试 2 次并使用指数退避和随机抖动；鉴权失败、验证失败和非幂等真实写入不得自动重试。
- 阶段一入库任务并发默认 2，Embedding 批量默认 16；所有并发和批量值必须可配置，并受连接池与模型配额限制。
- 超时、重试耗尽和降级必须写入结构化日志、任务状态和 Trace，但不得记录密钥或未经脱敏的正文。

---

## 八、最终检查清单

部署前确认：

- [ ] MySQL 数据库 `enterprise_agent_db` 已创建
- [x] MySQL Binlog 已开启（`log_bin=ON`, `binlog_format=ROW`）
- [ ] Milvus 运行中（`docker ps | grep milvus`）
- [ ] Neo4j 运行中（访问 `http://服务器IP:7474`）
- [x] Redis 已启动（`docker ps | grep redis_agent`）
- [x] `.env` 中所有密码与实际服务一致
- [x] Langfuse Cloud 已注册，密钥已写入 `.env`
