# 阶段一 MVP 环境准备清单

> 基于 `enterprise_multi_agent_architecture.md` 企业级多智能体架构方案的落地方案。
> 本文档总结当前环境状态、缺失组件部署方式、以及阶段一 MVP 的边界范围。

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
| 2  | **Milvus**       | ✅ 已就绪           | Docker              | 端口 19530，Collection:`weview_content_chunks`                       |
| 3  | **Neo4j**        | ✅ 已就绪           | Docker              | Bolt 7687，Web 7474                                                    |
| 4  | **Redis**        | ✅ 已就绪           | Docker              | 密码: `2001612wyj`，端口 6379，AOF 持久化已开启                     |
| 5  | **阿里百炼 LLM** | ✅ 已配置           | 云 API              | L3:`qwen-plus`，L2: `qwen-turbo`，Embedding: `text-embedding-v4` |
| 6  | **Langfuse**     | ✅ 已配置           | Cloud 免费版        | 密钥已写入 `.env`                                                    |
| 7  | **Kafka**        | ⏸ 推迟到阶段二     | Docker              | 阶段一走同步模式，无需消息队列                                         |
| 8  | **审批流通知**   | ⏸ 推迟到阶段二     | 企微/SMTP           | 阶段一无写操作，无审批流需求                                           |
| 9  | **外部业务 API** | ⏸ 阶段一用 Fake    | Mock 实现           | 阶段二接入真实 Order/Logistics/Refund 系统                             |
| 10 | **对象存储**     | ⏸ 阶段一用本地磁盘 | 本地 `./uploads/` | 阶段二文件量大后再上 MinIO/OSS                                         |

---

## 三、需立即执行的操作

### 1. ~~启动 Redis~~ ✅ 已完成

Redis 已在 Docker 中部署并运行，密码: `2001612wyj`，AOF 持久化已开启。

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
- 用户名/密码：与 `.env` 中 `DATABASE_URL` 一致（`root / 2001612`）

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
    public_key="pk-lf-xxx",
    secret_key="sk-lf-xxx",
    host="https://cloud.langfuse.com"
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

阶段一 MVP 只做只读的知识问答（检索 → 生成），不涉及退款、改地址等写操作，因此不需要审批流和企微/邮件通知。

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
        return requests.get(f"https://order-api.xxx.com/orders/{order_id}")
```

### 4.6 为什么对象存储用本地文件系统

文档解析管道需要存储原始文件（PDF/Word/图片）。阶段一只有单机部署、文件量小，直接用本地目录：

```python
UPLOAD_DIR = "./uploads"
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
| 写操作（退款/改地址）      | ❌     | ✅     |
| Human-in-the-loop 审批     | ❌     | ✅     |
| 异步数据同步（Kafka）      | ❌     | ✅     |
| 多租户隔离                 | ❌     | ✅     |
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

## 七、最终检查清单

部署前确认：

- [ ] MySQL 数据库 `enterprise_agent_db` 已创建
- [x] MySQL Binlog 已开启（`log_bin=ON`, `binlog_format=ROW`）
- [ ] Milvus 运行中（`docker ps | grep milvus`）
- [ ] Neo4j 运行中（访问 `http://服务器IP:7474`）
- [x] Redis 已启动（`docker ps | grep redis_agent`）
- [x] `.env` 中所有密码与实际服务一致
- [x] Langfuse Cloud 已注册，密钥已写入 `.env`
