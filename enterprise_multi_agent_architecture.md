# 企业级多智能体（Multi-Agent）智能客服系统架构方案

## 一、 项目背景与业务挑战

在构建面向 C 端海量用户或复杂 B 端 SaaS 业务的智能客服 Agent 时，传统的单体 LLM + RAG 架构（如简单的 LangChain Demo）在生产环境中面临巨大挑战：
1. **成本与延迟**：所有问题均调用千亿参数模型导致极高 Token 成本与响应延迟。
2. **数据安全与幻觉资损**：模型幻觉导致越权查阅机密知识，或在执行"退款/修改订单"等写操作时造成直接资金损失。
3. **数据一致性灾难**：关系型业务库（MySQL）与向量库（Milvus）、图数据库（Neo4j）之间存在数据孤岛，导致回答使用过期状态数据。
4. **复杂业务无法单 Agent 闭环**：真实客服场景涉及查单、查物流、退款、开票、投诉升级等多领域任务，单体 Agent 的 Context 与工具列表会无限膨胀，导致工具误调用与推理退化。

为此，本方案采用 **"分层解耦、多智能体协同、渐进式降级、多库协同"** 的微服务架构，以 **LangGraph** 作为多 Agent 编排引擎，确保系统的高可用（HA）、低成本与强安全。

---

## 二、 核心架构设计 (5 层体系)

> 说明：本章描述的 5 层是系统的基础骨架。多智能体协同（基于 LangGraph）横跨第 3、4 层，详见 **第三章**。

### 1. 接入层 (Gateway Layer)
*   **职责**：协议适配（WebSocket/HTTP）、鉴权、限流熔断。
*   **技术栈**：Nginx + Spring Cloud Gateway / Kong。
*   **关键设计**：拦截恶意流量，对不同渠道（APP、微信、Web）进行统一的 Context 封装。

### 2. Context 与会话状态管理层 (State Management)
*   **职责**：大模型无状态，需维护用户的短期记忆与长线业务状态。
*   **技术栈**：Redis Cluster。
*   **关键设计**：
    *   采用 **Hash 结构** 存储 `Session_ID`。
    *   **字段拆分**：分离 `chat_history`（滑动窗口保留最近 10 轮）、`user_profile`（VIP等级）、`fsm_state`（有限状态机，如 `waiting_for_photo`）。
    *   通过 TTL 防内存溢出，热点工具结果（如查物流）加入短缓存（防缓存击穿采用 Mutex Lock）。
    *   LangGraph 的 Checkpointer 对接 Redis，实现 Agent 状态图的持久化与断点续跑。

### 3. 多模型级联意图路由层 (Cascading Intent Router)
*   **职责**：控制成本与延迟，实现渐进式降级。
*   **技术栈**：规则引擎 + Embedding + 多尺度 LLM。
*   **关键设计（降级漏斗）**：
    *   **L1 (0耗时)**：规则引擎 & 正则匹配。处理"怎么开发票"等标准 SOP。
    *   **L2 (低成本)**：轻量级模型（如 Qwen-14B / GLM-4-9B）。处理普通意图分类与简单寒暄。
    *   **L3 (高性能)**：旗舰模型（如 GPT-4o / DeepSeek-V3）。专职处理复杂情绪安抚、多条件推理与工单升级决策。
    *   路由结果作为 LangGraph 的**条件边 (Conditional Edge)** 入口，决定进入哪个专家 Agent。

### 4. 工具调度与执行层 (Tool & Action Layer)
*   **职责**：安全可靠地执行外部 API 调用（读/写操作）。
*   **关键设计（防资损防幻觉的核心）**：
    *   **动态路由**：根据 L3 解析的意图，动态向当前 Agent 注入所需的 Tool Schema，防止 Context 溢出。
    *   **读写分离校验**：
        *   *读操作*：直接调用，结果脱敏。
        *   *写操作（如退款）*：**原子化拆解**。大模型仅允许调用 `CalculateRefund()` 和 `CreateDraft()`。
    *   **Human-in-the-loop (人在回路)**：致命 (Critical) 操作强制挂起，触发异步审批流。大模型回复："已为您生成退款申请，正在等待主管审核"。真实员工在企微点击通过后，回调完成闭环。在 LangGraph 中通过 `interrupt_before` 节点实现状态挂起。

### 5. 数据治理与融合检索层 (Data & Retrieval Layer)
本层采用 **MySQL + Milvus + Neo4j** 的企业级经典协同架构。核心思想：**各司其职，优势互补，以全局唯一 `Chunk_ID` 作为数据贯穿的绝对锚点。**

*   **MySQL (真理源与业务底座)**：存储工单、用户画像、文档元数据，以及**知识块高清原文 (Content)** 和动态发布状态、细粒度权限标签。利用 B+ 树优势处理高频状态变更与复杂鉴权。
*   **Milvus (专用向量召回引擎)**：仅存储 `Chunk_ID` 和 `Vector`，外加粗粒度租户标签（如 `tenant_id`）。专注大规模向量近似检索，保持索引结构轻量化。
*   **Neo4j (逻辑与推理大脑)**：存储 `Chunk_ID` 节点与提取出的业务实体（Entity）、关系（Relation）。负责多跳推理(Multi-hop Reasoning)与结构化上下文补全。

#### 5.1 数据入库与对齐机制 (Data Ingestion)
1. **分块与发号**：文档切分后，为每个文本块生成全局唯一的 `Chunk_ID` (UUID)。
2. **MySQL 本地事务 + 派生索引异步写入**：
   - MySQL 在同一个本地事务中写入原文、状态、权限、`Chunk_ID` 与 `outbox_events`，作为唯一真理源。
   - `embedding-worker` 消费 Outbox/Kafka 事件，生成向量并 upsert 到 Milvus。
   - `kg-worker` 消费 Outbox/Kafka 事件，通过 LLM 抽取实体关系，并 upsert 到 Neo4j，建立以 `Chunk_ID` 为起点的知识子图 `(Chunk)-[:MENTIONS]->(Entity)`。
   - Worker 必须基于 `event_id`、`Chunk_ID`、`content_hash` 做幂等处理，支持失败重试、死信队列与人工重放。

#### 5.2 终极检索链路：双路入口 + 混合过滤 + 图谱桥接 (Dual-Entry & Hybrid Filtering)
为了降低传统纯向量 RAG 的漏召回风险，检索链路采用以下工业级流水线：

1. **双路并发入口 (Dual-Entry)**：
   - **左路（向量前置过滤）**：携带用户的租户/部门标签去 Milvus 进行粗筛（`expr` 过滤），避免全库暴力扫描，拿到 Top 50 候选 `Chunk_ID`。
   - **右路（实体精准直达）**：利用 NER 或小模型快速提取用户问题中的实体（如"蒂姆·库克"），直接去 Neo4j 精准匹配关联节点，彻底摆脱对向量检索的单一依赖。
2. **后置业务精洗 (MySQL Post-filtering)**：
   - 将左路召回的 `Chunk_ID` 送入 MySQL 进行严苛的权限与上下线状态 `IN` 查询。
   - **作用**：剔除已下线或越权的数据，降低过期/越权数据进入上下文的风险，并在同一次批量查询中拉取合法原文。
3. **图谱桥接增强 (Neo4j Bridge Effect)**：
   - 拿着合法的 `Chunk_ID` 和右路命中的实体，在 Neo4j 中向外扩展 1~2 层。
   - **作用**：用图谱的物理连线，强行把那些"语义不相似，但逻辑强相关"的隐藏信息给"拽"回来（例如通过"库克"实体，关联出向量未召回的"奥本大学"片段）。
4. **扩展候选二次鉴权 (Authorization Recheck)**：
   - Neo4j 扩展出的新 `Chunk_ID` 不能直接读取原文，必须再次回到 MySQL 做租户、状态、版本、ACL/ABAC 校验。
   - 只有通过 MySQL 校验的 Chunk 才允许进入最终 Context，避免图谱扩展绕过权限边界。
5. **融合生成 (LLM Generation)**：
   - 将 MySQL 洗出的**合法原文** + Neo4j 拉出的**图谱扩展关系**组合成结构化 Context，喂给 LLM。

---

## 三、 多智能体协同架构 (Multi-Agent Orchestration based on LangGraph)

第二章的 5 层体系解决了"单 Agent + RAG"的工程化问题。但在真实客服场景中，一个会话往往横跨多个业务域（查单、物流、退款、开票、投诉），单体 Agent 会因工具列表膨胀、Context 过载、角色冲突而退化。因此本方案引入 **多智能体协同架构**，采用 **LangGraph** 作为编排引擎。

### 1. 设计原则
*   **角色专业化**：每个 Agent 只负责一个业务域，工具列表精简，Prompt 聚焦，降低误调用率。
*   **显式状态流转**：Agent 间的协作通过共享 State 显式传递，而非隐式 Context 拼接，可追溯、可重放。
*   **可控边界**：Supervisor 决定何时 Handoff、何时收敛、何时转人工，避免 Agent 失控死循环。
*   **可观测**：LangGraph 的每一步节点执行天然对应一个 Trace Span，便于全链路审计。

### 2. Agent 角色定义与拓扑

```
                         ┌─────────────────┐
                         │  Supervisor Agent│  (路由分发 / 收敛 / 转人工决策)
                         │  (LangGraph Entry)│
                         └────────┬────────┘
            ┌─────────────┬───────┴────────┬──────────────┐
            ▼             ▼                ▼              ▼
     ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
     │ Order Agent│ │Logistics   │ │ Refund     │ │ KB/Q&A     │
     │ (查单/改单) │ │Agent(物流) │ │ Agent(退款) │ │ Agent(知识) │
     └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
           │              │              │              │
           ▼              ▼              ▼              ▼
     [Order Tools]  [Track Tools]  [Refund Tools]  [RAG Retrieval]
                                                (Milvus+Neo4j+MySQL)
```

| Agent | 职责 | 绑定工具 | 使用模型 |
| :--- | :--- | :--- | :--- |
| **Supervisor Agent** | 意图分发、多 Agent 结果收敛、冲突仲裁、转人工决策、会话终止判断 | 无业务工具，仅做路由 | L2 小模型（低成本高频） |
| **Order Agent** | 订单查询、状态跟踪、修改收货地址（只读+受限写） | `QueryOrder`, `UpdateAddress` | L3 旗舰模型 |
| **Logistics Agent** | 物流轨迹查询、催单 | `QueryLogistics`, `UrgeDelivery` | L2 小模型 |
| **Refund Agent** | 退款计算、草单生成、审批流触发（写操作强约束） | `CalculateRefund`, `CreateDraft`, `SubmitApproval` | L3 旗舰模型 |
| **KB Agent** | 知识库问答（GraphRAG 检索链路） | `HybridRetrieve`（调用第二章 5.2 链路） | L3 旗舰模型 |
| **Escalation Agent** | 情绪安抚、投诉升级、转接坐席 | `CreateTicket`, `TransferToHuman` | L3 旗舰模型 |

### 3. LangGraph 状态图 (StateGraph) 设计

核心是定义一个**全局共享状态** `AgentState`，所有节点读写该状态：

```python
from typing import TypedDict, List, Literal, Annotated
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.redis import RedisSaver
import operator

class AgentState(TypedDict):
    # 输入域
    session_id: str
    tenant_id: str
    user_id: str
    roles: List[str]              # RBAC 上下文，贯穿权限校验
    query: str                    # 当前用户提问（经 Query Rewrite 后）
    chat_history: List[dict]      # 滑动窗口历史
    # 编排域
    next_agent: str               # Supervisor 决定的下一个 Agent
    visited_agents: Annotated[List[str], operator.add]  # 累加已访问 Agent，防环路
    iteration: int                # 循环计数，硬阈值防爆栈
    # 结果域
    retrieved_chunks: List[str]   # 召回的 chunk_id（待 MySQL 鉴权）
    evidence_context: str         # 最终进入 LLM 的合法上下文
    tool_calls: List[dict]        # 工具调用审计
    draft_action: dict            # 待审批的写操作草单
    # 输出域
    final_answer: str
    needs_human: bool             # 是否转人工
    needs_approval: bool          # 是否触发审批流
```

**图结构**：

```python
graph = StateGraph(AgentState)

# 节点注册
graph.add_node("supervisor", supervisor_node)
graph.add_node("order", order_agent_node)
graph.add_node("logistics", logistics_agent_node)
graph.add_node("refund", refund_agent_node)
graph.add_node("kb", kb_agent_node)
graph.add_node("escalation", escalation_node)
graph.add_node("synthesize", synthesize_node)  # 结果收敛与答案生成

# 入口
graph.set_entry_point("supervisor")

# Supervisor 条件分发（条件边）
graph.add_conditional_edges(
    "supervisor",
    route_by_intent,  # 返回 "order"|"logistics"|"refund"|"kb"|"escalation"|"synthesize"|"human"|"end"
    {
        "order": "order",
        "logistics": "logistics",
        "refund": "refund",
        "kb": "kb",
        "escalation": "escalation",
        "synthesize": "synthesize",
        "human": "human_handoff",
        "end": END,
    },
)

# 每个专家 Agent 执行完后回到 Supervisor（支持多轮编排）
for agent in ["order", "logistics", "refund", "kb", "escalation"]:
    graph.add_edge(agent, "supervisor")

# 收敛节点后结束
graph.add_edge("synthesize", END)

# 编译，挂载 Redis Checkpointer 实现状态持久化与断点续跑
app = graph.compile(
    checkpointer=RedisSaver(redis_client),
    interrupt_before=["refund"],  # 退款节点前挂起，触发 Human-in-the-loop
)
```

### 4. Agent 间通信与 Handoff 机制

LangGraph 的 Handoff 不依赖额外消息总线，而是通过**共享 State 的读改写**实现：

*   **Supervisor → 专家 Agent**：Supervisor 将 `query`、`roles`、`chat_history` 写入 State，设置 `next_agent`，专家 Agent 读取后执行。
*   **专家 Agent → Supervisor**：专家 Agent 将执行结果（`retrieved_chunks`、`tool_calls`、`draft_action`）写回 State，控制权回到 Supervisor。
*   **多 Agent 协同**：Supervisor 可在单轮内依次调度多个 Agent（如先 `Order` 查单 → 再 `Logistics` 查物流），通过 `visited_agents` 追踪路径，避免环路。
*   **防环路保护**：`iteration` 超过硬阈值（如 5 次）时强制跳转 `synthesize` 或转人工。

### 5. 子 Agent 生命周期管理

*   **创建**：Supervisor 按需激活专家 Agent，无需预加载全部工具，降低 Context 占用。
*   **复用**：同一会话内 Agent 实例通过 Checkpointer 复用历史状态，避免重复检索。
*   **隔离**：每个 Agent 的 Prompt、工具列表、权限上下文相互隔离，互不污染。
*   **销毁**：会话结束或 TTL 过期后，Redis 中的 Checkpoint 自动清理。

### 6. 多 Agent 状态共享与冲突仲裁

当多个 Agent 返回冲突结果（如 Order Agent 说"已发货"，Logistics Agent 说"无轨迹"）时：

*   **置信度标注**：每个 Agent 结果携带 `confidence` 分数。
*   **Supervisor 仲裁**：Supervisor 比较置信度与数据时效性，选择高置信结果或触发二次校验。
*   **冲突记录**：冲突事件写入审计日志，供后续 Bad Case 归因。

### 7. Agent 编排与降级策略

| 场景 | 降级策略 |
| :--- | :--- |
| 旗舰模型不可用 | Supervisor 降级为 L2 小模型 + 规则路由，牺牲推理深度保可用 |
| 某专家 Agent 工具超时 | 该 Agent 返回降级话术，Supervisor 决定转人工或换路 |
| LangGraph 编排异常 | 降级为单体 Agent 直连 KB 检索（回退到第二章基础链路） |
| 审批流超时未响应 | Refund Agent 草单自动作废，通知用户"审核超时请重试" |

### 8. Human-in-the-loop 落地（LangGraph interrupt）

LangGraph 原生支持 `interrupt_before` / `interrupt_after`：

1. 用户请求退款 → Supervisor 调度 Refund Agent。
2. LangGraph 在 Refund 节点前**挂起**，状态持久化到 Redis。
3. 系统通知用户"正在生成退款申请"。
4. Refund Agent 调用 `CreateDraft()` 生成草单，写入 `draft_action`。
5. 通过 `interrupt_before` 挂起，推送审批到企微。
6. 主管审批通过 → 回调恢复 LangGraph 执行，调用 `SubmitApproval()` 闭环。
7. 审批拒绝 → Supervisor 转入安抚话术。

---

## 四、 高级工程实践与系统保障

### 1. 数据一致性保障 (Data Consistency)
*   **异构库同步**：运营人员在后台修改 MySQL 知识库后，通过 **Canal 监听 Binlog**，推送到 Kafka。Milvus 和 Neo4j 异步消费消息更新自身，实现无感知的最终一致性。
*   **分布式事务**：跨微服务的退款等写操作，采用 **Saga 补偿模式**。若后续步骤失败，自动触发反向补偿脚本（如回退订单状态），确保业务完整性。
*   **Outbox 模式**：MySQL 写入与 Outbox 事件在同一本地事务中提交，保证"要么都成功要么都失败"，再由 Worker 消费 Outbox 投递到 Kafka，避免数据与消息不一致。

### 2. 向量库性能优化
*   **软删除 (Soft Delete)**：避免在 Milvus 中执行昂贵的物理删除。通过 MySQL 同步 `is_deleted=1` 标签，在后置过滤阶段拦截，利用夜间定时任务异步执行物理 Compaction。
*   **冷热分层**：高频知识向量常驻内存（HNSW 索引），长尾归档知识降级存储。

### 3. 可观测性平台化建设 (Observability)
建立 **Metrics + Logs + Traces** 三位一体的统一可观测体系：
*   **链路追踪 (Traces)**：每次请求携带 `request_id`，贯穿 API → Supervisor → 各 Agent → Milvus/Neo4j/MySQL → Rerank → LLM。LangGraph 每个节点天然对应一个 Span。推荐 Jaeger / Tempo。
*   **指标监控 (Metrics)**：Prometheus 采集，Grafana 展示。核心指标：
    *   路由层：L1/L2/L3 命中分布、降级率。
    *   Agent 层：各专家 Agent 调用频次、工具调用成功率、平均迭代轮次。
    *   检索层：Milvus 查询延迟、TopK 命中数、MySQL 鉴权过滤比例、Reranker 延迟。
    *   生成层：LLM Token 消耗、TTFT、拒答率、异常率。
    *   一致性：Outbox 堆积量、Worker 重试次数、三库索引差异计数。
*   **日志 (Logs)**：结构化 JSON 日志，接入 Loki / ELK。默认记录 `query_hash` 与 `chunk_id`，**不记录完整敏感原文**。支持按 `session_id`、`request_id` 检索全链路日志。
*   **LLM 专用追踪**：Phoenix / Langfuse 记录 LLM 入参快照、检索的 Context 片段、Tool 耗时，为 Bad Case 归因与数据飞轮提供语料池。

### 4. 微服务与服务治理 (Microservice & Service Governance)
*   **服务拆分**：
    | 微服务 | 职责 |
    | :--- | :--- |
    | `gateway-service` | 接入、鉴权、限流 |
    | `agent-orchestrator` | LangGraph 编排引擎（Supervisor + 专家 Agent） |
    | `retrieval-service` | 多路召回 + 融合 + 重排 |
    | `ingest-service` | 文档解析、分块、入库任务管理 |
    | `embedding-worker` | 向量生成 Worker |
    | `kg-worker` | 图谱抽取 Worker |
    | `auth-service` | RBAC/ABAC 权限校验 |
    | `tool-service` | 外部业务 API 代理（订单、物流、退款） |
    | `approval-service` | 审批流引擎 |
    | `admin-service` | 运营管理后台 |
*   **服务注册发现**：Nacos / Consul，支持健康检查与服务上下线。
*   **服务间通信**：内部 gRPC（低延迟），对外 REST + OpenAPI 契约。
*   **舱壁隔离 (Bulkhead)**：不同业务域线程池隔离，防止某 Agent 工具超时拖垮全局。
*   **熔断降级**：Sentinel / Resilience4j，对 LLM、Milvus、Neo4j、外部业务 API 设置独立熔断器。

### 5. 领域模型与服务边界 (Domain Model & Bounded Context)

多 Agent 不是按代码文件拆角色，而是按业务边界拆职责。每个 Agent 只能通过明确的服务契约访问自己被授权的领域能力，禁止直接跨库读取其他领域数据。

| 边界上下文 | 核心实体/聚合 | 真理源 | 主要服务 | 对外能力 |
| :--- | :--- | :--- | :--- | :--- |
| Identity & Tenant | User, Role, Tenant, Group | MySQL / IAM | `auth-service` | 登录态解析、RBAC/ABAC、租户配额 |
| Conversation | Session, Message, ChannelProfile | Redis + MySQL | `conversation-service` | 会话归档、上下文窗口、渠道适配 |
| Agent Orchestration | AgentRun, AgentState, Handoff | Redis Checkpoint + MySQL | `agent-orchestrator` | 路由、状态机、Agent 编排 |
| Order | Order, OrderItem, Address | 订单业务库 | `order-service` | 查单、改地址、订单状态快照 |
| Logistics | Shipment, TrackingEvent | 物流系统 | `logistics-service` | 查轨迹、催单、异常物流判断 |
| Refund | RefundDraft, RefundRequest | 退款业务库 | `refund-service` | 退款计算、草单、提交审批 |
| Approval | ApprovalTask, ApprovalDecision | 审批库 | `approval-service` | 审批流、回调、超时作废 |
| Knowledge | Document, Chunk, Entity, Relation | MySQL + Milvus + Neo4j | `retrieval-service`, `ingest-service` | GraphRAG 检索、知识入库 |
| Audit & Evaluation | AuditLog, EvalCase, BadCase | 审计库 / 评测库 | `audit-service`, `eval-service` | 审计追踪、评测门禁、数据飞轮 |

边界规则：

*   每个领域只能修改自己的聚合根，跨领域协作通过 API 或领域事件完成。
*   Agent 不直接访问数据库，只调用 Tool/API；Tool/API 内部再执行业务权限校验。
*   跨领域展示使用快照，例如退款流程读取订单信息时保存 `order_snapshot`，避免后续订单变化导致审批依据漂移。
*   关键状态机必须由业务服务持有，不能只存在 LLM 对话上下文中。

### 6. 核心数据模型与存储边界 (Core Data Model)

正式实现前必须先固化核心表和存储边界，否则权限、审计、恢复和评测都无法闭环。

| 数据域 | 建议表/结构 | 关键字段 | 说明 |
| :--- | :--- | :--- | :--- |
| 会话 | `sessions`, `messages` | `session_id`, `tenant_id`, `user_id`, `channel`, `status`, `created_at` | 会话归档、上下文回放、客服质检 |
| Agent 执行 | `agent_runs`, `agent_steps` | `run_id`, `session_id`, `agent_name`, `state_version`, `latency_ms`, `status` | 记录 LangGraph 每个节点执行 |
| 工具调用 | `tool_call_logs` | `tool_call_id`, `run_id`, `tool_name`, `request_hash`, `response_hash`, `decision` | 工具审计、失败归因、幂等排查 |
| 写操作草单 | `action_drafts` | `draft_id`, `action_type`, `risk_level`, `payload`, `idempotency_key`, `status` | 退款、改地址等先生成草单 |
| 审批 | `approval_tasks`, `approval_events` | `approval_id`, `draft_id`, `approver`, `decision`, `expired_at` | Human-in-the-loop 闭环 |
| 知识库 | `documents`, `chunks`, `chunk_acl`, `outbox_events` | `doc_id`, `chunk_id`, `content_hash`, `acl_policy_id`, `event_id` | MySQL 真理源与索引事件 |
| 评测 | `eval_sets`, `eval_cases`, `eval_runs`, `eval_results` | `case_id`, `expected_chunk_id`, `metric`, `score` | 发布门禁与回归测试 |
| 审计 | `audit_logs`, `data_access_logs` | `request_id`, `actor_id`, `resource_type`, `resource_id`, `action` | 合规、追责、问题复盘 |

存储边界：

*   MySQL 保存业务事实、原文、权限、审计和状态；Milvus/Neo4j 是派生索引。
*   Redis 保存短期会话状态和 LangGraph checkpoint，长期可追溯状态必须异步归档到 MySQL。
*   日志系统默认保存 hash、ID、耗时和决策，不保存完整敏感原文。
*   所有表必须包含 `tenant_id` 或能通过父实体关联到 `tenant_id`，避免跨租户查询漏洞。

### 7. API 与 Tool 契约治理 (API & Tool Contract)

所有 Agent Tool 必须像正式 API 一样治理，不能只靠 Prompt 描述。每个 Tool 至少定义：

| 字段 | 要求 |
| :--- | :--- |
| `name` | 全局唯一，如 `refund.create_draft.v1` |
| `description` | 面向模型的短描述，禁止包含模糊副作用 |
| `input_schema` | JSON Schema / Pydantic Model，字段类型、枚举、必填项明确 |
| `output_schema` | 标准返回结构，包含 `success`, `data`, `error_code`, `trace_id` |
| `auth_policy` | 调用所需角色、属性、租户边界和数据范围 |
| `risk_level` | `read`, `low_write`, `critical_write` |
| `idempotency_key` | 写操作必填，防止重复扣款、重复退款、重复改地址 |
| `timeout_ms` | 单工具超时阈值，默认不超过 3 秒 |
| `retry_policy` | 是否可重试、最大次数、退避策略 |
| `audit_policy` | 入参脱敏、出参脱敏、是否记录审批依据 |

示例契约：

```json
{
  "name": "refund.create_draft.v1",
  "risk_level": "critical_write",
  "required_roles": ["customer_service"],
  "idempotency_key": "tenant_id + order_id + user_id + action_type",
  "input_schema": {
    "order_id": "string",
    "reason_code": "enum",
    "requested_amount": "decimal",
    "evidence_ids": "array<string>"
  },
  "output_schema": {
    "draft_id": "string",
    "risk_level": "enum",
    "approval_required": "boolean",
    "expires_at": "datetime"
  }
}
```

契约治理规则：

*   Tool Schema 版本化，破坏性变更必须新增版本，不能原地修改。
*   Agent 只能看到当前任务允许的 Tool 子集，禁止一次性注入全量工具。
*   Tool 返回错误必须结构化，Supervisor 根据错误码决定重试、降级、转人工或终止。
*   生产环境禁止 Agent 调用未注册、未鉴权、未审计的动态工具。

### 8. 事件驱动架构、幂等与死信 (Event-Driven Architecture)

Kafka/Outbox 不只是消息通道，还需要明确事件契约、消费语义和恢复策略。

| Topic | 事件 | 生产者 | 消费者 | 用途 |
| :--- | :--- | :--- | :--- | :--- |
| `knowledge.events` | `knowledge.chunk.created`, `knowledge.chunk.updated`, `knowledge.chunk.deleted` | `ingest-service` | `embedding-worker`, `kg-worker` | 构建 Milvus/Neo4j 派生索引 |
| `agent.events` | `agent.run.started`, `agent.step.completed`, `agent.run.failed` | `agent-orchestrator` | `audit-service`, `eval-service` | Trace、审计、Bad Case 采集 |
| `action.events` | `action.draft.created`, `action.approved`, `action.rejected`, `action.expired` | `tool-service`, `approval-service` | `agent-orchestrator`, `audit-service` | 写操作审批闭环 |
| `eval.events` | `eval.run.requested`, `eval.run.completed` | CI/CD, Admin | `eval-service` | 发布前评测门禁 |

事件 Envelope 统一结构：

```json
{
  "event_id": "uuid",
  "event_type": "knowledge.chunk.updated",
  "event_version": "1.0",
  "occurred_at": "datetime",
  "tenant_id": "string",
  "aggregate_id": "chunk_id",
  "trace_id": "request_id",
  "payload": {},
  "payload_hash": "sha256"
}
```

硬性要求：

*   所有消费者必须幂等，使用 `event_id` 去重，使用 `aggregate_id + version` 防止乱序覆盖。
*   消费失败进入 DLQ，DLQ 事件必须支持人工查看、修复、重放。
*   事件 Schema 版本化，推荐接入 Schema Registry 或在代码仓库中维护 JSON Schema。
*   对退款、审批、知识发布等关键事件保留重放能力，支持灾难恢复和审计复盘。

### 9. 写操作风控与审批矩阵 (Action Risk Control)

智能客服系统的高风险点不是“答错一句话”，而是错误执行写操作。所有写操作必须经过风险分级、幂等、审批和审计。

| 操作 | 风险等级 | 自动执行条件 | 审批要求 | 必备保护 |
| :--- | :--- | :--- | :--- | :--- |
| 查订单 / 查物流 | Read | 用户身份和订单归属校验通过 | 无 | 出参脱敏、访问审计 |
| 催单 | Low Write | 同一订单未超过频率限制 | 可选 | 幂等键、频控、结果回执 |
| 修改地址 | Medium Write | 未出库、地址变更次数未超限 | 高价值订单需审批 | 地址校验、订单快照、撤销策略 |
| 退款草单 | Critical Write | 只允许生成草单，不直接退款 | 必须审批 | 金额阈值、黑名单、重复检测、审批留痕 |
| 提交退款 | Critical Write | 审批通过且草单未过期 | 必须审批 | Saga 补偿、资金流水审计、不可重复提交 |
| 批量导出 | Critical Read | 管理员且有工单理由 | 必须审批 | 水印、脱敏、下载审计 |

风控规则：

*   Agent 只能生成 `draft_action`，不能直接执行高风险写操作。
*   写操作必须携带 `idempotency_key`，服务端拒绝重复提交。
*   审批矩阵按金额、租户等级、用户角色、风险标签动态决定审批人。
*   所有写操作保存执行前快照、执行后结果和操作者，支持追责和回滚。
*   风控命中时优先转人工，不允许模型自行解释并绕过规则。

### 10. Agent 失败语义、恢复与运行手册 (Failure Semantics & Runbook)

多 Agent 系统必须定义失败语义，否则线上问题会表现为“模型偶发异常”，难以定位。

| 失败类型 | 判定方式 | 系统动作 | 记录内容 |
| :--- | :--- | :--- | :--- |
| 路由失败 | 意图置信度低于阈值或多意图冲突 | 追问澄清或转人工 | 候选意图、置信度、用户原句 |
| Tool Schema 校验失败 | LLM 输出不符合 JSON Schema | 最多重试 1 次，仍失败则转人工 | 错误字段、修正前后入参 |
| 工具超时 | 超过 `timeout_ms` | 熔断、降级话术、可选转人工 | tool_name、latency、依赖服务 |
| 工具业务失败 | 返回明确错误码 | Supervisor 按错误码决策 | error_code、业务状态快照 |
| Agent 环路 | `iteration` 超过阈值 | 强制进入 synthesize 或转人工 | visited_agents、state_diff |
| 审批回调丢失 | 草单超过 `expired_at` 未完成 | 草单作废并通知用户 | draft_id、approval_id |
| 状态恢复失败 | Checkpoint 不存在或版本不匹配 | 从 MySQL 归档状态恢复，不可恢复则转人工 | state_version、checkpoint_id |
| LLM 输出风险 | 命中越权、PII、越狱或高毒性 | 阻断输出并转人工 | safety_rule、output_hash |

运行手册要求：

*   每个 P1/P2 告警必须有 Runbook，包含影响范围、排查命令、回滚方式、升级联系人。
*   建立 On-call 值班机制和故障分级：P0 资损/数据泄露，P1 大面积不可用，P2 局部降级。
*   事故复盘必须沉淀为评测集或自动化告警规则，进入数据飞轮。
*   对模型、Prompt、知识库、工具契约的变更都必须能定位到版本和责任人。

### 11. 多租户隔离与成本治理 (Multi-Tenancy & Cost Governance)
*   **租户隔离策略**：
    *   **MySQL**：按 `tenant_id` 行级隔离，核心表强制带 `tenant_id` 索引；高安全租户可独立 Schema。
    *   **Milvus**：按租户分 Partition 或独立 Collection（百亿级租户分库）。
    *   **Neo4j**：所有节点/关系携带 `tenant_id`，查询必须 tenant-scoped；大租户使用独立 Database。
    *   **Redis**：Key 命名空间隔离 `tenant:{id}:session:{sid}`。
*   **租户配额**：QPS 配额、日 Token 预算、存储配额、调用次数上限，超限返回友好限流提示。
*   **成本归因**：每个 `request_id` 记录其消耗的 Token 数、模型档位、检索次数，归因到 `tenant_id` / `user_id` / `session_id`。
*   **成本看板**：按渠道、模型、租户、意图维度统计成本，支持成本预警与预算告警。
*   **成本优化闭环**：监控降级漏斗各层命中率，L1 命中率低时优化规则库，L2 命中率低时优化分类器，持续压降 L3 调用量。

### 12. 高可用与容灾 (HA & Disaster Recovery)
*   **SLO 定义**：
    *   在线问答可用性 ≥ 99.9%。
    *   在线问答 P95 延迟 < 5 秒（含 LLM 生成）。
    *   检索编排 P95 < 1.5 秒。
    *   文档发布后 5 分钟内可检索。
    *   权限误召回进入上下文数量 = 0。
*   **多组件高可用**：
    | 组件 | 高可用方案 |
    | :--- | :--- |
    | MySQL | 主从复制 + 半同步，定期备份 + PITR，跨可用区部署 |
    | Redis | 哨兵或 Cluster 模式，AOF + RDB 双持久化 |
    | Milvus | 生产环境 cluster 模式，多 QueryNode |
    | Neo4j | 因果集群或企业版集群 |
    | Kafka | Kraft 模式 3 节点，副本因子 ≥ 2 |
    | Agent 服务 | 无状态化，多实例 + 负载均衡，水平扩展 |
*   **降级链路闭环**：
    *   Neo4j 不可用 → 降级向量 + 关键词检索。
    *   Milvus 不可用 → 降级关键词 + 图谱实体检索。
    *   Reranker 不可用 → 使用融合分数排序，降低 TopK。
    *   LLM 不可用 → 返回检索摘要或友好错误。
    *   **权限系统不可用 → 一律拒绝返回内容，禁止绕过授权。**
    *   降级触发条件、自动恢复、降级告警、降级期间数据补偿均纳入监控。
*   **混沌工程**：定期故障演练，主动注入 Milvus/Neo4j/LLM 故障，验证降级链路与告警时效。
*   **RTO/RPO 目标**：MySQL RPO ≤ 1 分钟，RTO ≤ 30 分钟；灾难恢复后必须执行三库索引一致性校验。

### 13. 配置中心与动态治理 (Config Center)
*   **技术栈**：Nacos / Apollo。
*   **动态配置项**：Prompt 模板、模型路由策略、Top-K 阈值、熔断参数、限流阈值、降级开关、Feature Flag。
*   **灰度配置**：按租户/用户分桶灰度发布新 Prompt 或新模型。
*   **配置版本管理**：配置变更记录版本，支持一键回滚。

---

## 五、 生产级核心难题与硬核解决方案 (Advanced Engineering Deep Dive)

在真实的高并发生产环境中，仅仅打通链路是不够的。以下是决定系统生死存亡的核心技术攻坚方案：

### 1. RAG 性能瓶颈与全链路加速
RAG 上线变慢是通病，必须在各个节点进行极限压榨：
*   **检索加速 (Retrieval)**：放弃暴力检索，全面转向 HNSW 或 IVF_PQ 索引。使用多线程并发执行左路(Milvus)和右路(Neo4j)的查询。
*   **重排加速 (Rerank)**：Cross-Encoder 模型极慢。策略是限制重排数量（如 Top-20 进重排，输出 Top-5），并部署专门的 GPU 推理服务（如 TEI/vLLM）来支撑重排。
*   **上下文构造加速 (Context)**：严格控制输入长度，采用 KV-Cache 复用技术（如果自建推理服务）。
*   **模型生成加速 (Generation)**：全面采用 **流式输出 (SSE, Server-Sent Events)**，优化 TTFT（首字响应时间）。
*   **外部依赖 (External API)**：所有外部工具调用必须设置严格的 Timeout（如 3秒），并配置熔断降级策略（返回"系统查询超时"而非让大模型死等）。

### 2. 意图识别与大模型路由策略
*   **多路混合意图识别**：稳妥的做法绝不是单靠大模型盲猜。标准流程是：**规则正则 (Regex) -> 语义向量 (Embedding 相似度) -> 小模型分类器 -> 大模型深层提取**。
*   **输出结构化 (Stable JSON)**：强制稳定输出 JSON 的方法不仅是 Prompt 里写一句"请输出 JSON"，更要结合 OpenAI 的 `response_format: {"type": "json_object"}`、Function Calling (Tool Use)，并在代码层使用 Pydantic 做强校验，配合 Retry 机制将错误栈扔给大模型让其自我修正。

### 3. Query Rewrite 与检索重写 (Query Translation)
这两者并非一回事，必须分层处理：
*   **Query Rewrite (问题重写)**：发生在最前端。解决用户的口语化、指代代词（如"那个怎么弄？"）。通常结合聊天历史，用极快的小模型将当前提问改写为上下文完整的独立句子。
*   **Retrieval Rewrite (检索重写)**：发生在进入 Milvus 前。解决语义鸿沟。采用 Multi-Query（将一个问题扩展为多个角度的子问题）或 HyDE（先让大模型假答一段，用假答案的向量去检索真实文档），以提高向量召回的命中率。

### 4. Agent 的多维状态管理 (State Machine)
Redis 不能只存一个字符串，必须结构化拆分（与 LangGraph 的 `AgentState` 对齐）：
*   **会话状态 (Session State)**：短期记忆，最近 10 轮对话的滑动窗口。
*   **任务状态 (Task State)**：例如退款流程的 DAG（有向无环图）节点位置，记录"已收集单号，待收集照片"。LangGraph 的 Checkpointer 自动持久化此状态。
*   **用户状态 (User State)**：从 MySQL 实时加载的 RBAC 权限、VIP 等级。
*   **外部状态 (External State)**：第三方 API 传回的异步回调结果。
*   **状态写入策略**：为了不阻塞对话，采用 **Write-behind (异步写回)** 策略，状态先更新到 Redis 内存，再通过 MQ 异步持久化到 MySQL。

### 5. 权限控制 (RBAC) 在 Agent 中的落地
*   **网关层 RBAC**：API 层面拦截无权用户的非法端点请求。
*   **数据层 RBAC (Metadata Filtering)**：在 Milvus 检索时，将用户的 `Role_ID` 或 `Dept_ID` 作为硬性表达式 (`expr`) 传入，从物理层面隔离越权检索。
*   **工具层 RBAC (Pre-execution Hook)**：大模型决定调用 `delete_user()` 工具前，拦截器先校验当前 `Session` 里的角色是否有 Admin 权限。在 LangGraph 中作为节点前置 Hook 实现。
*   **ABAC 细粒度控制**：结合文档密级、地域、有效期、审批状态做属性级判定。Deny 优先，最小权限。

### 6. 安全防御：Prompt 注入与越狱 (Jailbreak)
*   **隔离防护**：严格分离 `System Prompt` 和 `User Input`。使用 XML 标签（如 `<user_input> {{text}} </user_input>`）将用户输入强行框定在数据域。
*   **前置/后置审计**：引入 Llama-Guard 或专门的 Moderation API，在输入 LLM 前和输出给用户前进行双重毒性与越权检测。
*   **检索内容隔离**：从文档中检索出的内容不得被视为系统指令，必须标注为"证据域"，模型应将其视为普通文本而非执行指令。
*   **PII 治理**：对生成上下文做敏感信息（身份证、手机号、银行卡）识别与脱敏，符合《个人信息保护法》《数据安全法》。
*   **审计合规**：完整记录操作审计日志、数据访问审计，满足等保 2.0 要求。管理员、调试、批量导出接口强审计。
*   **密钥管理**：KMS 集成，API Key 轮转，敏感字段加密存储。
*   **红蓝对抗**：定期对 Agent 系统进行越狱测试与 Prompt 注入自动化测试。

### 7. Agent 性能量化与评估平台 (Evaluation Platform)
不能仅凭"感觉好不好"来发布版本，必须建设平台化评估体系：
*   **评测数据集管理**：建立 Golden Set，覆盖精确事实问答、多跳关系问答、权限边界问答、时效性问答、专有名词查询、无答案问题。每条样本标注期望命中的 `doc_id`、`chunk_id`、实体路径。数据集需版本管理、标注审核、持续扩充。
*   **RAG 黄金三维**：`Context Precision` (检索出的片段是否有用)、`Context Recall` (是否漏掉了关键片段)、`Faithfulness` (大模型回答是否忠于检索出的片段，即幻觉率)。
*   **Agent 评估**：工具调用成功率 (Tool Calling Accuracy)、任务完成率 (Task Success Rate)、端到端响应时间 (P95 Latency)、多 Agent 编排轮次、Handoff 准确率。
*   **检索指标**：Recall@K、MRR、NDCG、Hit Rate、**权限误召回数量（目标为 0）**。
*   **用户反馈**：隐式反馈（是否采纳了代码/点击了链接）+ 显式反馈（点赞/点踩按钮）。
*   **评测自动化**：CI 触发评测、定时回归、线上流量回放评测。每次 Prompt/模型/知识库变更必须跑评测集，达标才允许发布（Eval Gate）。
*   **A/B 测试框架**：线上流量分流、显著性检验、版本对比报告。
*   **人工评估流程**：标注规范、双盲标注、一致性校验（Cohen's Kappa）。
*   **评测工具**：RAGAS / TruLens 离线评估，Langfuse 在线追踪。

### 8. Prompt 架构与 Top-K 设置
*   **Prompt 搭建**：对于复杂推理，内部可以使用分步推理策略，但不向用户、日志或下游系统暴露完整 Chain of Thought。生产输出应采用结构化的 `reason_summary`、`evidence_checklist`、`answer`，只保留可审计的关键依据；对于执行任务，采用 ReAct 风格的受控工具调用，但每一步 Action 必须经过 Tool 契约校验。
*   **Top-K 策略**：不再是死板的固定数字。采用 **Dynamic Top-K (动态阈值)**，只召回相似度分数大于 0.75 的片段；并引入 **MMR (Maximal Marginal Relevance)** 算法，确保召回的多个 Chunk 既相关又彼此多样化，避免给大模型喂一堆重复的废话。一般检索阶段设为 K=10~20，经过 Rerank 后精简为 K=3~5 喂给模型。

### 9. 文档处理管道与知识库运营 (Document Pipeline & Knowledge Ops)
企业级最大的工程量在文档处理与知识运营，不能假设"文档已经分好块"：
*   **文档解析管道**：
    *   支持 PDF / Word / Excel / PPT / HTML / 图片 / 扫描件。
    *   OCR（PaddleOCR / Tesseract）+ 版面分析 + 表格结构化 + 公式识别。
    *   去噪、语言识别、编码统一。
*   **智能分块策略**：
    *   结构感知分块：优先按标题、段落、句子、表格边界切分，避免固定长度切断语义。
    *   父子分块 (Parent-Child Chunking)：检索用小块精准命中，生成用父块提供完整上下文。
    *   表格保持：表格不拆分，整体作为一个 Chunk 并附带结构化标注。
*   **知识质量评估**：chunk 信息量评分、抽取实体置信度、重复/冲突检测。
*   **知识审核流**：入库 → 人工审核 → 评测 → 灰度发布 → 全量。下线同样需审批。
*   **知识时效性**：过期检测、失效告警、自动提醒更新。文档带 `valid_until` 字段。
*   **知识冗余治理**：相似 chunk 合并、冲突 chunk 标注、低质 chunk 清理。
*   **知识更新流水线**：知识变更走"提交 → 审核 → 评测 → 灰度 → 全量"流程，与 CI/CD 评测门禁联动。

### 10. Prompt 与模型版本治理 (Prompt & Model Governance)
LLM 系统最大的运维难点不是代码，而是 Prompt / 模型 / 知识库的持续迭代：
*   **Prompt 版本管理**：Prompt 像代码一样纳入 Git，版本化、可回滚、可 A/B、可灰度。每个 Prompt 版本绑定评测集快照。
*   **模型版本管理**：embedding 模型、抽取模型、生成模型版本绑定。模型切换需记录 `embedding_model_version`、`extractor_version`，支持按版本重建索引。
*   **Prompt 测试**：每次 Prompt 变更自动跑评测集，对比版本间指标变化。
*   **模型 A/B 测试**：线上流量分流，新模型 vs 旧模型，显著性检验后决定全量或回滚。
*   **回滚机制**：Prompt/模型异常时一键回滚到上一稳定版本。

### 11. CI/CD 与发布门禁 (CI/CD & Release Gate)
*   **代码 CI/CD**：代码提交 → 单元测试 → 集成测试 → 构建镜像 → 灰度部署 → 全量。Agent 服务支持蓝绿/金丝雀发布。
*   **评测门禁 (Eval Gate)**：每次发布前必须跑离线评测集，指标达标才允许上线。评测不通过阻断发布。
*   **知识库发布流水线**：知识更新走独立 CI/CD，提交 → 审核 → 评测 → 灰度 → 全量。
*   **数据飞轮闭环**：Bad Case 采集（线上低分会话）→ 标注 → 回灌评测集 → Prompt/模型优化 → 重新评测 → 发布。形成持续迭代闭环。
*   **基础设施即代码 (IaC)**：Docker Compose / Helm Chart 管理环境配置，确保多环境一致性。

---

## 六、 基础设施环境部署指南 (Infrastructure Deployment)

为了将上述架构落地，底层数据库环境的搭建是第一步。根据实际生产环境的灵活性与运维成本，本方案采用"物理机+容器化"混合部署模式。

### 部署前置原则：PoC 与生产环境分层

本章中的宝塔面板、单机 Docker Compose 示例适用于开发环境、PoC 或小规模试运行。对标企业级生产环境时，必须补充以下部署边界：

| 环境 | 目标 | 部署方式 | 发布要求 |
| :--- | :--- | :--- | :--- |
| Dev | 本地开发和功能验证 | Docker Compose / 单机容器 | 允许快速重建，不承诺数据可靠性 |
| Test | 自动化测试和集成验证 | 独立测试环境 | 每次合并触发单元、集成、契约测试 |
| Staging | 生产等价预发 | Kubernetes + Helm / 云托管中间件 | 生产同构配置，接入评测门禁和压测 |
| Prod | 正式生产 | Kubernetes 多副本 + 托管数据库/集群中间件 | 灰度发布、回滚、审计、变更审批 |

生产原则：

*   密码、API Key、数据库凭据必须通过 Secret Manager / KMS 注入，禁止写入 Compose 文件或 `.env` 明文传播。
*   网关、Agent 服务、Worker、检索服务必须无状态化，支持水平扩缩容。
*   MySQL、Redis、Kafka、Milvus、Neo4j 生产环境应优先使用高可用集群或云托管服务。
*   所有入口启用 TLS，内部服务通过私有网络、服务网格或安全组隔离。
*   Helm Chart、Terraform 或 Ansible 纳入版本管理，基础设施变更必须可审计、可回滚。

### 1. MySQL 部署 (基于宝塔面板)
MySQL 作为真理源和业务底座，对 I/O 性能和数据安全性要求极高，建议直接在宿主机裸机部署（或使用云数据库 RDS）。
*   **操作指引**：
    1. 在宝塔面板（BT Panel）的"软件商店"中一键安装 MySQL（建议 8.0+ 版本）。
    2. 在"数据库"菜单中添加业务数据库（如命名为 `enterprise_agent_db`），并妥善保管生成的账号密码。
    3. **开启 Binlog（关键步骤）**：为了支持架构中提到的使用 Canal 异步同步数据到 Milvus/Neo4j，必须在宝塔的 MySQL 设置中，修改 `my.cnf`，确保开启 `log-bin=mysql-bin` 并设置 `binlog_format=ROW`。
    4. **安全设置**：如果是跨服务器访问，需在宝塔的"安全"中放行 3306 端口；若是同机部署，建议仅限 `127.0.0.1` 访问以保证数据不被公网嗅探。

### 2. Neo4j 部署 (基于 Docker)
Neo4j 负责图谱推理，对版本要求较新（需支持向量索引与 SEARCH 语法），采用 Docker 容器化部署以便于环境隔离和无缝升级。
*   **`docker-compose.yml` 配置示例**：
    ```yaml
    version: '3.8'
    services:
      neo4j:
        image: neo4j:2026.05.0  # 采用支持最新特性的大版本
        container_name: neo4j_graphrag
        ports:
          - "7474:7474"         # Web 可视化管理端口
          - "7687:7687"         # Bolt 协议代码连接端口
        environment:
          - NEO4J_AUTH=neo4j/YourSecurePassword123  # 初始账号必须为 neo4j
          - NEO4J_PLUGINS=["apoc"]                  # 安装 APOC 扩展包
        volumes:
          - ./neo4j_data:/data                      # 核心图数据持久化
          - ./neo4j_logs:/logs                      # 运行日志
        restart: unless-stopped
    ```
*   **操作指引**：在服务器新建目录保存该 yaml 文件，执行 `docker-compose up -d` 启动。在浏览器访问 `http://服务器IP:7474` 验证。

### 3. Milvus 部署 (基于 Docker)
Milvus 作为高并发向量搜索引擎，官方强烈推荐使用 Docker Compose 部署其 Standalone（单机版）。
*   **操作指引**：
    1. 新建一个目录用于存放 Milvus 数据，下载官方 Standalone 编排文件：
       `wget https://github.com/milvus-io/milvus/releases/download/v2.4.x/milvus-standalone-docker-compose.yml -O docker-compose.yml`
    2. 启动服务：执行 `docker-compose up -d`
    3. **验证端口**：启动后默认暴露出 `19530`（gRPC/TCP 端口，Python SDK 连接专用）和 `9091`（HTTP 管理端口）。
    4. **数据持久化**：官方提供的 Docker Compose 会自动在当前目录下创建 `volumes` 映射，确保百亿级向量数据不会随容器重启而丢失。

### 4. Redis 部署 (基于宝塔面板或 Docker)
Redis 在架构中承担了极其关键的 **Context 会话状态管理** 以及 **短效结果缓存** 任务，同时作为 LangGraph Checkpointer 的持久化后端。
*   **操作指引**：
    1. **宝塔直装**：在软件商店搜索 Redis 并一键安装。适用于单机快速起步。
    2. **Docker 部署**：
       ```bash
       docker run -d --name redis_agent -p 6379:6379 -v ./redis_data:/data redis:7.0 redis-server --appendonly yes --requirepass "YourRedisPass123"
       ```
    3. **关键配置**：必须开启 AOF（`appendonly yes`）以防服务器宕机导致用户对话状态（State Machine）全部丢失；必须设置复杂的密码（`requirepass`）防止公网被勒索。

### 5. 消息队列与数据同步中间件 (Kafka + Canal)
为了实现 MySQL 到 Milvus/Neo4j 的**数据一致性异步同步**，以及处理 Agent 的**写操作异步人工审批（Saga补偿）**，消息中间件必不可少。
*   **部署组合**：
    *   **Kafka/RabbitMQ**：作为高吞吐消息总线。可通过 Docker 快速部署单节点 Kafka（结合 Kraft 模式免 Zookeeper）。
    *   **Canal (Server)**：下载官方 Docker 镜像 `canal/canal-server:v1.1.7`。
*   **协同机制**：配置 Canal 连接宝塔中的 MySQL（利用刚刚开启的 Binlog），拦截到 `chunks` 表的 INSERT/UPDATE 操作后，组装成 JSON 投递至 Kafka 的 `knowledge_sync_topic`，供 Python Worker 消费。

### 6. 大模型网关与可观测性平台 (LLM Proxy & Observability)
为了防范大模型 API 故障、统计 Token 成本并提供 Trace 追踪：
*   **LLM Gateway (如 OneAPI 或 LiteLLM)**：
    *   **作用**：统一代理 OpenAI/Qwen 等多渠道 Key，提供自动重试、并发限流与渠道负载均衡。
    *   **部署**：Docker 一键部署 `ghcr.io/songquanpeng/one-api`。 Agent 代码中的 `OPENAI_BASE_URL` 直接指向此本地网关。
*   **可观测性平台 (Phoenix 或 Langfuse)**：
    *   **作用**：收集 Agent 的执行轨迹（Trace）、检索的文档块（Context）和耗时（Latency）。LangGraph 的每个节点天然映射为一个 Span。
    *   **部署**：使用 Docker 启动 `arize-phoenix` 容器。
    *   **接入**：在 Agent 代码入口处通过 OpenTelemetry 标准引入，实现对 LlamaIndex / LangChain / LangGraph 乃至自定义 GraphRAG 的零侵入打点。
*   **统一监控栈**：
    *   Prometheus + Grafana：指标采集与展示。
    *   Loki / ELK：结构化日志检索。
    *   Jaeger / Tempo：分布式链路追踪。

### 7. 多活与灾备部署 (Multi-Active & DR)
*   **MySQL**：主从复制 + 半同步，跨可用区部署，定期备份 + PITR 演练。
*   **Redis**：Cluster 模式，跨节点分片 + 副本。
*   **Milvus / Neo4j**：生产环境 cluster 模式，多节点冗余。Neo4j 可由 MySQL + Outbox 重放重建。
*   **Agent 服务**：无状态化，多实例 + 负载均衡，跨可用区部署。
*   **备份恢复演练**：定期演练 MySQL 恢复、Milvus Collection 重建、Neo4j 重放，校验 RTO/RPO 与索引一致性。

---

## 七、 分阶段实施路线 (Roadmap)

### 阶段一：可控 MVP
*   明确领域边界、核心数据模型和 Tool/API 契约，先冻结最小可用接口。
*   建立 MySQL 文档、Chunk、入库任务表，作为真理源。
*   保留 Neo4j 图谱抽取原型，所有 Chunk 元数据与原文写入 MySQL。
*   增加基础向量索引（Milvus 或 Neo4j vector 二选一）。
*   实现最小闭环：召回候选后回 MySQL 做状态和权限校验。
*   单体 Agent 跑通 GraphRAG 检索链路。

### 阶段二：多智能体 + 生产检索链路
*   引入 LangGraph 编排引擎，落地 Supervisor + 专家 Agent 拓扑。
*   建立 Tool Registry、结构化错误码、幂等键和工具调用审计。
*   引入 Milvus Collection 与 Embedding Worker。
*   实现向量、实体、关键词三路召回 + RRF 融合 + Reranker。
*   实现 Query Rewrite / Retrieval Rewrite。
*   落地 Human-in-the-loop 审批流（LangGraph interrupt）。
*   建立离线评测集与基础监控面板。

### 阶段三：企业级治理
*   引入 Outbox/CDC、事件 Schema、DLQ、补偿任务、索引一致性校验。
*   完成 RBAC + ABAC 权限模型、审计日志、PII 治理、安全合规。
*   完成写操作风控矩阵、审批矩阵、Saga 补偿和资损防护演练。
*   完成多租户隔离、成本归因、配额管理。
*   完成微服务拆分、配置中心、CI/CD、契约测试与评测门禁。
*   完成高可用部署、多活灾备、备份恢复演练、混沌工程。
*   建立 On-call、Runbook、故障分级、事故复盘和线上流量回放机制。
*   落地 Prompt/模型版本治理与数据飞轮闭环。
*   根据评估结果调优分块、embedding 模型、图谱 schema、召回权重与 Agent 编排策略。
