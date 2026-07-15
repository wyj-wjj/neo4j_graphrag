# 合成数据字典与状态说明

## 公共约定

所有 JSONL 业务记录均包含 `dataset_id`、`dataset_version`、`scenario_id`、`tenant_id`、`source=fake`、
`is_synthetic=true` 和 `schema_version`。Schema 严格拒绝额外字段。Manifest 是文件所有权、数量、大小、
SHA-256、Profile、Seed 和兼容版本的权威清单。

MySQL 风格事实由 `tenant/user/product/order/package/refund/approval/knowledge` 表示；事件、向量和图谱仍属于可
重建派生数据。本数据工厂不把动态订单、物流或退款事实写入知识正文和长期记忆。

## 数据产品

| 记录类型 | 文件 | 作用与关键真值 |
| --- | --- | --- |
| tenant / user | `tenants.jsonl` / `users.jsonl` | 租户隔离、角色、不可联系 `.invalid` 身份 |
| access_decision | `access-decisions.jsonl` | 显式 Deny、跨租户、缺策略、依赖失败的 fail-closed Oracle |
| product / order | `products.jsonl` / `orders.jsonl` | Decimal 金额、商品可退性、订单状态历史与所有权 |
| order_oracle | `order-oracles.jsonl` | 查询、改址和退款试算的独立预期结果 |
| package / logistics_oracle | `packages.jsonl` / `logistics-oracles.jsonl` | 物流时间线、异常、催单限流和升级真值 |
| refund / approval | `refunds.jsonl` / `approvals.jsonl` | 草单、审批要求、快照和回调幂等字段 |
| refund_lifecycle | `refund-lifecycles.jsonl` | 重放、载荷冲突、未知结果、对账和补偿终态 |
| knowledge | `knowledge-metadata.jsonl` | 文档版本、状态、有效期、权威等级和 Evidence Anchor |
| physical_knowledge_file | `physical-knowledge-files.jsonl` | 9 格式 × 正常/边界/损坏，记录 MIME、Hash 与预期结果 |
| conversation / evaluation_case | `conversations.jsonl` / `evaluation-cases.jsonl` | 多轮任务、Split、Agent/Tool/引用/副作用银标和版本绑定 |
| memory_case | `memory-cases.jsonl` | 长会话约束、恢复模式、零重复副作用与 `context-policy-v1` 预算 |
| security_case | `security-cases.jsonl` | 跨租户、角色伪造、注入、PII、伪引用和未审批写入 |
| golden_candidate | `golden-candidates.jsonl` | 待双人独立审核包；自动生成时永远不可晋升 |
| event / event_delivery | `events.jsonl` / `event-deliveries.jsonl` | 正常、重复、乱序、毒消息和未知 Schema 投递 |
| replay_expectation | `replay-expectations.jsonl` | Inbox 去重、DLQ 数量和排序后终态 Hash |
| fault_schedule | `fault-schedules.jsonl` | 仅测试可用的依赖故障计划和预期错误语义 |

## 核心状态机

- 订单：`created → paid → allocated → shipped → delivered → closed`，另有 `cancelled` 和
  `refund_pending`。改址只允许 `created/paid/allocated`。
- 包裹：`label_created → picked_up → in_transit → out_for_delivery → delivered`，异常分支包括
  `exception/returned`。
- 退款：`draft → approval_pending → approved/rejected/expired`，执行不确定时进入对账，补偿失败进入人工处理。
- 入库客户端只把应用返回的 `completed/partial_failed/failed` 视为终态；数据工厂不伪造应用 Chunk。

## 评测和版本

Silver Case 绑定 Prompt、Fake 模型 Profile、Router、Agent State、Context Policy、Tool、Event、Oracle 和
Dataset 版本。行为评分按意图、Agent、Tool 顺序、证据、答案状态、副作用和租户隔离分别计分；权限误召回
不能被文本相似度抵消。

`context-policy-v1` 的 32,768 Token 示例预算先预留 4,096 输出和 2,048 安全余量，再把 26,624 输入按
短期原文 7,987、工作记忆 5,325、外部证据 10,650、系统协议 2,662 分配，对应 30/20/40/10 初始目标。
它是评测起点，不是生产权威比例。

Golden Candidate 记录事实 ID、Evidence Anchor、预期行为和审核清单。只有候选指纹未变化、两名不同审核人
均批准时，审核模块才产生 `GoldenRecord`；任何拒绝、同人重复审核或旧指纹审核都不能晋升。
