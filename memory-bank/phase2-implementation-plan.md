# 阶段二生产能力实施计划

> 文档状态：执行中
> 制定日期：2026-07-15
> 代码基线：`main@cae1f7c`
> 前置基线：阶段一、阶段 1.5、合成数据工厂与不可变大数据 Release 已完成。

## 1. 实施原则

- MySQL 继续作为唯一真理源；Milvus、Neo4j、Kafka 消费状态和缓存均不得提升为业务真理源。
- 阶段二优先新增 Adapter、应用服务和部署单元，不允许 Agent 直接连接 Kafka、S3 或业务 HTTP 服务。
- 每个里程碑先冻结版本化契约和失败语义，再实现 Adapter、测试、配置与运维文档。
- 真实业务契约、审批组织和风控阈值缺失时，只实现供应商无关边界和明确标记的合成验证，不伪造生产规则。
- 所有事件消费至少一次，消费者必须依靠 `event_id` 和聚合版本幂等；不能把 Kafka Producer 的幂等误当成端到端恰好一次。
- 生产配置默认关闭未完成能力；启用时必须在启动阶段校验依赖、凭据和互斥配置。
- `dev-standard`、`failure-lab` 只能用于工程、安全、容量趋势与故障验证，不能替代真实业务校准。

## 2. 门禁零：阶段二入口验收

### 2.1 环境与数据

1. 在专用开发环境启动 MySQL、Redis、Milvus、Neo4j，并执行 Alembic 到最新版本。
2. 从固定 Release `synthetic-commerce-v1-data-v3` 下载并完整验证 `dev-standard`。
3. 只通过 `/api/v1/documents` 和任务查询 API 入库知识物理文件，不直接写表或派生索引。

### 2.2 必验链路

- 解析、父子 Chunk、Evidence Anchor 到 Chunk 映射。
- Dense、Graph、Keyword、RRF、MySQL ACL 复核、Rerank、引用与拒答。
- 跨租户隔离、依赖单故障、派生索引部分失败和补偿重试。
- 删除并重建 Milvus Collection 与 Neo4j 派生子图，比较重建前后的授权结果和检索指标。

### 2.3 通过标准

- Anchor 覆盖、引用、拒答、检索指标和权限误召回达到数据规范中的门槛。
- 权限误入上下文、跨租户结果和未审批真实写入均为零。
- 报告绑定代码、数据集、Manifest、Prompt、模型、Embedding、Router、State 和 Context Policy 版本。
- 当前云端执行环境没有 Docker/Podman，真实执行必须在用户本地或自托管 Runner 完成；该限制不阻塞后续代码开发。

## 3. 里程碑一：Outbox Relay 与 Kafka 发布

### 3.1 可靠 Outbox 领取

- 为 `outbox_events` 增加可用时间、租约所有者、租约到期、最后错误和发布时间。
- Relay 使用有限批次和短租约领取；多实例通过数据库行锁与租约避免同一时刻重复处理。
- 进程终止或租约过期后事件可以重新领取，不能永久卡在处理中。

### 3.2 Kafka Publisher

- 使用 `confluent-kafka`，开启 Producer 幂等与 `acks=all`。
- Topic 由受控事件类型映射，不接受载荷指定任意 Topic。
- Kafka Key 固定为租户与聚合标识，Envelope 使用稳定 JSON，Header 保存事件类型、版本和 Trace ID。
- 只有收到 Broker delivery acknowledgement 后才把 Outbox 标记为已发布。

### 3.3 失败与验收

- 瞬时失败按配置指数退避；取消保留至少一次语义并允许租约后重投。
- 日志只记录事件 ID、类型、租户、重试次数和错误类型，不记录业务原文。
- 单元、SQLite 租约集成和可选真实 Kafka 集成测试必须覆盖成功、超时、重复投递、多 Relay 竞争和重启恢复。

## 4. 里程碑二：Inbox、消费者、DLQ 与派生索引 Worker

- 新增版本化 Inbox、消费尝试和 DLQ 修复审计表；`event_id` 唯一，未知事件版本 fail closed。
- 建立 Embedding Worker 与 KG Worker，消费 `knowledge.events.v1`，从 MySQL 重新读取当前版本 Chunk。
- 乱序事件按聚合版本拒绝覆盖更新状态；成功处理后原子写 Inbox 终态和 MySQL 索引状态。
- 毒消息进入 DLQ；人工修复必须保存原始 hash、操作者、原因、目标版本和重放结果。
- 通过 `failure-lab` 验证重复、乱序、毒消息、处理中断、部分索引成功和终态 Oracle。

## 5. 里程碑三：S3 兼容对象存储

- 新增 S3/MinIO/OSS 兼容 `ObjectStorePort` Adapter，本地 Adapter 保留给 dev/test。
- Bucket、Endpoint、Region、寻址方式、TLS 与凭据均由配置注入；Milvus 内部 MinIO 与业务对象存储使用不同 Bucket 和账号。
- 对象 Key 继续使用 `{tenant}/{document}/{hash-prefix}/original.ext`，禁止绝对路径和路径穿越。
- 上传保存内容 hash、大小和 MIME 元数据；读取时验证租户、对象 Key 与 hash，删除保持幂等。
- 使用独立 MinIO Bucket 运行保存、重复保存、跨租户、损坏对象、删除和服务故障测试。

## 6. 里程碑四：业务 HTTP Adapter 与 Tool 服务边界

- 先接入合成 HTTP 模拟器验证 Order、Logistics、Refund、Approval Port 替换，不把模拟器称为真实服务。
- 真实 Adapter 使用独立 Base URL、服务身份、连接池、超时、熔断和审计；不能复用用户可控 Header 提升角色。
- 只读请求可有限重试；写请求必须携带稳定幂等键，且只在服务端明确支持幂等时重试。
- 外部错误映射为稳定领域错误，响应必须通过严格 Schema、来源和租户校验后才能进入 Agent State。
- 获得真实业务 OpenAPI、审批矩阵和风控规则后再新增对应版本 Adapter；禁止猜测真实字段或执行路径。

## 7. 里程碑五：生产审批与受控关键写入

- 保持 `calculate → create_draft → approval_interrupt → execute`，审批通过前没有 execute 能力。
- 审批回调使用非对称签名或 mTLS、时间窗、nonce、幂等键和审批主体授权；Fake HMAC 路由不得在 staging/prod 注册。
- 执行前重新读取订单/退款当前状态、草单快照、审批决定和风控结果，禁止只信任 Checkpoint。
- 结果未知时进入对账，不自动重复资金写入；补偿和人工处理均写审计。
- 真实执行功能必须在业务契约、审批组织、限额、回滚和资损演练齐备后单独启用。

## 8. 里程碑六：受控双专家编排

- 复用 `bounded-plan-v1` 和 `deterministic-consolidator-v1`，最多两个专家、固定步骤和总预算。
- 首批只开放只读组合，例如订单状态 + 物流轨迹；任何写意图仍走单一受控业务状态机。
- 仲裁只使用来源权威等级、数据时效和确定性冲突规则，不比较模型自报置信度猜答案。
- 使用合成多意图集验证路由、Handoff、冲突、取消、超时、引用筛选和上下文预算；达标后再按 Feature Flag 灰度。

## 9. 里程碑七：多租户、生产治理和灾备

- 实现租户生命周期、QPS/Token/存储配额、成本归因与管理员审计。
- 建立 Kafka、对象存储、业务 API、审批和 Worker 的 readiness、指标、告警和 Runbook。
- 根据负载证据拆分部署单元，不为形式提前拆微服务。
- 建立 MySQL 备份/PITR、Redis 恢复、Kafka 重放、Milvus/Neo4j 重建与季度演练报告。
- Staging 完成真实百炼、Router、Dynamic Top-K、容量和隐私校准后，才进入生产发布门禁。

## 10. 当前执行位置

- 门禁零：已提供只走正式 API、拒绝 Fake readiness 且冻结 Commit/Manifest 的入口脚本；真实环境中的
  Anchor/ACL/引用、派生索引销毁重建和故障矩阵仍待执行。当前环境缺少 Docker/Podman。
- 里程碑一：Outbox 租约、单聚合顺序、Kafka Publisher、Broker ACK、退避与运行时接线已完成；真实
  Broker 下的多 Relay 竞争、重启和 Rebalance 仍待自托管 Runner 验证。
- 里程碑二：Inbox、DLQ 存储/修复审计、Consumer offset 语义、MySQL 驱动的向量/图 Handler 和独立
  Worker 启停入口、本地单 Broker/双 Worker Compose 已完成；生产编排平台清单、DLQ 管理/重放 API、
  `failure-lab` 终态对账仍待完成。
- 里程碑三：Local/S3 Adapter、隔离 MinIO Profile、Bucket 本地引导和可选真实 S3 测试已完成；当前
  分支仍需在 Docker CI 实际运行。
- 里程碑四：合成 HTTP Adapter 已完成并保持 Fake 标记；真实 OpenAPI、服务身份与错误/幂等契约尚未提供。
- 里程碑五：生产审批、执行和对账依赖真实审批组织、签名、限额及资损契约，保持未注册和 fail-closed。
- 里程碑六：只读订单查询 + 物流查询的恰好两专家编排已完成；写意图和第三意图不会进入 Compound。
- 里程碑七：真实多租户配额、成本归因、灾备演练和生产校准尚未开始，不在缺少真实环境证据时宣称完成。
