# 阶段二合成数据工厂中间验收报告

> 验收日期：2026-07-14
> 结论：`ci-small` 与离线工具链通过；阶段二数据工厂最终验收尚未通过
> measurement_mode：`synthetic_offline`
> production_slo_eligible：`false`

## 已通过范围

- 固定 Seed 的 `ci-small` 原子生成、完整验证、替换保护和逐字节确定性。
- 18,992 条结构化记录、103 个文件、39 份 JSON Schema；Manifest 管理除自身外的 102 个文件。
- 权限、订单、物流、退款/审批独立 Oracle；跨租户、金额、时间、状态和引用关系硬不变量。
- 2,000 条 Silver、5,000 条 Security、500 条 Memory、300 条 Golden Candidate。
- Markdown、TXT、HTML、PDF、DOCX、XLSX、PPTX、PNG、JPEG 正常/边界/损坏矩阵。
- 2,000 个事件、2,300 次正常/乱序/重复/DLQ 投递、986 条数据集/逐聚合终态和 200 条 DLQ 修复真值。
- 独立 HTTP 模拟器的身份/租户、Fake Envelope、草单幂等冲突、审批角色和受保护故障语义。
- 正式上传客户端的 multipart、显式 Token、任务轮询和损坏拒绝 HTTP 契约。
- 根项目真实 FastAPI Test/Fake 组合完成 18/18 个有效样本，生成 18 个文档、18 个版本、199 个 Chunk；
  14/14 个可解析文档的 Evidence Anchor 均能从应用 Chunk 反查。
- 行为评分、Recall@K/MRR/NDCG、双人独立审核/候选指纹失效和安全清理门禁。
- `dev-standard` 与 `failure-lab` 的批次派生、规范 JSONL、fsync 检查点、显式恢复、未提交尾部截断、
  磁盘型关系验证和原子发布；中断恢复结果与干净生成逐字节一致。
- `failure-lab` 已物化 1,380,226 条记录、326 个文件、1,155,538,493 字节；生成和全量验证耗时
  223.850 秒，峰值 RSS 342,696 KiB，Manifest 摘要
  `2c64f88dc89625a4b44f504e62c7730cfe6caad003a365c19ac34f1b55d98a60`。
- `dev-standard` 已物化 3,372,108 条记录、1,226 个文件、2,842,845,571 字节；优化后生成和全量验证
  耗时 570.486 秒，峰值 RSS 1,447,980 KiB，Manifest 摘要
  `5bbea39339499703796fe5d9d01fdc8541f4414d11ae3b6f3ad65f30baef7fe2`。
- 流式与旧生成器在 CI Profile 的事件、投递、重放、DLQ 及九类派生产品逐对象相等；独立校验器能发现
  同步伪造文件校验和后的关系篡改，并拒绝不兼容生成器版本。
- 最终快速门禁：格式、Ruff、mypy strict（20 个源码文件）通过；53 项 pytest 通过，分支覆盖率
  88.01%，高于 85% 门槛。

固定 Fixture Manifest 摘要：
`fe860fa31c1d49e406246f2bf46b25afa7338b0bd7f20a9e3b507ccdf38e489b`（历史兼容 v2）。全新 v3
生成的预期摘要为 `a6e48bc37562ae4cc499c8d8de803a363e138cc3fd2efcde8ecdfb6175eb5ee4`。

## 未通过与未执行范围

- 已完成内存/Fake 应用链路，但没有可达的用户本地真实依赖环境，因此未验证本次数据对 MySQL、Milvus、
  Neo4j 和 Redis 的实际写入与销毁重建。
- 没有实现或部署生产 Kafka Relay、消费者、DLQ 修复执行与重放审计；当前只有 Kafka 无关离线真值。
- `staging-large` 尚未物化。其百万订单核心事实仍是内存工作集，因此即使提供全部确认也会硬停止；不能把
  `dev-standard`/`failure-lab` 的结果外推为千万事件档位已通过。
- 未运行真实百炼模型、真实业务 API、真实审批或生产容量测试；离线指标不能作为生产 SLO。
- Golden Candidate 均处于 `pending_independent_review`，没有自动生成 Golden 数据。

## 阻断最终验收的下一步

1. 在可达真实依赖开发环境复验正式上传，核对 Anchor Map、引用、拒答和派生索引重建结果。
2. 实现 Kafka 无关 Port 的 Relay/消费者 Adapter 测试，再在显式集成环境执行重复、乱序、DLQ 修复与重放。
3. 把 `staging-large` 的核心事实改为磁盘工作集，再单独执行容量、恢复和资源验收。
4. 运行完整 GraphRAG、长会话、Tool/审批安全和销毁重建验收，生成新的最终 Manifest 与报告。
