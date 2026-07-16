# 运维与故障恢复

## 部署前检查

1. 从 `.env.example` 创建密钥管理系统中的配置，不把 `.env` 放入镜像或版本库。
2. staging/prod 使用 `APP_ENV=staging|prod`、`USE_FAKE_EXTERNAL_CLIENTS=false`、非对称 JWT 和 JWKS。
3. MySQL 使用非 root 应用账号；Redis、Neo4j、Milvus 内部 MinIO 和业务对象存储密码必须分别轮换。
4. 运行 `alembic upgrade head`，再启动后端；readiness 全绿后才接流量。
5. 运行 Golden Set、受控真实模型评测、跨租户安全测试和备份恢复演练。

## 入库失败

- `failed`：两类派生索引都失败或解析失败；修复依赖后调用任务 retry。
- `partial_failed`：Milvus/Neo4j 之一失败；retry 会复用 MySQL Chunk，只补失败索引。
- 不要直接改 Milvus/Neo4j 状态。MySQL 的 `vector_status`、`graph_status` 和错误字段是恢复依据。
- 对象 hash 不一致时停止任务并检查对象存储，不要跳过完整性验证。

## 派生索引重建

以 MySQL 当前 active、有效且允许的 child Chunk 为输入，按 `embedding_version` 重建新 Milvus Collection；
模型或维度变化必须换 Collection。Neo4j 使用稳定 tenant/document/version/chunk/entity ID 幂等 upsert。
验证 ID 集合和抽样召回后切换配置，旧索引再延迟清理。

Embedding/KG Kafka Handler 只接受版本化 Envelope，并从 MySQL 重读 Chunk；不要把 Kafka 载荷当事实。
消费者处理成功但 offset 提交前中断时会重复 upsert，因此派生 Adapter 必须保持幂等。
本地阶段二验收使用 `scripts/rebuild_derived_indexes.py`：默认只做 dry-run；执行时必须同时提供
`--execute` 和与 `--tenant-id` 完全一致的 `--confirm-tenant-id`。工具只删除指定租户的派生数据，从 MySQL
当前有效版本重建，并要求 Milvus/Neo4j Chunk ID 与 MySQL 集合完全一致。逐步命令见
`docs/local-deployment-guide.md`。

## Kafka Outbox、Inbox 与 DLQ

- `KAFKA_ENABLED` 只初始化 Publisher；`OUTBOX_RELAY_ENABLED` 还要求真实 SQL Adapter 模式。
- Outbox 租约必须长于 Broker ACK 超时；取消或崩溃后等待租约到期重投。不要手工把 `published` 改为真。
- Consumer 禁用自动 offset；只有 Inbox 终态、可证明重复、stale 或已持久化 DLQ 才提交。
- 同一 `event_id` 不同 Hash 是安全冲突，不能覆盖；未知 `event_version` 必须进入 DLQ。
- 向量与图谱消费者是两个独立部署进程；迁移完成且 Kafka Topic 已由基础设施预建后分别启动：

  ```bash
  uv run graphrag-index-worker vector
  uv run graphrag-index-worker graph
  ```

  两者使用不同 Consumer Group，只消费受控知识 Topic，并在接流量前检查 MySQL、目标索引和 Kafka 元数据。
- DLQ 原始载荷可能含业务摘要，只允许受审计管理员访问。修复必须绑定原始/修复 Hash、操作者、原因、
  目标版本和 replay audit ID；当前仅完成存储契约，管理面 API 尚未开放。

## S3 兼容对象存储

- staging/prod 必须使用 `OBJECT_STORE_BACKEND=s3` 且启用 TLS 校验。IAM/工作负载身份优先于静态密钥。
- 业务原文件 Bucket/账号/Volume 必须与 Milvus 内部 MinIO 隔离。Local Compose 使用 `object-store` 服务，
  `ALLOW_S3_BUCKET_BOOTSTRAP=true` 只用于本地引导，生产 Bucket 由基础设施流程预建。
- 读取会校验租户 Metadata 与 SHA-256；失败必须停止入库，不允许跳过完整性检查。

## 阶段二入口验收

先下载并验证固定 `dev-standard` Release，再只走正式上传 API：

```bash
cd synthetic-data
uv sync --frozen --group dev
uv run graphrag-data fetch-release --profile dev-standard
cd ..
uv run python scripts/phase2_entry_acceptance.py \
  synthetic-data/generated/synthetic-commerce-v1/dev-standard \
  --base-url http://127.0.0.1:8000 \
  --token-file /path/outside/repo/admin-token.txt \
  --output evaluation/phase2-entry-api-ingestion.json
```

脚本拒绝 Fake readiness、验证数据 Manifest、调用正式上传客户端、等待终态并冻结绑定 Git Commit 的报告；
不会输出 Token，也不会直写数据库。该报告状态会明确保留三项未完成门禁：Anchor/ACL/引用、Milvus/Neo4j
销毁重建、拒答/跨租户/依赖故障/部分写入。三项人工或自托管 Runner 证据齐全前不得把入口验收标为通过。

## 文档更新和下线

新版本只有在两个派生索引都成功时才激活并使旧版本失效。下线先提交 MySQL 状态，再尽力删除派生索引；
即使删除失败，MySQL ACL 后置过滤也会阻止返回。不要为了索引可用而恢复已下线真理源状态。

## 依赖故障策略

| 依赖 | 行为 |
| --- | --- |
| MySQL/ACL | fail-closed，不返回企业知识 |
| Milvus | 图谱/关键词可继续，记录分支失败 |
| Neo4j | Dense/关键词可继续，记录分支失败 |
| Reranker | 使用 RRF 顺序回退 |
| LLM/Tool | 总超时、幂等有限重试、依赖舱壁/熔断；失败安全停止并转人工，不用参数知识补企业事实 |
| Redis | readiness 失败；Checkpoint/限流不静默绕过 |
| Kafka | API 真理源写入不回滚；Outbox 保留并退避，Consumer 不提交非终态 offset |
| S3 | 上传失败不创建 MySQL 版本；读取 Hash/租户不一致立即停止 |
| 业务 HTTP | 只读有限重试；写请求不因网络错误自动重试，结果未知进入人工对账 |

## 数据与隐私

日志、Trace、审计只保存脱敏摘要和必要 ID。数据库备份、上传对象和 Langfuse 数据必须遵循同一保留策略。
删除/下线采用真理源失效再清派生索引；生产删除策略需要额外的合规审批与保留期配置。
阶段 1.5 长期记忆没有 Redis 或向量副本；删除会在 MySQL 事务中擦除同一键全部版本的原值。未来接入
语义记忆索引时，必须先实现并测试同一删除传播契约。自动长期记忆写入必须保持关闭。

## 验收

`make acceptance` 运行冻结安装、静态门禁、覆盖率、两套 Golden Set、OpenAPI、前端测试/构建和 Playwright。
真实依赖与镜像检查由 GitHub Actions 的 `real-integration`、`images` Job 执行。
