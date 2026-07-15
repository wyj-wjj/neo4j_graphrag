# 运维与故障恢复

## 部署前检查

1. 从 `.env.example` 创建密钥管理系统中的配置，不把 `.env` 放入镜像或版本库。
2. staging/prod 使用 `APP_ENV=staging|prod`、`USE_FAKE_EXTERNAL_CLIENTS=false`、非对称 JWT 和 JWKS。
3. MySQL 使用非 root 应用账号；Redis、Neo4j、MinIO 密码必须轮换。
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

## 数据与隐私

日志、Trace、审计只保存脱敏摘要和必要 ID。数据库备份、上传对象和 Langfuse 数据必须遵循同一保留策略。
删除/下线采用真理源失效再清派生索引；生产删除策略需要额外的合规审批与保留期配置。
阶段 1.5 长期记忆没有 Redis 或向量副本；删除会在 MySQL 事务中擦除同一键全部版本的原值。未来接入
语义记忆索引时，必须先实现并测试同一删除传播契约。自动长期记忆写入必须保持关闭。

## 验收

`make acceptance` 运行冻结安装、静态门禁、覆盖率、两套 Golden Set、OpenAPI、前端测试/构建和 Playwright。
真实依赖与镜像检查由 GitHub Actions 的 `real-integration`、`images` Job 执行。
