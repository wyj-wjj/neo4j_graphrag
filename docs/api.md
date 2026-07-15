# API 使用说明

所有端点以 `/api/v1` 开头；OpenAPI 位于 `/api/v1/openapi.json`，仓库快照为 `openapi.json`。

## 身份

dev/test 可调用 `POST /auth/dev-token` 获取短期测试 Token。staging/prod 不注册可用的本地签发旁路，
必须配置非对称 JWT/JWKS。后续请求使用 `Authorization: Bearer <token>`。

## 主要端点

| 方法与路径 | 作用 | 权限 |
| --- | --- | --- |
| `GET /health/live` | 进程存活 | 公开 |
| `GET /health/ready` | MySQL/Redis/Milvus/Neo4j/模型就绪 | 公开但不泄露连接值 |
| `POST /sessions` | 创建会话 | 已登录 |
| `GET /sessions/{id}` | 会话详情 | 所有者或管理员 |
| `GET /sessions/{id}/history` | 分页历史 | 所有者或管理员 |
| `DELETE /sessions/{id}` | 关闭会话并清 Checkpoint | 所有者或管理员 |
| `GET/POST /memories` | 查看或显式确认创建长期记忆 | 当前用户 |
| `PATCH/DELETE /memories/{id}` | 纠正或删除长期记忆版本链 | 当前用户 |
| `GET/PUT /memories/settings` | 查看、启用或禁用长期记忆 | 当前用户 |
| `POST /documents` | 上传新文档并返回任务 | admin/knowledge_admin |
| `GET /documents` | 文档列表 | admin/knowledge_admin |
| `POST /documents/{id}/versions` | 上传新版本 | admin/knowledge_admin |
| `GET /documents/{id}/versions` | 版本历史，不返回对象 Key | admin/knowledge_admin |
| `DELETE /documents/{id}` | 下线文档并清理派生索引 | admin/knowledge_admin |
| `GET /ingestion-tasks/{id}` | 查询任务 | 已登录、租户隔离 |
| `POST /ingestion-tasks/{id}/retry` | 重试失败/部分失败任务 | admin/knowledge_admin |
| `POST /chat` | 完整回答 | 已登录 |
| `POST /chat/stream` | SSE 回答 | 已登录 |
| `POST /admin/retrieval-debug` | 分支/融合/引用调试 | admin |
| `GET /admin/knowledge-quality` | 冲突/重复/过期/低质量只读报告 | admin/knowledge_admin |
| `POST /approvals/fake-callback` | HMAC Fake 回调占位 | admin，dev/test only |

## SSE 事件

顺序为 `start → route? → retrieving? → delta* → citation* → status → end`；异常为 `error → end`。
模型 delta 直接来自 Provider，不是完整答案的事后切片。每个事件含 `event_version`、递增 sequence 和
稳定事件 ID。断线会向下取消 Provider/Tool，未完成轮次不会提交 assistant 半成品；客户端读取会话历史
后以同一 `client_turn_id` 安全重试。

## 错误

所有 API 与验证错误归一为：`error_code`、`message`、`request_id`、`retryable`。客户端不能依赖内部异常名，
也不会收到堆栈、SQL、Token 或连接字符串。
