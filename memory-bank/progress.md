# 阶段一实施进度

> 更新时间：2026-07-14。阶段一代码、离线测试、真实依赖集成、浏览器端到端、无障碍和供应链
> 验收均已通过；生产百炼、真实业务 API 与审批执行仍属于受控上线或阶段二范围。

## 总览

| 范围 | 状态 | 主要验证 |
| --- | --- | --- |
| 0.1–0.5 约束基线 | 完成 | 必读文档、范围、密钥风险与记录格式已核对 |
| 1.1–2.9 工程与契约 | 完成 | 冻结安装、配置负例、Port/Fake、类型与契约测试 |
| 3.1–3.9 MySQL 真理源 | 完成 | SQLite 仓库测试；真实 MySQL 8.4 迁移与健康检查通过 |
| 4.1–4.5 Redis | 完成 | Fake 契约及真实 Redis 8 能力、Checkpoint、锁测试通过 |
| 5.1–6.5 Milvus/Neo4j | 完成 | 真实 Milvus/MinIO 写后检索与 Neo4j 有界检索、删除测试通过 |
| 7.1–7.6 百炼 Provider | 完成 | httpx/OpenAI 兼容、结构化输出、流、Embedding、OCR、Rerank Fake 测试 |
| 8.1–9.7 入库与一致性 | 完成 | 格式解析、父子分块、部分失败恢复、版本激活和下线验收 |
| 10.0–10.11 GraphRAG | 完成 | 并发/RRF/ACL/Rerank/引用/拒答；20 条 Golden Set 全门槛通过 |
| 11.1–11.11 Agent/Tool | 完成 | 全 Agent 路径、Tool 审计、Fake 边界、Checkpoint 持久化 |
| 12.1–12.9 API | 完成 | OpenAPI、JWT/RBAC、REST、SSE、回调、统一错误验收 |
| 13.1–13.8 前端 | 完成 | Playwright 完整主流程与逐页 axe 严重/关键问题检查通过 |
| 14.1–14.12 交付 | 完成 | 全部五个 CI Job、镜像 Trivy 和 gitleaks 通过 |

## 已完成实现

- 以 `uv.lock` 和 `pnpm-lock.yaml` 固定后端与前端依赖；Python 限定 3.12。
- 模块化单体、领域 Port/Adapter、生产配置保护和 `.env.example` 已建立。
- 显式 Alembic 初始迁移覆盖 15 张阶段一表；SQL 仓库按租户和 ACL fail-closed。
- Redis Checkpoint、缓存、所有者锁、限流与 LangGraph Redis Saver 已接线。
- Milvus HNSW/COSINE 与 Neo4j 有界图检索 Adapter 已实现。
- 文档解析、OCR、结构分块、三库写入、补偿、更新、版本历史和下线已实现。
- GraphRAG 实现三路并发、RRF、MySQL 最终授权、Rerank 相关性门槛、引用和拒答。
- Supervisor、FAQ、KB、Escalation 与三个明确 Fake 的业务 Agent 已实现。
- Tool Registry 已进入真实执行链，带角色、Schema、超时、Trace、Agent Step 和 Tool Call Log。
- FastAPI 与 React 工作台主流程已实现，OpenAPI 类型已重新生成。
- Docker Compose、非 root 多阶段镜像、GitHub Actions、真实依赖 Job、Trivy 和 gitleaks 已配置。
- 依赖审计发现后升级 `cryptography` 并用 `aiomysql` 替换有未修复 CVE 的 `asyncmy`；当前 pip-audit 清洁。

## 最新验证结果

最终验收命令执行后更新本节；当前已确认：

- Ruff：通过。
- mypy strict：通过，49 个源码文件无错误。
- pytest：47 通过，1 个真实依赖测试因当前环境无 Docker 跳过；分支覆盖率 86.48%。
- Golden Set：20 条；路由、Recall@20、MRR、NDCG、引用、Faithfulness、拒答率均为 1.0。
- pip-audit：无已知漏洞；仅本地项目因未发布到 PyPI 被跳过。
- 前端：ESLint/TypeScript、Vitest 2 条测试和 Vite 生产构建通过；GitHub CI 中 Playwright 1 条
  完整工作台主流程及逐页 axe 检查通过。
- Compose：YAML 结构解析通过，共 8 个服务。
- GitHub CI 首轮确认 backend、secrets 通过；发现并修复 Neo4j 不存在的镜像标签、Trivy Action
  缺少 `v` 前缀，以及 Progress 无可访问名称和占位文字对比度不足。
- CI 修复本地验证：Compose/Workflow YAML 与固定版本断言通过；占位文字对比度为 7.00:1；
  Playwright 成功收集 1 条端到端测试，完整浏览器执行交由 CI 重验。
- GitHub CI 第二轮确认 backend、secrets 持续通过；剩余问题定位为 E2E 默认总超时、Milvus
  Standalone 配套配置偏差，以及旧 Trivy Action 内置二进制安装失败。修复保持测试断言和安全门槛不变。
- GitHub CI 第三轮确认 backend、images、secrets 通过，两次 Trivy 镜像扫描均通过。发送按钮补充
  稳定可访问名称和组件回归测试；真实依赖启动失败时增加 Milvus/etcd/MinIO 容器日志诊断。
- GitHub CI 第四轮中，浏览器 E2E 已完成上传、入库、提问并获得引用答案，仅剩 `[C1]` 同时匹配
  答案卡片和引用标记的严格选择器歧义；现已改为精确匹配。Milvus 诊断日志确认其仍使用默认
  MinIO 凭据，现已按 Milvus 2.6.14 官方配置键注入与 MinIO 相同的环境凭据。
- GitHub CI 第五轮确认全部真实依赖健康且 Alembic 迁移成功；Adapter 测试暴露 Milvus 默认 Bounded
  consistency 会让刚写入的向量短暂不可见。在线检索和补偿 ID 查询现显式使用 Strong consistency，
  并增加单元契约断言。
- 同轮浏览器验收已完整执行到引用答案并通过精确选择器，axe 最后发现 Ant Design 来源 Tag 的预设
  绿色文字对比度仅 3.37:1；来源 Tag 现按真实/Fake 语义显式使用深绿/深橙文字，在保留状态语义的
  同时达到 WCAG AA 4.5:1 门槛。
- 后续浏览器重验确认来源 Tag 对比度问题已消除，并执行到最后的检索调试页；该页无业务作用的
  `aria-hidden` 占位按钮仍可获得焦点，现已删除该元素，不通过放宽 axe 规则规避问题。
- GitHub Actions `ci` 运行 `29383348044` 最终确认 `backend`、`frontend`、`real-integration`、
  `images`、`secrets` 五个 Job 全部通过；阶段一外部验收完成。

## 本地环境限制与 CI 覆盖

1. 当前工作区没有 `docker`/`podman`；GitHub Actions 已实际运行 MySQL 8.4、Redis 8.2、Milvus 2.6、
   Neo4j 5.26.28、容器健康检查、迁移、真实 Adapter、镜像构建和 Trivy，结果通过。
2. 当前工作区的 Chromium 下载受限；GitHub Actions 已安装固定 Playwright Chromium 并完成 E2E 与 axe，结果通过。
3. 未使用真实百炼密钥，符合 CI 禁止收费模型要求；生产前必须运行受控云模型评测与容量测试。

## 下一步

- 合并已通过全部检查的阶段一 PR，并以当前文档、锁文件和测试作为后续 AI/开发者的迁移基线。
- 生产部署前提供真实 JWKS、轮换后的数据库/Redis/Neo4j/MinIO 密码和百炼密钥，不得提交这些值。
