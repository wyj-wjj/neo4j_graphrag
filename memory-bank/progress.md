# 阶段一实施进度

> 更新时间：2026-07-14。状态“代码完成/外部待验”表示实现和离线测试已完成，但当前工作区缺少 Docker
> 或浏览器二进制，不能声称真实环境已经执行；相应验证已写入 CI。

## 总览

| 范围 | 状态 | 主要验证 |
| --- | --- | --- |
| 0.1–0.5 约束基线 | 完成 | 必读文档、范围、密钥风险与记录格式已核对 |
| 1.1–2.9 工程与契约 | 完成 | 冻结安装、配置负例、Port/Fake、类型与契约测试 |
| 3.1–3.9 MySQL 真理源 | 代码完成/真实 MySQL 待 CI | SQLite 仓库测试；迁移 upgrade/diff/downgrade/upgrade |
| 4.1–4.5 Redis | 代码完成/真实 Redis 待 CI | Fake Adapter 契约；Redis 8 Compose 与真实 Adapter CI |
| 5.1–6.5 Milvus/Neo4j | 代码完成/真实服务待 CI | Schema、维度、租户、跳数、CRUD Fake 契约；真实 Adapter CI |
| 7.1–7.6 百炼 Provider | 完成 | httpx/OpenAI 兼容、结构化输出、流、Embedding、OCR、Rerank Fake 测试 |
| 8.1–9.7 入库与一致性 | 完成 | 格式解析、父子分块、部分失败恢复、版本激活和下线验收 |
| 10.0–10.11 GraphRAG | 完成 | 并发/RRF/ACL/Rerank/引用/拒答；20 条 Golden Set 全门槛通过 |
| 11.1–11.11 Agent/Tool | 完成 | 全 Agent 路径、Tool 审计、Fake 边界、Checkpoint 持久化 |
| 12.1–12.9 API | 完成 | OpenAPI、JWT/RBAC、REST、SSE、回调、统一错误验收 |
| 13.1–13.8 前端 | 代码完成/浏览器待 CI | ESLint+TS、2 个 Vitest、生产构建；Playwright 下载受限 |
| 14.1–14.12 交付 | 代码完成/容器待 CI | Trace、脱敏、安全、故障、评测、CI、镜像、文档 |

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
- 前端：ESLint/TypeScript 通过，Vitest 2 通过，Vite 生产构建通过；Playwright 已确认后端与
  Vite 能自动启动，但浏览器可执行文件因下载受限而缺失。
- Compose：YAML 结构解析通过，共 8 个服务。

## 环境阻塞与补跑入口

1. 当前环境没有 `docker`/`podman`，未本地执行 MySQL 8.4、Redis 8.2、Milvus 2.6、Neo4j 5.26、
   镜像构建、容器健康检查和 Trivy。GitHub Actions `real-integration` 与 `images` Job 是强制补跑入口。
2. Playwright Chromium 下载端返回空或截断归档，未本地运行浏览器 E2E 与 axe；CI 会安装 Chromium 后执行。
3. 未使用真实百炼密钥，符合 CI 禁止收费模型要求；生产前必须运行受控云模型评测与容量测试。

## 下一步

- 在有 Docker 和公网浏览器镜像的 runner 推送分支并观察全部 CI Job。
- 只有 `backend`、`frontend`、`real-integration`、`images`、`secrets` 全绿后，才能把阶段一外部验收标记为完全完成。
- 生产部署前提供真实 JWKS、轮换后的数据库/Redis/Neo4j/MinIO 密码和百炼密钥，不得提交这些值。
