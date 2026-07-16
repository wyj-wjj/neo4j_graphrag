# 项目实施进度

> 更新时间：2026-07-15。阶段一与阶段 1.5 已完成；阶段二首批基础设施、对象存储、合成 HTTP 适配和
> 只读双专家已实现。真实大数据入口验收、Kafka Broker、真实业务契约与生产审批仍未完成。

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
| 1.5-A 会话一致性 | 完成 | client_turn_id、消息序号、revision、单会话单运行、事务提交与回放测试 |
| 1.5-B 上下文与短期记忆 | 完成 | 30/20/40/10 Token 基线、摘要/结构状态、Manifest、多轮指代测试 |
| 1.5-C Checkpoint 与恢复 | 完成 | State v1→v2、真实 interrupt/resume、MySQL 快照及 Redis 丢失回退 |
| 1.5-D 真实流式与执行可靠性 | 完成 | Provider delta、RunEvent v1、统一终态、取消、Tool 输出/重试/舱壁/熔断 |
| 1.5-E 治理与评测 | 完成 | 长期记忆治理、Safety Port、Prompt/Generation Manifest、质量报告、可靠性门禁、受控结果收敛契约 |
| 2.0 合成数据设计 | 完成设计 | `phase2-data-spec-v1` 与 118 步数据工厂实施计划 |
| 2.1 `ci-small` 合成数据基线 | 完成 | 18,992 条记录、多格式文件、独立 Oracle、原子导出、Manifest；数据工厂共 53 项测试 |
| 2.2 HTTP 模拟与上传工具 | 完成内存验收/待真实依赖 | 非生产模拟器；正式 API 18/18 入库、199 Chunk、14/14 Anchor；未连真实三库 |
| 2.3 评测与安全治理 | 完成离线基线 | 2,000 Silver、5,000 Security、500 Memory、300 Candidate、评分与双人审核门禁 |
| 2.4 事件与清理 | 完成离线基线 | 2,300 次投递、Inbox/DLQ 重放 Oracle、资源估算、Manifest dry-run/确认清理 |
| 2.5 大规模与全链路 | 部分完成 | dev/failure 已流式物化并独立验收；staging、Kafka、真实依赖与 GraphRAG 待完成 |
| 2.6 Outbox/Kafka 发布 | 完成代码/待真实 Broker | 租约、单聚合顺序、退避、Broker ACK、受控 Topic、运行时开关 |
| 2.7 Inbox/DLQ/消费者 | 完成核心/待重放管理面 | 消费者级幂等、Hash 冲突、stale、未知版本、DLQ、offset 终态提交、独立索引 Worker 入口 |
| 2.8 S3 对象存储 | 完成代码/待当前分支 CI | 独立 MinIO Profile、租户前缀、Hash、SSE/KMS、真实集成测试 |
| 2.9 业务 HTTP Adapter | 完成合成契约 | Fake Envelope、租户/用户复核、读重试/写不重试；真实 OpenAPI 未提供 |
| 2.10 受控双专家 | 完成首个只读组合 | 仅订单查询+物流查询，恰好两意图、固定预算、确定性互补收敛 |

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

- 阶段二首批增量：Ruff 与 mypy strict（75 个源码文件）通过；pytest 125 通过、2 个真实依赖测试因当前
  环境无 Docker 跳过；分支覆盖率 82.72%，高于 80% 门槛。已覆盖 Outbox 单聚合顺序、Broker ACK、Inbox
  租约/重复/Hash 冲突/stale、未知版本与无效 Envelope DLQ、Kafka offset 终态提交、MySQL 驱动的向量/图
  Handler 幂等重建、S3 租户与完整性、合成 HTTP 响应隔离、只读订单+物流 Compound，以及写意图禁止进入
  Compound。`uv lock --check`、Compose/Workflow YAML 解析和 `git diff --check` 通过；Compose 共 12 个服务，
  Workflow 共 6 个 Job。
- 本窗口 `pip-audit` 已使用可写缓存重新启动，但访问 PyPI 超时，未获得新的漏洞结论；基线主分支的安全
  Job 已通过，当前分支新增依赖仍须由 GitHub CI 的联网审计确认。

- 阶段 1.5 里程碑 A–E 本地增量验收：pytest 96 通过、1 个真实依赖测试因当前环境无 Docker 跳过；
  新增内存/SQL 并发、重复请求、取消重试、消息顺序、迁移、API 回放、Token 预算、摘要/结构状态、
  Manifest 持久化、多轮指代、State 迁移、审批中断/恢复、冲突决定、Redis 丢失回退、Provider 首个
  delta、SSE/持久化终态一致、取消传播、Tool 输出验证、幂等重试、总超时、舱壁、熔断、长期记忆
  确认/纠正/禁用/删除/隔离、Safety、Prompt hash、Generation Manifest、知识质量，以及最多两个专家的
  权威/时效/冲突收敛测试。
- Ruff：通过。
- mypy strict：通过，58 个源码文件无错误。
- pytest：96 通过，1 个真实依赖测试因当前环境无 Docker 跳过；覆盖率 86.05%，高于 80% 门槛。
- Golden Set：20 条；路由、Recall@20、MRR、NDCG、引用、Faithfulness、拒答率均为 1.0。
- Memory & Reliability Golden Set：4/8/12/16 轮共 4 组；关键约束保留、上下文预算、幂等、State
  迁移和隔离率均为 1.0；自动长期记忆写入率、长会话成功率下降、重复追问率和关键约束违反率均为 0。
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
- 阶段二数据工厂当前基线：Ruff、格式和 mypy strict（21 个源码文件）通过；58 项测试通过，覆盖率
  89.19%（门槛 85%）。
  固定 `ci-small` 生成 18,992 条结构化记录，包含 2,000 Silver、5,000 Security、500 Memory、300
  Golden Candidate、2,000 事件、2,300 次投递、986 条逐聚合重放终态、200 条 DLQ 修复真值，以及
  9 种知识格式的正常/边界/损坏矩阵。产物共 103 个文件、约 14 MB，Manifest 管理 102 个文件，摘要为
  `fe860fa31c1d49e406246f2bf46b25afa7338b0bd7f20a9e3b507ccdf38e489b`；重复生成逐字节一致。
- 数据工厂独立 HTTP 模拟器已通过跨租户、Fake Envelope、草单幂等/冲突和受保护故障控制契约测试。
  正式上传客户端除 HTTP Mock 外，已直接连接根项目真实 FastAPI Test/Fake 组合：18 个有效样本全部完成，
  生成 18 个文档、18 个版本、199 个 Chunk，14 个可解析文档的 Evidence Anchor 全部映射到 Chunk。
  首轮验收发现边界 XLSX 的超长 Token 会让 Fake 图抽取失败；新增复现断言并限制实体长度后复验通过。
  该结果仍未验证用户本地 MySQL/Milvus/Neo4j。
- 公共契约现有 39 份 JSON Schema；行为评分、Recall/MRR/NDCG、候选双人审核与指纹失效、安全 dry-run
  清理和完整 dataset ID 确认均有自动化测试。GitHub CI 新增独立 `synthetic-data` 快速 Job。
- 阶段二大 Profile 现采用 `graphrag-data-factory-v3`：规范 JSONL 批次写入、每批 fsync、版本化检查点、
  显式 `--resume`、已提交前缀校验/尾部截断、完成文件复核、临时 SQLite 关系索引、独立全量验证和原子发布。
  CLI 只开放 `dev-standard`/`failure-lab`；`staging-large` 因核心事实仍是内存工作集而继续硬停止。
- `failure-lab` 已物化到 `synthetic-data/generated/synthetic-commerce-v1/failure-lab/`：1,380,226 条记录、
  326 个文件、1,155,538,493 字节；生成+校验 223.850 秒，峰值 342,696 KiB，Manifest 摘要
  `2c64f88dc89625a4b44f504e62c7730cfe6caad003a365c19ac34f1b55d98a60`。
- `dev-standard` 已物化到 `synthetic-data/generated/synthetic-commerce-v1/dev-standard/`：3,372,108 条记录、
  1,226 个文件、2,842,845,571 字节；优化后生成+校验 570.486 秒，峰值 1,447,980 KiB，Manifest 摘要
  `5bbea39339499703796fe5d9d01fdc8541f4414d11ae3b6f3ad65f30baef7fe2`。批次派生相对优化前实测耗时
  下降约 12.6%，峰值下降约 37.6%，优化前后 v2 内容摘要一致。
- Office 容器内 `modified` 时间现统一规范为固定值，修复跨秒生成 XLSX Hash 漂移；全新 v3
  `ci-small` 生成与验证摘要为 `a6e48bc37562ae4cc499c8d8de803a363e138cc3fd2efcde8ecdfb6175eb5ee4`。
  已提交 Fixture 保留为可验证的历史 v2（摘要 `fe860fa3…e489b`），验证器只兼容 v2/v3并拒绝未知版本。
- 大数据分发链路已建立：固定 Release 标签 `synthetic-commerce-v1-data-v3`，确定性 tar/gzip 元数据、
  Archive/Manifest 双重 SHA-256、GitHub Actions 生成与发布门禁，以及本地 `fetch-release` 安全下载、
  全量验证和原子安装。GitHub 发布的 `dev-standard` 压缩包 445,969,395 字节（SHA-256
  `4bcbc5f6…a61237`），`failure-lab` 压缩包 197,895,810 字节（SHA-256 `877e7b22…b01ffd`）；大文件继续
  保持在 Git 历史之外。

## 本地环境限制与 CI 覆盖

1. 当前工作区没有 `docker`/`podman`；基线 GitHub Actions 已实际运行 MySQL 8.4、Redis 8.2、Milvus 2.6、
   Neo4j 5.26.28、容器健康检查、迁移、真实 Adapter、镜像构建和 Trivy，结果通过。
2. 当前工作区的 Chromium 下载受限；GitHub Actions 已安装固定 Playwright Chromium 并完成 E2E 与 axe，结果通过。
3. 未使用真实百炼密钥，符合 CI 禁止收费模型要求；生产前必须运行受控云模型评测与容量测试。

## 下一步

- 下一步优先在可达的真实依赖环境加载已验收 Profile，复验 Anchor Map、引用、拒答和派生索引重建；
  同时保持 `staging-large` 关闭，直到核心事实也改为磁盘工作集并完成单独容量验收。
- 在可达的真实 MySQL/Milvus/Neo4j 开发环境复验正式上传与 Evidence Anchor 映射；之后执行派生索引销毁
  重建和完整 GraphRAG 评测。
- Kafka Relay、Consumer/Inbox/DLQ 核心、修复审计表、独立 Worker 和本地单 Broker Compose 已实现；仍需
  在本地/自托管 Runner 实际运行 Broker，多实例/Rebalance/重启、管理面修复/重放 API 和 `failure-lab`
  终态对账尚未完成。本地单节点 PLAINTEXT 配置不代表生产 Kafka 已部署。
- 已新增逐命令本地部署手册、随机开发密钥引导和指定租户的 MySQL→Milvus/Neo4j 重建工具；重建默认
  dry-run，执行时要求租户确认并核对两个派生索引的 Chunk ID 集合。
- S3 与合成业务 HTTP Adapter 已实现；真实业务 OpenAPI、服务身份、审批矩阵、执行/对账和补偿契约未提供，
  因而 staging/prod 保持 fail-closed。
- 合成数据只能用于工程、安全和容量趋势验证；调整上下文比例、启用模型 Router、真实双专家执行和真实
  业务写入仍需受控评测及相应真实契约。
- 生产部署前仍需提供真实 JWKS、轮换后的数据库/Redis/Neo4j/MinIO 密码和百炼密钥，且不得提交这些值。
