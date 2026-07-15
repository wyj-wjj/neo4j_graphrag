# 阶段一验收报告

- 日期：2026-07-14
- 数据集：`synthetic-phase1-v1`
- 结果文件：`evaluation/report.json`

## 已通过

- 后端 Ruff 与 mypy strict；阶段 1.5 后 pytest 95 通过、1 个真实依赖用例按环境条件跳过，
  分支覆盖率 86.05%。
- 单元、SQLite 集成、安全、API 端到端、迁移与评测测试。
- 文档上传、三库状态补偿、版本激活/下线、检索、引用、拒答、会话和全 Agent/Fake 路径。
- Golden Set 20 条：路由准确率、Recall@20、MRR@20、NDCG@10、引用正确率、Faithfulness、
  无答案拒答率均为 1.0；这是确定性离线指标，不代表生产延迟或真实模型质量。
- 前端 ESLint/TypeScript、Vitest 和 Vite 生产构建。
- pip-audit 无已知第三方漏洞；本地项目本身因未发布到 PyPI 不在审计数据库中。
- `memory-reliability-v1` 的 4/8/12/16 轮约束、上下文预算、幂等、状态迁移和身份隔离门槛全部通过；
  该报告明确标记为 `synthetic_fake_local`，不能解释为生产 SLO。
- 阶段 1.5 的会话一致性、上下文、状态恢复、真实流式、Tool 可靠性、长期记忆治理、Safety Port、
  Prompt/Generation Manifest、知识质量报告和受控多专家结果收敛契约已实现。

## CI 外部环境验收

当前工作环境无 Docker CLI，且 Playwright Chromium 下载被网络环境截断，因此以下项目不是本地验收结果，
而是由 GitHub Actions `ci` 运行 `29383348044` 完成并通过：

- MySQL 8.4、Redis 8.2、Milvus 2.6、Neo4j 5.26 真实 Adapter 与迁移；
- 后端/前端镜像构建、非 root 运行、健康检查和 Trivy；
- Chromium E2E、键盘流程和 axe 自动可访问性扫描。

该运行的 `backend`、`frontend`、`real-integration`、`images` 和 `secrets` 五个 Job 全绿；阶段一外部环境
验收已完成。以后修改依赖、镜像、迁移、真实 Adapter 或前端主流程时，仍必须重新通过相同门禁。

## 生产前非 CI 项

- 使用受控百炼账号运行真实模型质量、P50/P95、首 Token 和容量测试。
- 验证真实 JWKS 轮换、密钥轮换、备份恢复、数据保留和告警。
- 用目标业务 ACL 数据扩充 Golden Set；当前无真实业务数据，合成结果不能替代业务验收。
