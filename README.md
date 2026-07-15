# Neo4j GraphRAG 多智能体客服系统

阶段一是可迁移、可测试的模块化单体：MySQL 为唯一真理源，Milvus/Neo4j 为可重建索引，Redis 保存
Checkpoint 与短状态；LangGraph 编排 Supervisor、FAQ、KB、Escalation 及三个明确标记的 Fake 业务 Agent。

## 离线快速验证

要求 Python 3.12、uv、Node.js 24 和 pnpm 11：

```bash
uv sync --frozen --group dev
uv run pytest -q
uv run python scripts/evaluate.py
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend lint
pnpm --dir frontend test
pnpm --dir frontend build
```

默认 dev/test 使用确定性 Fake，不访问收费模型或真实业务 API。

## 本地启动

1. 复制 `.env.example` 为 `.env`，生成并替换所有密码；不要提交 `.env`。
2. 只启动依赖：`docker compose up -d --wait redis neo4j milvus`。
3. 若使用 Compose MySQL：`docker compose --profile mysql up -d --wait mysql`。
4. 执行迁移：`uv run alembic upgrade head`。
5. 后端：`uv run uvicorn graphrag.main:app --reload`。
6. 前端：`pnpm --dir frontend dev`。

完整容器工作台使用 `docker compose --profile app up -d --build --wait`。真实 Adapter 模式需要百炼密钥；
staging/prod 还必须配置非对称 JWT/JWKS，配置校验会拒绝 Fake、HS256、SQLite 和 MySQL root 账号。

API 文档位于 `/api/v1/docs`，前端默认位于 `http://127.0.0.1:8080`。

## 常用命令

```bash
make lint
make coverage
make evaluate
make openapi
make acceptance
```

`scripts/acceptance.sh` 包含 Playwright；没有浏览器二进制时可先执行其余门禁，浏览器测试由 CI 补跑。

## 文档

- `memory-bank/phase1_env_checklist.md`：权威设计、阶段边界和量化门槛。
- `memory-bank/tech-stack.md`：技术版本与依赖选择。
- `memory-bank/implementation-plan.md`：AI 开发分步实施计划。
- `memory-bank/architecture.md`：当前真实架构、数据流和已知限制。
- `memory-bank/progress.md`：完成项、验证结果和阻塞项。
- `docs/api.md`：端点与调用流程。
- `docs/operations.md`：迁移、故障恢复、重建与生产检查。
- `evaluation/acceptance-report.md`：阶段一验收结论。

## 安全边界

- 生产仅接受 JWKS 验证的外部身份；角色和租户不能由请求正文声明。
- 知识候选在进入模型前必须回 MySQL 做 ACL、状态、版本和有效期复核；依赖故障时拒绝返回。
- Order、Logistics、Refund 阶段一只有 Fake Adapter；退款只创建幂等草单，没有真实 execute 路径。
- 日志、Trace、Agent Step 和 Tool 审计不记录完整敏感正文。
- 任何在历史中出现过的真实密钥都必须轮换，不能因 `.gitignore` 已配置而继续使用。
