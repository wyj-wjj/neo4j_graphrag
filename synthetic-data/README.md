# GraphRAG 合成数据工厂

这是与生产运行时隔离的可移植子项目，用固定 Profile 与 Seed 生成完全虚构的中文电商数据。它不读取
`.env`，不连接 MySQL、Milvus、Neo4j、Redis 或收费模型，也不执行任何真实业务写操作。

## 目录职责

- `src/graphrag_data_factory/`：确定性生成、导出、验证和命令行工具。
- `profiles/`：版本化规模、Seed、时间和分布配置。
- `schemas/`：供其他语言和 AI 使用的 JSON Schema 快照。
- `templates/`：知识与会话模板说明；事实仍由生成器决定。
- `tests/`：Schema、确定性、业务不变量和安全门禁。
- `docs/`：运行手册、数据字典、状态机和安全清理说明。
- `fixtures/ci-small/`：可提交 Git 的小型基线数据集。
- `manifests/`：Manifest 使用说明；每个数据集的权威 Manifest 与产物同目录。
- `generated/`：可重建的大规模产物，已被根 `.gitignore` 忽略。

## 快速使用

在本目录执行：

```bash
uv sync --frozen --group dev
uv run graphrag-data generate --profile ci-small --output fixtures/ci-small --replace
uv run graphrag-data validate fixtures/ci-small
uv run pytest -q
uv run ruff check .
uv run mypy src
```

`dev-standard` 和 `failure-lab` 已支持批次流式、fsync 检查点、显式恢复、磁盘型独立验证和原子发布。
它们必须同时提供 `--allow-large`、精确 Profile 名、精确估算字节和 `generated/` 专用路径；中断后只有
显式 `--resume` 才会继续。`staging-large` 仍被硬门禁阻止，因为百万订单核心事实尚未改为磁盘工作集。
普通 Git 只提交 `fixtures/ci-small/`；大规模产物位于已忽略的 `generated/`，可在本地重建，或作为带
Manifest 与校验和的 Release/LFS/对象存储产物分发。

已验收的大 Profile 同时发布为不可变 GitHub Release。新环境可直接下载、校验并原子安装：

```bash
uv run graphrag-data fetch-release --profile all
```

也可以只下载 `dev-standard` 或 `failure-lab`。命令会固定校验 Release 标签、压缩包大小、压缩包
SHA-256、内部 Manifest SHA-256，并在解压后运行完整数据验证；详情见 `release/README.md`。

## 安全边界

- 每条业务记录都包含 `is_synthetic=true` 和 `source=fake`。
- 联系方式只使用 `.invalid`、遮罩值和不存在的“测试省/演示市/样例区”。
- 动态订单、物流、退款事实只供未来模拟业务 Port 使用，不得写入 RAG 或长期记忆。
- 知识源文件只在这里生成；进入应用时仍必须经过正式上传、解析、分块和索引链路。
- 清理工具不得使用通配删除；本阶段未提供数据库或外部资源清理能力。

当前交付包括 `ci-small`、已物化的 `dev-standard`/`failure-lab`、HTTP 业务模拟器、事件离线重放 Oracle
和正式上传客户端。大 Profile 已完成文件级生成与独立离线验收；生产 Kafka、真实依赖入库和 GraphRAG
全链路验收仍属于后续里程碑。

已实现的独立 HTTP 模拟器、正式上传客户端、多格式样本、评测评分和安全清理使用方式见
[`docs/runbook.md`](docs/runbook.md)；记录语义见 [`docs/data-dictionary.md`](docs/data-dictionary.md)。
当前验收边界见 [`docs/acceptance-report.md`](docs/acceptance-report.md)。
“正式上传客户端已实现”不等于已在本环境完成 MySQL/Milvus/Neo4j 入库；真实依赖验收仍需操作者提供可达的
开发/测试环境和显式 Token。
