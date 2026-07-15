# 合成数据工厂运行手册

## 安全边界

本子项目只生成 `Synthetic/Fake` 数据，不读取 `.env`，不调用收费模型，也不会主动连接 MySQL、
Milvus、Neo4j、Redis 或 Kafka。正式上传和 HTTP 模拟器只有在操作者显式执行对应命令后才会访问指定端点。
模拟器只能绑定回环地址，配置 Schema 不接受生产环境。

`ci-small` 是唯一提交 Git 的 Profile。`dev-standard` 与 `failure-lab` 已完成流式物化和独立离线验收，
产物位于 Git 忽略的 `generated/synthetic-commerce-v1/`。`staging-large` 仍被硬门禁阻止。

## 安装和快速门禁

在 `synthetic-data/` 中执行：

```bash
uv sync --frozen --group dev
uv run ruff format --check src tests
uv run ruff check src tests
uv run mypy src
uv run pytest -q --cov=graphrag_data_factory
uv run graphrag-data validate fixtures/ci-small
```

所有快速门禁离线运行。CI 不会启动大 Profile、外部数据库或收费模型。

## 在新环境下载完整大 Profile

Git 仓库只保存 `ci-small`；已物化的完整大数据通过不可变标签
`synthetic-commerce-v1-data-v3` 的 GitHub Release 分发。在 `synthetic-data/` 目录执行：

```bash
uv sync --frozen --group dev
uv run graphrag-data fetch-release --profile all
```

只需要单个 Profile 时，把 `all` 改为 `dev-standard` 或 `failure-lab`。下载器固定验证发布包大小、
SHA-256 和内部 Manifest SHA-256，拒绝路径穿越、链接和特殊文件，随后在临时目录完成全量验证并原子安装到
`generated/synthetic-commerce-v1/`。目标已存在时会重新验证并复用；只有明确传入 `--replace` 才允许替换。

## 生成和验证

```bash
uv run graphrag-data estimate --profile ci-small
uv run graphrag-data generate --profile ci-small --output fixtures/ci-small --replace
uv run graphrag-data validate fixtures/ci-small
uv run graphrag-data schemas --output schemas
```

导出先写同目录临时树，完成 Schema、关系、权限、金额、时间、重放、文件解析、敏感模式和校验和验证后再
原子发布。`--replace` 只接受具有有效 Synthetic Manifest 的目录。

大 Profile 先校验 `--allow-large`、完全匹配的 `--confirm-profile`、完全匹配资源估算的
`--confirm-estimated-bytes` 和专用 `synthetic-data/generated/` 路径。先执行 `estimate`，再把返回值原样用于
确认，例如：

```bash
uv run graphrag-data estimate --profile failure-lab
uv run graphrag-data generate --profile failure-lab \
  --allow-large \
  --confirm-profile failure-lab \
  --confirm-estimated-bytes 2941757500

uv run graphrag-data estimate --profile dev-standard
uv run graphrag-data generate --profile dev-standard \
  --allow-large \
  --confirm-profile dev-standard \
  --confirm-estimated-bytes 7091015000
```

生成器在目标同级隐藏 staging 目录写入规范 JSONL，每个批次 fsync 后原子更新
`.generation-state.json`。进程中断时正式目标目录不会出现；检查 staging 身份和 Profile Hash 后，使用同一
命令并追加 `--resume`。恢复会校验已提交前缀、截断未提交尾部，并验证所有已完成文件。不要手工改检查点或
`.part` 文件。

全部记录完成后，独立验证器使用临时 SQLite 关系索引逐行检查 Schema、租户、权限、金额、时间、引用、
事件顺序、Inbox 去重、DLQ 修复、回放终态、文件清单和敏感模式；只有通过后才移除检查点并原子发布。
`staging-large` 即使提供全部确认也会停止，因为其核心事实尚未采用磁盘工作集。估算是保守规划值，不是
实测性能。

2026-07-14/15 的当前环境实测（`graphrag-data-factory-v3`，Synthetic Offline，不是生产 SLO）：

| Profile | 结构化记录 | 实际字节 | 文件数 | 生成+全量校验 | 峰值 RSS | Manifest 摘要 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `failure-lab` | 1,380,226 | 1,155,538,493 | 326 | 223.850 秒 | 342,696 KiB | `2c64f88d…98a60` |
| `dev-standard` | 3,372,108 | 2,842,845,571 | 1,226 | 570.486 秒 | 1,447,980 KiB | `5bbea393…f7fe2` |

`dev-standard` 优化前基线为 652.735 秒、2,320,072 KiB；批次派生和校验前释放对象后，内容摘要在 v2
对比运行中保持一致，耗时下降约 12.6%，峰值下降约 37.6%。

## 启动独立业务模拟器

```bash
uv run graphrag-data serve-simulator fixtures/ci-small --host 127.0.0.1 --port 8099
```

请求必须携带 `X-Synthetic-Tenant-Id`、`X-Synthetic-User-Id` 和 `X-Trace-Id`。身份必须存在于数据集，
Header 不能提升用户角色。响应统一带 `source=fake`、`is_synthetic=true`、`scenario_id` 和 `trace_id`。

需要故障注入时，把至少 16 字符的无真实价值测试密钥写到未提交文件，再显式启用：

```bash
uv run graphrag-data serve-simulator fixtures/ci-small \
  --enable-fault-control --control-secret-file /tmp/synthetic-control-secret
```

故障控制端点为 `/synthetic/v1/control/faults`，仅支持 `order`、`logistics`、`refund`、`approval` 的
`off/error/timeout`。普通业务身份不能通过请求正文触发故障。

## 通过正式应用 API 上传知识

先从开发/测试应用获取 `knowledge_admin` Token，并写入权限受限的临时文件；不要把 Token 放入参数、仓库或
文档。然后执行：

```bash
uv run graphrag-data upload fixtures/ci-small \
  --base-url http://127.0.0.1:8000 --token-file /tmp/graphrag-dev-token
```

该命令只上传预期可解析的物理样本并轮询 `/api/v1/ingestion-tasks/{task_id}`。要验证损坏文件拒绝路径，额外
传入 `--include-expected-rejections`。客户端不会直接写数据库或派生索引。

## 清理与重建

默认只输出 dry-run：

```bash
uv run graphrag-data cleanup fixtures/ci-small
```

实际清理必须复制 dry-run 返回的完整 `dataset_id`：

```bash
uv run graphrag-data cleanup generated/synthetic-commerce-v1/example \
  --execute --confirm-dataset-id '<完整 dataset_id>'
```

清理前会重新执行完整验证；缺 Manifest、未登记文件、校验和变化或确认值不一致都会停止。清理只处理数据集
目录内 Manifest 登记文件，不清理数据库、Bucket、Collection、Redis Key 或 Topic。

## 常见失败

- `profile ... requires --allow-large`：未明确选择大规模任务；先运行 `estimate`。
- `generation checkpoint exists`：检测到未完成 staging；检查身份后追加 `--resume`，不要覆盖。
- `resume requested but no generation checkpoint exists`：目标不是可恢复任务；移除 `--resume` 并核对路径。
- `staging-large ... disk-backed working set`：千万事件档位仍被硬禁用，不能用参数绕过。
- `file inventory mismatch`：存在缺失或未登记文件；不要覆盖，先确认来源。
- `checksum mismatch`：产物被修改；从固定 Profile 和 Seed 重建，不要改 Manifest 掩盖差异。
- `physical knowledge parse failed`：多格式生成或解析依赖发生漂移；比较 `uv.lock` 和对应文件 Schema。
- `idempotency_payload_conflict`：同一幂等键被用于不同请求；换新键或恢复原始载荷。
- 上传任务 `partial_failed/failed`：通过正式任务查询和重试 API 排查，不直接修派生索引。
