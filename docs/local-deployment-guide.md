# 本地完整部署与阶段二入口验收手册

> 适用分支：`agent/phase2-runtime`
> 适用范围：个人电脑或专用开发服务器上的开发/验收环境
> 不适用范围：直接作为公网生产部署、处理真实客户数据或执行真实退款/改地址
> 最后更新：2026-07-16

这份手册假设操作者没有接触过本项目。除明确标注“可选”或“危险”的步骤外，请按顺序执行，不要跳步。
所有命令默认在仓库根目录运行。Windows 用户统一在 WSL2 的 Ubuntu 终端中执行，不要混用 PowerShell、
CMD 和 WSL 路径。

## 1. 最终会启动什么

完整本地阶段二环境包含以下服务：

| 服务 | 用途 | 本机入口 |
| --- | --- | --- |
| `frontend` | React 管理/客服工作台 | <http://127.0.0.1:8080> |
| `backend` | FastAPI、Agent、入库和 Outbox Relay | <http://127.0.0.1:8000> |
| `mysql` | 唯一业务真理源 | `127.0.0.1:3306` |
| `redis` | Checkpoint、锁、限流和短状态 | `127.0.0.1:6379` |
| `milvus` | 可重建向量索引 | `127.0.0.1:19530` |
| `neo4j` | 可重建知识图谱 | <http://127.0.0.1:7474> / `bolt://127.0.0.1:7687` |
| `object-store` | 知识原文件的独立 MinIO | API `127.0.0.1:9002`，控制台 <http://127.0.0.1:9003> |
| `kafka` | Outbox 事件及索引 Worker 消息 | `127.0.0.1:9092` |
| `vector-worker` | 消费知识事件并重建 Milvus | 无公开端口 |
| `graph-worker` | 消费知识事件并重建 Neo4j | 无公开端口 |
| `etcd`、`minio` | Milvus 自己的内部依赖 | 不暴露给本机 |

`object-store` 与 Milvus 内部 `minio` 是两个不同服务、不同账号和不同 Volume，不要合并。

## 2. 电脑资源要求

推荐配置：

- 64 位 CPU，至少 8 核；
- 32 GB 内存，分配至少 20 GB 给 Docker；
- 仓库所在磁盘至少 50 GB 可用空间；
- 稳定网络，可访问 GitHub、Docker Registry 和阿里云百炼；
- `dev-standard` 解压后约 2.8 GB，Docker 镜像和 Volume 还会额外占用十几 GB。

最低可尝试配置是 16 GB 内存、35 GB 可用磁盘，但 Milvus、Neo4j、Kafka 和构建过程可能明显变慢。

执行以下命令查看资源：

```bash
free -h
df -h .
nproc
```

若 `df -h .` 的可用空间低于 35 GB，先清理或更换磁盘，不要继续下载大数据。

## 3. Windows 11 环境准备（推荐路径）

### 3.1 安装 WSL2 Ubuntu

以管理员身份打开 PowerShell，只执行：

```powershell
wsl --install -d Ubuntu-24.04
wsl --update
```

执行完成后重启电脑。第一次打开 Ubuntu 时，Windows 会要求创建 Linux 用户名和密码。该密码只用于本机
`sudo`，输入时终端不会显示字符，这是正常现象。

### 3.2 安装 Docker Desktop

1. 从 <https://www.docker.com/products/docker-desktop/> 安装 Docker Desktop。
2. 打开 Docker Desktop → Settings → General，确认启用 WSL 2 engine。
3. 打开 Settings → Resources → WSL Integration，启用刚才安装的 Ubuntu。
4. 为 Docker 分配建议 20 GB 内存、8 个 CPU；保存并重启 Docker Desktop。

如果需要手工限制 WSL 资源，在 Windows 用户目录创建 `.wslconfig`：

```ini
[wsl2]
memory=20GB
processors=8
swap=8GB
```

修改后在 PowerShell 执行 `wsl --shutdown`，再重新打开 Ubuntu和 Docker Desktop。

### 3.3 在 Ubuntu 中安装基础命令

打开 Ubuntu 终端：

```bash
sudo apt update
sudo apt install -y git curl jq ca-certificates build-essential python3
```

验证 Docker 已接入 WSL：

```bash
docker version
docker compose version
git --version
python3 --version
jq --version
```

五条命令都必须成功。若 `docker` 不存在，回到 Docker Desktop 的 WSL Integration 设置检查 Ubuntu 是否启用。

## 4. macOS 或 Linux 环境准备

macOS 安装 Docker Desktop 和 Git；Ubuntu Linux 按 Docker 官方文档安装 Docker Engine 与 Compose Plugin。
安装完成后执行：

```bash
docker version
docker compose version
git --version
python3 --version
```

Linux 若出现 Docker Socket 权限错误，可将当前用户加入 `docker` 组，然后注销并重新登录：

```bash
sudo usermod -aG docker "$USER"
```

不要通过长期使用 `sudo docker ...` 掩盖权限问题。

## 5. 安装 uv

本项目的应用由 Docker 运行，但下载/验证合成数据和执行验收脚本需要 `uv`：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"
uv --version
```

如果最后一条仍提示找不到命令，关闭终端重新打开，再执行 `uv --version`。

## 6. 下载并切换到阶段二代码

选择一个不在 Windows `/mnt/c` 下的 WSL 目录；Linux 文件系统的 Docker 构建速度更快：

```bash
cd "$HOME"
git clone https://github.com/wyj-wjj/neo4j_graphrag.git
cd neo4j_graphrag
git fetch origin --prune
git switch --track origin/agent/phase2-runtime
git pull --ff-only
git status --short
git log -1 --oneline
```

`git status --short` 此时应当没有输出。若最后显示了本地修改，不要执行 reset；先备份修改并确认来源。

## 7. 创建本地密钥与配置

### 7.1 处理旧 `.env`

若以前已有 `.env`，先改名保留：

```bash
if [ -f .env ]; then
  mv .env ".env.backup.$(date +%Y%m%d-%H%M%S)"
fi
```

聊天中曾经出现或上传过的真实密码、Token 和模型 Key 都应轮换，不要继续沿用。

### 7.2 自动生成开发环境密钥

```bash
python3 scripts/bootstrap_local_env.py
chmod 600 .env
```

脚本会：

- 为 MySQL、Redis、Neo4j、Milvus 内部 MinIO、业务对象存储和开发 JWT 生成不同随机值；
- 自动配置真实 MySQL/Redis/Milvus/Neo4j/S3 Adapter；
- 打开本地 Kafka 与 Outbox Relay；
- 不打印任何生成的密钥；
- 不生成、猜测或覆盖百炼 `LLM_API_KEY`；
- 已有 `.env` 时拒绝覆盖。

### 7.3 填入百炼 API Key

打开文件：

```bash
nano .env
```

找到：

```text
LLM_API_KEY=
```

在等号后粘贴你新建或轮换后的百炼 Key。按 `Ctrl+O`、回车保存，再按 `Ctrl+X` 退出。

不要把 `.env`、Key 或完整连接字符串发到聊天、截图、Issue、PR 或 Git。验证配置时只执行：

```bash
test -s .env && echo ".env exists"
test -n "$(sed -n 's/^LLM_API_KEY=//p' .env)" && echo "LLM_API_KEY is set"
if grep -nE '^[A-Z0-9_]+=(replace-me|replace-with)' .env; then
  echo "ERROR: placeholder remains"
else
  echo "No secret placeholder remains"
fi
git status --short --ignored .env
```

预期最后一条显示 `!! .env`，表示 Git 已忽略该文件。

## 8. 第一次拉取镜像并构建应用

先验证 Compose，不输出展开后的密钥：

```bash
docker compose --profile app --profile phase2 config --quiet
docker compose --profile app --profile phase2 config --services
```

应看到 12 个服务。然后拉取固定镜像：

```bash
docker compose pull redis neo4j etcd minio object-store kafka milvus mysql
```

构建后端、两个 Worker 和前端：

```bash
docker compose --profile app --profile phase2 build --pull \
  backend vector-worker graph-worker frontend
```

第一次执行可能需要 10–30 分钟。不要在构建中途关闭 Docker Desktop。

## 9. 按顺序启动完整环境

### 9.1 启动基础设施

```bash
docker compose --profile app --profile phase2 up -d --wait \
  mysql redis neo4j etcd minio milvus object-store kafka
```

查看状态：

```bash
docker compose --profile app --profile phase2 ps
```

上述基础服务应显示 `Up` 或 `healthy`。Milvus 首次启动可能等待约 2 分钟。

### 9.2 显式创建 Kafka Topic

本地只有一个 Broker，所以副本数固定为 1；这只是开发配置，不是生产配置：
Compose 中的 KRaft 环境变量遵循
[Apache Kafka 官方 Docker 示例](https://github.com/apache/kafka/tree/trunk/docker/examples/docker-compose-files/single-node/plaintext)。

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create --if-not-exists \
  --topic knowledge.events.v1 \
  --partitions 3 \
  --replication-factor 1

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create --if-not-exists \
  --topic action.events.v1 \
  --partitions 3 \
  --replication-factor 1

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --list
```

最后应至少显示 `knowledge.events.v1` 和 `action.events.v1`。

### 9.3 启动后端并自动迁移数据库

```bash
docker compose --profile app --profile phase2 up -d --wait backend
docker compose logs --no-color --tail=200 backend
```

后端启动命令会依次创建本地对象 Bucket、执行 `alembic upgrade head`、启动 API 和 Outbox Relay。

检查存活和依赖就绪：

```bash
curl -fsS http://127.0.0.1:8000/api/v1/health/live | jq
curl -fsS http://127.0.0.1:8000/api/v1/health/ready | jq
```

第二条必须显示顶层 `"ready": true`。依赖中应包含 MySQL、Redis、Milvus、Neo4j、模型、Kafka、对象存储
和明确标记为 Fake 的本地业务 Adapter。Fake 业务 Adapter 不能执行真实订单操作。

### 9.4 启动两个索引 Worker 和前端

```bash
docker compose --profile app --profile phase2 up -d \
  vector-worker graph-worker frontend
sleep 10
docker compose --profile app --profile phase2 ps
```

确认 `vector-worker`、`graph-worker` 没有反复重启：

```bash
docker compose logs --no-color --tail=100 vector-worker
docker compose logs --no-color --tail=100 graph-worker
```

最后在浏览器打开：

- 工作台：<http://127.0.0.1:8080>
- API 文档：<http://127.0.0.1:8000/api/v1/docs>
- Neo4j Browser：<http://127.0.0.1:7474>
- 对象存储控制台：<http://127.0.0.1:9003>

控制台密码在本地 `.env`，不要复制到聊天。

## 10. 创建本地管理员 Token

Token 写入仓库外的权限受限文件，不要直接打印：

```bash
mkdir -p "$HOME/graphrag-local-secrets"
chmod 700 "$HOME/graphrag-local-secrets"
TOKEN_FILE="$HOME/graphrag-local-secrets/knowledge-admin.token"
umask 077
curl -fsS -X POST http://127.0.0.1:8000/api/v1/auth/dev-token \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"local-admin","roles":["user","admin","knowledge_admin"],"expires_minutes":120}' \
  | jq -r '.access_token' > "$TOKEN_FILE"
test -s "$TOKEN_FILE" && echo "admin token file created"
```

Token 最长有效 120 分钟。过期后重复本节命令即可，不要延长开发 Token 的代码上限。

## 11. 先用小数据做冒烟测试

安装冻结的 Python 依赖：

```bash
uv sync --frozen --group dev
uv sync --project synthetic-data --frozen --group dev
```

验证仓库已附带的小数据：

```bash
uv run --project synthetic-data graphrag-data validate \
  synthetic-data/fixtures/ci-small
```

通过正式上传 API 入库：

```bash
TOKEN_FILE="$HOME/graphrag-local-secrets/knowledge-admin.token"
uv run --project synthetic-data graphrag-data upload \
  synthetic-data/fixtures/ci-small \
  --base-url http://127.0.0.1:8000 \
  --token-file "$TOKEN_FILE"
```

命令输出中所有正常样本的 `task.status` 都应为 `completed`。该步骤会调用百炼 Embedding、图抽取及相关模型，
会产生受控费用。

检查 Kafka 消费组：

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --list

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 \
  --describe --group graphrag-vector-index-v1

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 \
  --describe --group graphrag-graph-index-v1
```

查看数据库事件终态，不在命令行暴露密码：

```bash
docker compose exec -T mysql sh -lc \
  'mysql -uapp_user -p"$MYSQL_PASSWORD" enterprise_agent_db -e "
   SELECT published, COUNT(*) AS total FROM outbox_events GROUP BY published;
   SELECT consumer_name, status, COUNT(*) AS total
   FROM inbox_events GROUP BY consumer_name, status;
   SELECT status, COUNT(*) AS total FROM dead_letter_events GROUP BY status;
  "'
```

正常情况下 Outbox 最终应发布，两个 Consumer Group 应有 `processed` 记录，DLQ 为空或只有明确测试的毒消息。

## 12. 下载并运行 `dev-standard` 正式入口验收

### 12.1 下载不可变 Release

确保还有至少 10 GB 可用空间：

```bash
df -h .
uv run --project synthetic-data graphrag-data fetch-release \
  --profile dev-standard
```

下载器会验证归档 SHA-256、Manifest SHA-256、路径安全和全部数据 Schema。不要自行解压或修改 Manifest。

再次创建新 Token，避免旧 Token 在大批量上传中途过期，然后执行：

```bash
TOKEN_FILE="$HOME/graphrag-local-secrets/knowledge-admin.token"
umask 077
curl -fsS -X POST http://127.0.0.1:8000/api/v1/auth/dev-token \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"local-admin","roles":["user","admin","knowledge_admin"],"expires_minutes":120}' \
  | jq -r '.access_token' > "$TOKEN_FILE"
```

### 12.2 运行只走正式 API 的验收脚本

报告保存在仓库外：

```bash
mkdir -p "$HOME/graphrag-acceptance"
uv run python scripts/phase2_entry_acceptance.py \
  synthetic-data/generated/synthetic-commerce-v1/dev-standard \
  --base-url http://127.0.0.1:8000 \
  --token-file "$HOME/graphrag-local-secrets/knowledge-admin.token" \
  --output "$HOME/graphrag-acceptance/phase2-entry-api-ingestion.json"
```

检查结果：

```bash
jq '{status, git_commit, dataset_id, dataset_version, upload_count, remaining_gate}' \
  "$HOME/graphrag-acceptance/phase2-entry-api-ingestion.json"
```

预期状态是 `api_ingestion_passed_rebuild_and_failure_checks_pending`。这意味着正式上传通过，但派生索引重建和
故障矩阵还需要继续执行，不能手工改成“全部通过”。

## 13. 执行 Milvus/Neo4j 删除与 MySQL 重建验收

这是会删除指定租户派生索引的操作。MySQL 和对象原文件不会删除；请先确认上一节上传已全部完成，且当前
没有其他人在使用 `default` 租户。重建会重新调用 Embedding 和图抽取模型并产生费用。

先停止消费者，避免与重建并发：

```bash
docker compose stop vector-worker graph-worker
```

先只做 dry-run：

```bash
docker compose --profile app --profile phase2 run --rm --no-deps backend \
  python scripts/rebuild_derived_indexes.py \
  --tenant-id default
```

记录 `active_documents` 和 `expected_child_chunks`。确认租户确实是 `default` 后再执行：

```bash
docker compose --profile app --profile phase2 run --rm --no-deps backend \
  python scripts/rebuild_derived_indexes.py \
  --tenant-id default \
  --execute \
  --confirm-tenant-id default \
  > "$HOME/graphrag-acceptance/derived-index-rebuild.json"
```

验证：

```bash
jq . "$HOME/graphrag-acceptance/derived-index-rebuild.json"
```

必须满足：

- `status` 为 `completed`；
- `expected_child_chunks`、`vector_ids`、`graph_ids` 三者相等；
- 四个 `missing`/`unexpected` 数组都为空。

重新启动消费者：

```bash
docker compose --profile app --profile phase2 up -d vector-worker graph-worker
sleep 5
docker compose ps vector-worker graph-worker
```

## 14. 基本检索、引用和跨租户检查

最简单的方法是在 <http://127.0.0.1:8080> 使用工作台：

1. 登录或获取本地测试身份；
2. 新建会话；
3. 针对刚上传的知识提出可由文档回答的问题；
4. 确认答案包含引用，且引用能定位到文档；
5. 提问数据中不存在的事实，确认系统拒答而不是编造；
6. 创建另一个租户身份时，确认不能看到 `default` 租户文档。

跨租户、依赖故障和部分写入属于安全验收。不要直接编辑 MySQL ACL 或伪造 JWT Claim；应使用测试身份、
`failure-lab` 和正式 API。当前自动入口报告会把这些项目继续列为 `remaining_gate`，完成证据后再冻结总报告。

## 15. 每次重启电脑后的启动方式

Docker Desktop 启动后，在仓库根目录执行：

```bash
docker compose --profile app --profile phase2 up -d --wait
docker compose --profile app --profile phase2 ps
curl -fsS http://127.0.0.1:8000/api/v1/health/ready | jq
```

数据保存在 Docker Volume 中，普通 `stop`、`down` 或电脑重启不会删除数据。

## 16. 正常停止、更新和备份

### 16.1 正常停止

```bash
docker compose --profile app --profile phase2 stop
```

停止并移除容器但保留全部 Volume：

```bash
docker compose --profile app --profile phase2 down
```

### 16.2 更新代码

```bash
git status --short
git pull --ff-only
docker compose --profile app --profile phase2 build \
  backend vector-worker graph-worker frontend
docker compose --profile app --profile phase2 up -d --wait
```

若 `git status --short` 有输出，先确认本地改动，不能直接覆盖。

### 16.3 备份 MySQL

```bash
mkdir -p "$HOME/graphrag-backups"
docker compose exec -T mysql sh -lc \
  'exec mysqldump -uapp_user -p"$MYSQL_PASSWORD" --single-transaction enterprise_agent_db' \
  > "$HOME/graphrag-backups/mysql-$(date +%Y%m%d-%H%M%S).sql"
ls -lh "$HOME/graphrag-backups"
```

Milvus 和 Neo4j 是可重建派生索引，但 MySQL 与知识原文件不是。正式环境还必须单独备份对象 Bucket；本地
合成验收可以从固定 Release 重新上传恢复。

## 17. 危险：彻底清空本地环境

下面命令会永久删除该 Compose 项目的 MySQL、Redis、Neo4j、Milvus、Kafka、MinIO 和对象存储 Volume。
只有确认全部是可丢弃的合成开发数据、备份已经验证后才能执行：

```bash
docker compose --profile app --profile phase2 down --volumes --remove-orphans
```

此操作通常不可恢复。不要为了修复单个服务问题先执行它。

## 18. 常见问题排查

### 18.1 某个容器不健康

```bash
docker compose --profile app --profile phase2 ps
docker compose logs --no-color --tail=300 <服务名>
```

将 `<服务名>` 换成 `backend`、`milvus`、`kafka`、`vector-worker` 等。不要直接发送包含 Token 的请求日志。

### 18.2 `backend` 反复重启

```bash
docker compose logs --no-color --tail=300 backend
```

常见原因：

- `.env` 中 `LLM_API_KEY` 为空；
- Neo4j/MySQL/Redis 密码不一致；
- Kafka 尚未健康；
- 对象存储 Bucket 无法创建；
- 迁移失败。

不要手工改数据库版本表。保留日志后再修配置并重新启动。

### 18.3 Milvus 启动超时或电脑卡顿

先确认 Docker 至少获得 12–20 GB 内存：

```bash
docker stats --no-stream
free -h
```

增加 Docker 内存并重启；不要同时运行其他大型模型或虚拟机。

### 18.4 端口被占用

```bash
ss -ltnp | grep -E ':(3306|6379|7474|7687|8000|8080|9002|9003|9092|19530)\b'
```

先停止占用端口的旧服务。不要随意改一半端口；容器内部地址和本机地址不同。

### 18.5 Kafka Worker 没有消费

```bash
docker compose logs --no-color --tail=300 kafka vector-worker graph-worker
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --list
```

确认 Topic 已创建、Worker 没有重启、Backend 使用 `kafka:19092`，本机工具才使用 `127.0.0.1:9092`。

### 18.6 `fetch-release` 下载失败

检查 GitHub 网络和磁盘空间，重新执行相同命令。下载器会验证并复用完整目标；不要关闭校验、手工修改归档
或把不完整目录改名为正式数据集。

### 18.7 百炼调用 401、429 或超时

- 401：轮换 Key，确认 Key 所属区域与 Endpoint；
- 429：降低并发或等待配额恢复；
- 超时：检查网络，再按任务重试 API 恢复，不直接改派生索引状态；
- 所有真实模型调用都可能收费，先用 `ci-small` 控制范围。

## 19. 可以安全发给协作者或 GPT 的材料

可以发送：

```bash
docker compose --profile app --profile phase2 ps
curl -fsS http://127.0.0.1:8000/api/v1/health/ready | jq
```

以及以下两个报告：

- `$HOME/graphrag-acceptance/phase2-entry-api-ingestion.json`
- `$HOME/graphrag-acceptance/derived-index-rebuild.json`

日志发送前先人工检查：

```bash
docker compose logs --no-color --tail=300 \
  backend vector-worker graph-worker \
  > "$HOME/graphrag-acceptance/runtime-logs.txt"
```

绝对不要发送：

- `.env` 或 `.env.backup.*`；
- Token 文件；
- 百炼 Key、数据库密码、对象存储密码；
- 完整真实业务数据或未脱敏日志。

## 20. 可选：配置 GitHub Self-hosted Runner

建议先完成本手册的本地人工验收，再配置 Runner：

1. 打开仓库 GitHub 页面 → Settings → Actions → Runners；
2. 选择 New self-hosted runner，并选择你的操作系统；
3. 使用独立低权限系统账号执行 GitHub 页面临时生成的注册命令；
4. 添加标签 `phase2-integration`；
5. 把 Runner 工作目录与个人文件隔离；
6. 不要把注册 Token、`.env` 或 Runner 服务账号密码发给任何人；
7. 后续工作流只允许受保护分支或受信任维护者触发。

Runner 让 GitHub Actions 在你的电脑上执行命令，但不等于开放公网数据库端口，也不应提供任意交互式远程
Shell。配置完成后只需告知“Runner online”和标签，不要提供注册 Token。

## 21. 本地验收完成标准

只有同时具备下列证据，才可把阶段二入口标为通过：

- Compose 12 个服务状态正常；
- `/health/ready` 为 true，核心依赖不是 Fake；
- `dev-standard` 通过固定 Release 校验并只走正式上传 API；
- 所有正常上传任务完成；
- Outbox 发布、两个 Inbox Consumer 终态和 Kafka Group 正常；
- Milvus/Neo4j 删除后，从 MySQL 重建并通过 Chunk ID 集合核对；
- 引用、拒答、跨租户隔离、依赖故障和部分写入补偿有冻结报告；
- 报告绑定 Git Commit、数据集和 Manifest；
- `.env`、Token 和真实 Key 从未进入 Git 或聊天。

真实业务 Adapter、生产审批、真实退款执行、生产配额和灾备演练仍需要真实业务契约，不能因为本地合成验收
通过就视为生产上线完成。
