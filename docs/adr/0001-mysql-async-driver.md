# ADR 0001：使用 aiomysql 代替 asyncmy

- 状态：已接受
- 日期：2026-07-14

## 背景

原技术基线采用 `asyncmy`。依赖审计发现 `asyncmy 0.2.11` 受
`CVE-2025-65896 / GHSA-qhqw-rrw9-25rm` 影响，问题涉及由恶意字典键触发的 SQL 注入，
且截至决策日期 PyPI 没有可升级的修复版本。

## 决策

阶段一使用 SQLAlchemy 2 官方异步方言支持的 `aiomysql`。应用层、领域层、仓库 Port、
ORM 和迁移均不依赖具体驱动，因此只修改依赖和连接 URL。

## 影响

- MySQL URL 使用 `mysql+aiomysql://`。
- 不接受把 `asyncmy` 重新加入运行依赖，除非上游发布修复版本、完成审计并通过真实 MySQL 集成测试。
- 性能与连接池行为必须在真实环境压测；驱动切换不改变数据库事务和 Schema。
