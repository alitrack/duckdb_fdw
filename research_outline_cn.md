# 研究大纲：Postgres 与 DuckDB 整合的未来之路

## 1. 研究背景
PostgreSQL（OLTP）与 DuckDB（OLAP）的结合是当前“极简架构”趋势下的核心课题。作为 `duckdb_fdw` 的作者，面临着从单纯的“连接性工具”向“高性能加速引擎”转型的压力。

## 2. 研究目标
- **架构深度解构**：对比 FDW 模式与插件式嵌入模式（pg_duckdb）的优劣。
- **痛点识别**：识别 FDW 在大数据量下的序列化瓶颈及其底层原因。
- **战略转型建议**：为 `duckdb_fdw` 寻找在 pg_duckdb 竞争下的生态位。

## 3. 核心议题
- FDW 协议是否已死？（流式传输与批处理的冲突）
- Table Access Method (TAM) 是否是 DuckDB 的更佳归宿？
- 如何利用 DuckDB 的 Secrets Manager 简化云端数据访问？
- 数据湖（Iceberg/Parquet）在 Postgres 生态中的爆发点。
