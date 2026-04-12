# DuckDB FDW 核心功能增强计划 (TODO.md)

## 1. 读路径现状校准 (Current Read Path Reality)
- [x] **当前已实现的是 DuckDB result chunk 扫描**：`BeginForeignScan` 仍通过 `duckdb_query` / `duckdb_execute_prepared` 执行查询，再用 `duckdb_result_get_chunk` 取块。
- [x] **当前 chunk scan 只覆盖部分 PG 标量类型**：布尔、整数、浮点、日期、时间戳可直接走块读取；其他类型仍会回退到 `duckdb_value_*`/文本转换。
- [ ] **完整 Arrow C Data 主路径尚未实现**：仓库当前没有把 `duckdb_query_arrow` / Nanoarrow 接到主扫描循环。
- [ ] **后续 Arrow 目标**：在不破坏现有 chunk-result 路径稳定性的前提下，引入真正的 Arrow C Data 读取与类型转换。

## 2. 高性能写入优化 (The Appender Mission)
- [x] **引入 DuckDB Appender API**：在 `ExecForeignInsert` 中放弃 SQL 拼接，采用二进制直接追加。
- [x] **类型映射**：实现了 PG 基础类型到 Appender API 的映射，包括 `DATE` (Epoch调整)。
- [x] **实现批量写入缓存 (Batch Ingestion)**：
    - [x] 实现了 PG 14+ `GetForeignModifyBatchSize` 和 `ExecForeignBatchInsert` 钩子。
    - [x] 批量大小设置为 2048，匹配 DuckDB 内部向量大小。
- [x] **支持 `COPY` 协议**：PG 的 COPY 会自动调用 Batch API，因此间接支持了高效 COPY。

## 3. 极致算子下推 (The Power Pushdown)
- [x] **基础下推**：`WHERE` 子句、`GROUP BY`、`ORDER BY`、`LIMIT` 已由 `deparse.c` 支持。
- [x] **高级函数下推**：
    - [x] 扩展了 `duckdb_foreign_expr_walker` 白名单。
    - [x] 支持 `stddev`, `variance`, `random`, `trunc`, `sqrt`, `power` 等分析与数学函数。
    - [x] 支持显式类型转换 (Cast) 下推（如 `::int4`, `::date`）。
- [x] **Join 下推优化**：通过 Cast 下推的增强，大幅提升了跨表 Join 的覆盖率（已在 `duckdbGetForeignJoinPaths` 框架下生效）。

## 4. 云原生与湖仓集成 (The Modern Stack)
- [x] **自动化 Schema 演进**：
    - [x] 增强了 `IMPORT FOREIGN SCHEMA`，支持 `DECIMAL(p,s)` 精度透传。
    - [x] 支持 `UUID` 和 `ARRAY` (e.g. `INTEGER[]`) 的自动类型映射。
- [x] **动态 Attach 管理**：通过 `CREATE SERVER` 的 `attach_catalogs` 选项，已支持在连接时动态挂载 S3/HTTP 数据库文件。
