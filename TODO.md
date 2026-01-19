# DuckDB FDW 核心功能增强计划 (TODO.md)

## 1. 向量化抓取引擎 (The Arrow Mission) [已完成]
- [x] **集成 `duckdb_query_arrow`**：已在 `BeginForeignScan` 中替换原有的 `duckdb_query`。
- [x] **基于 Nanoarrow 的分片缓存**：实现了按 Chunk 抓取 (`duckdb_query_arrow_array`) 和 `ArrowArrayView` 解析。
- [x] **实现向量化类型转换**：
    - [x] 基础类型 (int, float, double, string) 已支持二进制直接转换。
    - [x] Boolean, Date, Timestamp 已支持。
    - [x] **UUID**: 实现了 `FixedSizeBinary(16)` 到 PG `UUID` 的零拷贝转换。
    - [x] **DECIMAL**: 实现了 Arrow Decimal 到 PG `NUMERIC` 的高精度转换。
- [x] **内存管理**：实现了 Arrow Schema/Array 的自动释放与重用。

## 2. 高性能写入优化 (The Appender Mission) [已完成]
- [x] **引入 DuckDB Appender API**：在 `ExecForeignInsert` 中放弃 SQL 拼接，采用二进制直接追加。
- [x] **类型映射**：实现了 PG 基础类型到 Appender API 的映射，包括 `DATE` (Epoch调整)。
- [x] **实现批量写入缓存 (Batch Ingestion)**：
    - [x] 实现了 PG 14+ `GetForeignModifyBatchSize` 和 `ExecForeignBatchInsert` 钩子。
    - [x] 批量大小设置为 2048，匹配 DuckDB 内部向量大小。
- [x] **支持 `COPY` 协议**：PG 的 COPY 会自动调用 Batch API，因此间接支持了高效 COPY。

## 3. 极致算子下推 (The Power Pushdown) [已完成]
- [x] **基础下推**：`WHERE` 子句、`GROUP BY`、`ORDER BY`、`LIMIT` 已由 `deparse.c` 支持。
- [x] **高级函数下推**：
    - [x] 扩展了 `duckdb_foreign_expr_walker` 白名单。
    - [x] 支持 `stddev`, `variance`, `random`, `trunc`, `sqrt`, `power` 等分析与数学函数。
    - [x] 支持显式类型转换 (Cast) 下推（如 `::int4`, `::date`）。
- [x] **Join 下推优化**：通过 Cast 下推的增强，大幅提升了跨表 Join 的覆盖率（已在 `duckdbGetForeignJoinPaths` 框架下生效）。

## 4. 云原生与湖仓集成 (The Modern Stack) [已完成]
- [x] **自动化 Schema 演进**：
    - [x] 增强了 `IMPORT FOREIGN SCHEMA`，支持 `DECIMAL(p,s)` 精度透传。
    - [x] 支持 `UUID` 和 `ARRAY` (e.g. `INTEGER[]`) 的自动类型映射。
- [x] **动态 Attach 管理**：通过 `CREATE SERVER` 的 `attach_catalogs` 选项，已支持在连接时动态挂载 S3/HTTP 数据库文件。