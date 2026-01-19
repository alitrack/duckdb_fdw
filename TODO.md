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
- [ ] **支持 `COPY` 协议**：PG 的 COPY 会自动调用 Batch API，因此间接支持了高效 COPY。

## 3. 极致算子下推 (The Power Pushdown) [进行中]
- [x] **基础下推**：`WHERE` 子句、`GROUP BY`、`ORDER BY`、`LIMIT` 已由 `deparse.c` 支持。
- [ ] **Join 下推优化**：需进一步完善 `duckdbGetForeignJoinPaths` 对复杂 Join 条件的判断。
- [ ] **聚合函数覆盖**：增加对 `stddev`, `covar` 等高级统计函数的下推支持。

## 4. 云原生与湖仓集成 (The Modern Stack)
- [ ] **自动化 Schema 演进**：在 `IMPORT FOREIGN SCHEMA` 时，自动识别 Parquet 文件的 Metadata。
- [ ] **动态 Attach 管理**：支持在 PG 中通过 SQL 动态 `ATTACH` 远端的 S3/HTTP 数据库文件。
