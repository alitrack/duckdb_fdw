# DuckDB FDW 核心功能增强计划 (TODO.md)

## 1. 向量化抓取引擎 (The Arrow Mission) [已初步实现]
- [x] **集成 `duckdb_query_arrow`**：已在 `BeginForeignScan` 中替换原有的 `duckdb_query`。
- [x] **基于 Nanoarrow 的分片缓存**：实现了按 Chunk 抓取 (`duckdb_query_arrow_array`) 和 `ArrowArrayView` 解析。
- [x] **实现向量化类型转换**：
    - [x] 基础类型 (int, float, double, string) 已支持二进制直接转换。
    - [x] 针对 Date, Timestamp, Bool 等类型的转换逻辑已实现。
    - [x] 复杂类型 (List, Struct) 已实现基于 VARCHAR 的稳定回退解析逻辑。
- [ ] **性能压测与优化**：对比新旧实现的 TPC-H 性能。

## 2. 高性能写入优化 (The Appender Mission) [NEXT STEP]
*目前 FDW 的写入通常是一行一个 `INSERT`，效率极低。*
- [ ] **引入 DuckDB Appender API**：在 `ExecForeignInsert` 中放弃 SQL 拼接。
- [ ] **实现批量写入缓存 (Batch Ingestion)**：
    - [ ] 在 `festate` 中开辟一个 Batch Buffer（如 1000 行）。
    - [ ] 当达到阈值或事务提交时，调用 `duckdb_appender_append` 进行向量化高速写入。
- [ ] **支持 `COPY` 协议**：让 PostgreSQL 的 `COPY FROM` 能直接流向 DuckDB 文件。

## 3. 极致算子下推 (The Power Pushdown)
*让 DuckDB 承担更多计算，减少回传给 PG 的数据量。*
- [ ] **完善 Join 下推**：目前 `deparse.c` 对多表关联的下推还比较保守。
- [ ] **聚合函数下推 (Agg Pushdown)**：支持 `COUNT`, `SUM` 等基础聚合。

## 4. 云原生与湖仓集成 (The Modern Stack)
- [ ] **自动化 Schema 演进**：在 `IMPORT FOREIGN SCHEMA` 时，自动识别 Parquet 文件的 Metadata。
- [ ] **动态 Attach 管理**：支持在 PG 中通过 SQL 动态 `ATTACH` 远端的 S3/HTTP 数据库文件。