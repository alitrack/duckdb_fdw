# duckdb_fdw 2.0 重构进度 (Native C API Edition)

## 已完成核心重构 🚀
- [x] **架构升级**：彻底放弃 SQLite 兼容层，转向 DuckDB Native C API。
- [x] **连接管理**：重写 `connection.c`，实现基于 `duckdb_open` 和 `duckdb_connect` 的原生连接池。
- [x] **执行引擎**：重写 `IterateForeignScan`，直接从 `duckdb_result` 提取数据，消除中间层损耗。
- [x] **数据转换**：实现 `duckdb_query.c`，使用原生 API 进行高性能类型转换。
- [x] **SQL 生成**：优化 `deparse.c`，支持 `read_parquet` 等 DuckDB 特有语法。
- [x] **清理工程**：物理删除所有 SQLite 遗留文件 (`sqlite3.h`, `sqlite3_api_wrapper.cpp` 等)，重构 `Makefile`。
- [x] **代码净化**：全局清理注释和变量名中的 SQLite 残留影迹。

## 后续演进计划 🛠️
- [ ] **性能加速**：在 `duckdb_optimization.c` 中实装基于 Nanoarrow 的 **Fast-Copy** 批量转换算法。
- [ ] **湖仓一体**：实现 `IMPORT FOREIGN SCHEMA` 对接 Iceberg/Delta Lake。
- [ ] **云端集成**：集成 DuckDB Secrets Manager，支持无感 S3 访问。