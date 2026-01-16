# 附录 A：duckdb_fdw 技术演进路线图 (Technical Roadmap)

本文档是对主研究报告的补充，旨在为 `duckdb_fdw` 作者提供**工程落地**层面的具体指导，特别聚焦于利用 **Nanoarrow** 和 **DuckLake** 技术带来的新机遇。

## 路径 A：打造极致的 ETL 加速器 (The Turbo Loader)

### 战略目标
放弃与 `pg_duckdb` 在复杂分析查询上的性能竞争，转而成为 **Postgres 生态中最快的数据加载工具**。

### 工程实施细节
1.  **Postgres 14+ 批量 API**: 实现 `GetForeignModifyBatchSize` 和 `ExecForeignBatchInsert`。
2.  **DuckDB Appender**: 在 `ExecForeignBatchInsert` 中，利用 DuckDB 的 `duckdb_append_row` 系列函数，绕过 SQL 解析器，将 Postgres 传递来的 Tuple 数组直接写入 DuckDB。

---

## 路径 B：基于 DuckLake 的智能网关 (The Lakehouse Gateway)

### 战略目标
利用 DuckDB 强大的 Iceberg/Delta 支持，解决 FDW 手动定义表结构的痛点，实现“无感”连接数据湖。

### 核心技术点：IMPORT FOREIGN SCHEMA
**目标**：用户执行 `IMPORT FOREIGN SCHEMA "s3://bucket/iceberg_db" FROM SERVER duckdb_srv INTO public;` 即可自动创建所有外表。

**实现步骤**：
1.  **挂载 DuckLake**：在连接建立时，执行 DuckDB SQL：`INSTALL iceberg; LOAD iceberg;`。
2.  **获取元数据**：在 `ImportForeignSchema` 回调中，向 DuckDB 发送元数据查询：
    ```sql
    -- 伪代码
    SELECT table_name, column_name, data_type 
    FROM duckdb_tables 
    WHERE schema_name = 'iceberg_db';
    ```
3.  **类型映射**：将 DuckDB 的复杂嵌套类型（Struct/List）映射为 Postgres 的 JSONB，或扁平化处理。
4.  **生成 DDL**：基于查询结果，动态构建 `CREATE FOREIGN TABLE` 语句并执行。

### 核心技术点：分区裁剪 (Partition Pruning)
- 确保 FDW 的 `GetForeignPaths` 阶段能正确解析 `WHERE` 子句。
- 将时间、日期等过滤条件准确转换为 DuckDB SQL，以便 DuckDB 的 Iceberg Scanner 能读取 manifest 文件并跳过无关分区。

---

## 路径 C：基于 Nanoarrow 的 Fast-Copy 引擎 (Performance Breakthrough)

### 战略目标
利用 **Nanoarrow** 库处理 Arrow C Data Interface，消除 FDW 最大的性能瓶颈——行式迭代与函数调用开销。

### 技术原理
传统的 `duckdb_fdw` 是“拉（Pull）”模式，Postgres 拉一行，FDW 问 DuckDB 要一行。
新的 **Fast-Copy** 模式是“批（Batch）”模式。

### 实现步骤
1.  **引入 Nanoarrow**：将 `nanoarrow.h` / `nanoarrow.c` 引入项目（零依赖，非常轻量）。
2.  **重构 IterateForeignScan**：
    - **Buffer State**：在 FDW 的扫描状态（ScanState）中增加一个 `ArrowArray` 缓冲区。
    - **批量获取**：当缓冲区为空时，调用 `duckdb_query_arrow_array` 一次性获取（例如）2048 行数据。
    - **Nanoarrow 解析**：使用 `ArrowArrayView` 快速访问内存中的列数据。
    - **紧凑循环 (Tight Loop)**：
        ```c
        // 伪代码：从 Arrow 列读取数据填充 Postgres Slot
        // 这种连续内存访问对 CPU Cache 极其友好
        for (int i = 0; i < batch_size; i++) {
             slot->tts_values[col] = ArrowArrayViewGetInt64(view, i);
        }
        ```
3.  **性能预期**：
    - 减少了 99% 的 `duckdb_value_xxx` 函数调用。
    - 避免了 DuckDB 内部将 Vector 拆解为 Value 的开销。
    - 吞吐量有望提升 **3-5 倍**，接近 Postgres FDW 的理论极限。

---

## 总结
通过引入 **DuckLake**（提升易用性）和 **Nanoarrow**（提升吞吐量），`duckdb_fdw` 可以在不破坏 FDW 标准架构的前提下，实现质的飞跃。这不仅是性能优化，更是产品形态的升级。