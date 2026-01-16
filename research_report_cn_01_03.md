# 第一章：Postgres 迈向全能型数据库的趋势

## 1.1 "Postgres 统治一切" 的浪潮
在当今的数据库领域，PostgreSQL 已经不再仅仅是一个关系型数据库，它正逐渐成为一种“数据操作系统”。通过极其灵活的插件机制，Postgres 成功吞噬了地理空间（PostGIS）、向量检索（pgvector）、时序数据（TimescaleDB）等多个垂直领域。

对于开发者而言，减少技术栈的复杂性是核心驱动力。如果能在一个 Postgres 连接中处理 90% 的业务，没人愿意再去维护一套复杂的 Spark 或 Snowflake 集群。这种趋势直接催生了对 Postgres 增强型分析能力的需求。

## 1.3 性能鸿沟：为什么 Postgres 需要加速器
Postgres 传统的 Volcano（火山模型）执行引擎在处理大规模聚合查询时存在天然弱点：
- **CPU 效率低下**：逐行处理（Row-at-a-time）导致极高的指令跳转开销。
- **I/O 浪费**：行存模式下，即使只查一列，也必须读取整个数据块。
- **无法利用现代硬件**：难以有效利用 SIMD（单指令多数据流）等现代 CPU 特性。

DuckDB 的出现，恰好填补了这一鸿沟。它作为“分析界的 DuckDB”，为 Postgres 提供了一个高性能、零依赖的计算插件的可能性。

---

# 第二章：PostgreSQL 扩展机制的深度剖析

## 2.1 外部数据包装器 (FDW) 的地位与局限
FDW（Foreign Data Wrapper）是 `duckdb_fdw` 的基石。它基于 SQL/MED 标准，为 Postgres 提供了跨源查询的标准接口。

**FDW 的优势：**
- **标准、稳定**：API 极少变动，兼容性强。
- **松耦合**：对 Postgres 核心侵入极小。

**FDW 的致命伤：**
- **元组序列化开销**：FDW 要求将每一行数据转换为 Postgres 的 `Datum` 格式。在大数据量（亿级行）扫描时，这种“行转行”的转换耗时往往超过了查询本身。
- **下推（Pushdown）局限性**：虽然可以下推 WHERE 子句，但对于复杂的 JOIN、Window Function 或特定的聚合，FDW 难以将执行计划完美映射到 DuckDB。

## 2.3 Table Access Method (TAM)：更底层的尝试
Postgres 12 引入的 TAM 允许开发者定义新的存储格式（如列存）。虽然 Hydra 等项目利用了 TAM，但它仍然受限于 Postgres 的执行引擎。这意味着即使存储是列存，执行时可能还是被拉回到行式执行器。

## 2.4 执行计划劫持 (Hook Mechanism)
这是 `pg_duckdb` 采取的路径。它不通过 FDW 接口，而是通过 Hook 拦截查询计划。如果判定查询适合 DuckDB，则将整个子查询树转换成 DuckDB 的查询任务。这种方式绕过了 FDW 的序列化开销，实现了真正的性能飞跃。

---

# 第三章：DuckDB 的核心竞争力

## 3.1 向量化执行引擎：OLAP 的银弹
DuckDB 的核心是其向量化执行（Vectorized Execution）。它每次处理一批数据（通常是 1024 行），这使得：
- CPU 缓存利用率大幅提升。
- 可以利用 SIMD 指令进行并行计算。
- 大幅减少了函数调用的次数。

## 3.2 嵌入式哲学的胜利
DuckDB 不需要独立进程，它与 Postgres 共享同一个进程空间。这意味着：
- **零网络开销**：没有 TCP/IP 握手和数据包传输。
- **内存共享潜力**：虽然目前实现尚不完美，但理论上可以直接读取 Postgres 的缓冲池。

## 3.3 对云原生格式的极致优化
DuckDB 对 Parquet、Iceberg、JSON 的原生支持，配合其高效的 HTTP Range 请求能力，使其成为查询 S3 等对象存储的最佳选择。这种“即插即用”的云端数据查询能力，是 Postgres 传统架构最难以企及的。
