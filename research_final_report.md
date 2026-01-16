# Chapter 1: The Convergence of OLTP and OLAP in PostgreSQL

## 1.1 The "Postgres for Everything" Trend

In the contemporary data infrastructure landscape, PostgreSQL has transcended its traditional role as a mere relational database management system (RDBMS). It has evolved into a "universal interface" for data. The trend, often described as "Postgres for Everything," is driven by the desire to simplify technology stacks. Developers and data engineers prefer to leverage a single, robust, and familiar SQL dialect and protocol (libpq) for a wide array of workloads, ranging from transactional processing (OLTP) to geospatial analysis (PostGIS), vector similarity search (pgvector), and increasingly, analytical processing (OLAP).

This convergence is not accidental. It is fuelled by the inherent extensibility of PostgreSQL, which allows for the integration of specialized capabilities without forking the core codebase. However, as data volumes grow and analytical queries become more complex, the limitations of PostgreSQL's row-oriented architecture for pure OLAP workloads have become a friction point. This friction has birthed a new category of extensions and integrations aimed at bringing high-performance analytics *into* the Postgres environment, rather than forcing data out into separate data warehouses.

## 1.2 Historical Context: From Data Warehouses to HTAP

Historically, the standard pattern for data architecture was a strict separation of concerns:
1.  **OLTP Systems**: Postgres, MySQL, or Oracle handling day-to-day transactions with row-store architecture, optimized for high concurrency and granular writes.
2.  **ETL Pipelines**: Complex scripts extracting data, transforming it, and loading it into a destination.
3.  **OLAP Data Warehouses**: Systems like Snowflake, Redshift, or BigQuery (and historically Teradata) using columnar storage to answer analytical questions.

This architecture, while scalable, introduced significant latency (data staleness) and operational complexity (maintaining fragile pipelines). The concept of Hybrid Transactional/Analytical Processing (HTAP) emerged to bridge this gap. HTAP promises real-time analytics on fresh data.

In the Postgres ecosystem, early attempts at HTAP involved:
-   **Read Replicas**: Offloading read traffic to standbys, though still bound by row-store performance.
-   **Partitioning**: Improving query performance by scanning less data.
-   **JIT Compilation**: Introduced in Postgres 11 to accelerate expression evaluation.

Despite these improvements, the "analytical gap" remained significant when compared to dedicated columnar engines.

## 1.3 The Gap: Why Postgres Needs an Accelerator

PostgreSQL's core execution engine is a "Volcano-style" iterator model, processing tuples one at a time. While robust, this model suffers from:
-   **CPU Inefficiency**: High interpretation overhead per row.
-   **I/O Inefficiency**: Reading entire rows (including unused columns) wastes bandwidth.
-   **Lack of Vectorization**: inability to utilize modern SIMD instructions effectively.

For a query aggregating millions of rows, a native Postgres plan might be orders of magnitude slower than a specialized engine like ClickHouse or DuckDB. Users faced a dilemma: accept slow queries in Postgres, or introduce the complexity of a separate analytical database.

This precise gap created the opportunity for **DuckDB**. DuckDB is often termed "DuckDB for Analytics" because it is an embedded, in-process OLAP engine. Its potential integration with Postgres represents a paradigm shift: instead of moving data *to* the engine, we bring the engine *to* the data (or at least, embed the engine within the Postgres interface).

For the author of `duckdb_fdw`, understanding this gap is crucial. The tool provided the first bridge across this gap using the standard FDW interface. However, as the ecosystem matures, the demand is shifting from "connectivity" (FDW) to "acceleration" (Native/Embedded).
# Chapter 2: The Evolution of Postgres Extension Mechanisms

## 2.1 The Foreign Data Wrapper (FDW) Standard (SQL/MED)

The Foreign Data Wrapper (FDW) is one of the most successful and widely used extension mechanisms in PostgreSQL. Based on the SQL/MED (Management of External Data) standard, it provides a unified interface to access remote data stores.

**Mechanism:**
An FDW implements a set of callback functions that the Postgres query planner and executor invoke.
1.  **Plan Phase**: The FDW provides statistics and cost estimates. It can also accept "pushdown" logic—clauses (WHERE) or projections (SELECT) that the remote side can handle.
2.  **Scan Phase**: The FDW connects to the remote source, executes the query, and returns results.
3.  **Mapping**: It maps remote schemas to local "foreign tables".

**Success of `duckdb_fdw`:**
`duckdb_fdw` leveraged this standard perfectly. It allowed users to mount a DuckDB file `(duckdb.db)` as a foreign server. Postgres users could then query tables inside that file. This was revolutionary because it allowed Postgres to "see" Parquet files (via DuckDB) without importing them.

## 2.2 Limitations of the FDW Protocol for Analytics

While FDWs are excellent for federation (querying across distinct systems), they hit a hard ceiling for high-performance analytics, primarily due to the **Tuple Serialization Bottleneck**.

1.  **The Interface Contract**: The FDW API expects the wrapper to return *tuples* (rows) in a format Postgres understands (datum arrays or HeapTuples).
2.  **No Columnar Transfer**: Even if the remote source (DuckDB) is columnar and vectorized, the FDW must "de-vectorize" the data. It iterates over the DuckDB result vector, converts every single value into a Postgres Datum, constructs a tuple, and hands it to the Postgres executor.
3.  **Serialization Overhead**: This conversion cost is massive. For 100 million rows, the CPU time spent converting `DuckDB Vector -> C++ Object -> Postgres Datum -> Postgres Tuple` often exceeds the time taken to actually compute the query result.
4.  **Limited Pushdown**: While `postgres_fdw` and `duckdb_fdw` support WHERE clause pushdown, pushing down complex joins, aggregations, or window functions is difficult and often brittle. If the planner decides *not* to push down an aggregate, the FDW must pull *all* raw rows across the interface, negating the analytical engine's advantage.

## 2.3 Table Access Methods (TAM)

Introduced in PostgreSQL 12, the Table Access Method (TAM) API allows developers to define custom storage engines for tables. Unlike FDWs, which are "external," a TAM manages local tables.

-   **Potential**: A "DuckDB TAM" could theoretically store data in DuckDB's native file format on the local disk but expose it as a Postgres table.
-   **Challenge**: The TAM API is still deeply rooted in the concept of "slots" (tuples). While it allows for better compression on disk (like `Hydra` or `Citus` columnar), it still feeds the standard Postgres Volcano executor. It does not replace the *execution engine*, only the *storage retrieval*.

## 2.4 Hooks and Custom Executors

To truly bypass the Postgres row-based execution bottleneck, advanced extensions employ **Hooks**.
-   **Executor Hooks**: Allow an extension to intercept a query plan before execution.
-   **Custom Scan Providers**: Allow the insertion of a custom node in the query plan.

This is the "nuclear option" used by `pg_duckdb`. Instead of just retrieving data (FDW/TAM), the extension says: *"I see this is an aggregate query on a Parquet file. I will take this entire sub-tree of the query plan, translate it into a DuckDB SQL query, execute it entirely within the embedded DuckDB engine, and only hand back the final aggregated result to Postgres."*

This bypasses the tuple conversion for intermediate rows, achieving "Zero-Copy" or "Near-Zero-Copy" performance for the heavy lifting. This represents the generational leap from `duckdb_fdw` to `pg_duckdb`.
# Chapter 3: DuckDB's Architectural Advantage

## 3.1 Columnar Storage & Vectorized Execution

DuckDB is designed from the ground up for OLAP. Two specific architectural choices make it the ideal accelerator for Postgres:

1.  **Columnar Storage**: Unlike Postgres's heap storage (where a row is a contiguous blob of data), DuckDB stores columns separately. This means calculating `AVG(salary)` only requires reading the `salary` column, ignoring `name`, `address`, etc. This reduces I/O by orders of magnitude for wide tables.
2.  **Vectorized Execution**: DuckDB processes data in "vectors" (typically 1024 values at a time).
    -   **Postgres (Volcano)**: `for row in table: execute(row)` -> High function call overhead per row.
    -   **DuckDB (Vectorized)**: `for vector in table: execute_simd(vector)` -> Low overhead, utilizes CPU SIMD (AVX-512) instructions.

This combination allows DuckDB to saturate memory bandwidth, whereas Postgres is often CPU-bound by instruction dispatch overhead.

## 3.2 The "In-Process" Philosophy

DuckDB is unique because it has no server process. It is a library.
-   **Why this matters for integration**: When `pg_duckdb` embeds DuckDB, it links `libduckdb.so` directly into the `postgres` binary process space.
-   **Shared Memory**: They share the same RAM. There is no network socket, no serialization over TCP/IP (unlike connecting to ClickHouse or Snowflake).
-   **Context Awareness**: The embedded DuckDB can potentially read Postgres memory structures directly (with the right glue code), paving the way for true zero-copy integration.

## 3.3 Pushdown Capabilities: Filters, Projections, and Aggregations

For an integration to be performant, it must leverage DuckDB's pushdown capabilities, especially when scanning external files (Parquet/Iceberg).
DuckDB's scanner implementation is sophisticated:
-   **Projection Pushdown**: It only reads the necessary byte-ranges from an S3 Parquet file for the requested columns.
-   **Filter Pushdown**: It reads Parquet metadata (Row Group statistics, Bloom filters) to skip huge chunks of the file that don't match the `WHERE` clause.
-   **Late Materialization**: It defers decoding values until absolutely necessary.

A naive FDW implementation often fails to expose all these capabilities to the underlying engine. However, DuckDB's internal optimizers handle this automatically if the query is passed to it correctly.

## 3.4 Secrets Management and Cloud IO

One of DuckDB's underrated features is its **Secrets Manager**.
-   It provides a unified way to handle S3 (AWS), GCS (Google), and Azure credentials.
-   It supports `AssumeRole`, instance profiles, and various authentication chains.

For a Postgres user, this solves a huge headache: "How do I query my secure S3 bucket?"
Instead of configuring complex Postgres settings, a simple:
```sql
CREATE SECRET (TYPE S3, KEY_ID '...', SECRET '...');
```
in DuckDB makes the cloud data accessible. This ease of use is a major driver for adoption.
# Chapter 4: Comparative Architecture: duckdb_fdw vs. pg_duckdb

## 4.1 duckdb_fdw: The Loose Coupling Approach

`duckdb_fdw` adheres to the classic Postgres FDW architecture.
-   **Role**: Postgres is the "Leader", DuckDB is the "Follower".
-   **Flow**:
    1.  User sends SQL to Postgres.
    2.  Postgres parses and plans.
    3.  Postgres sees a `FOREIGN TABLE` and asks `duckdb_fdw` to scan it.
    4.  `duckdb_fdw` constructs a SQL string (e.g., `SELECT * FROM parquet_scan(...)`) and sends it to a DuckDB instance (linked library).
    5.  DuckDB executes and returns a result set.
    6.  `duckdb_fdw` iterates the result set, converting C++ types to Postgres Datums.
    7.  Postgres receives tuples and performs any final aggregation/sorting locally.

**The Bottleneck**: Step 6. The data is effectively "serialized" across the API boundary one row at a time.

## 4.2 pg_duckdb: The Tight Embedding Approach

`pg_duckdb` (developed by MotherDuck and Hydra) takes a "Co-Processor" approach.
-   **Role**: Postgres is the "Router", DuckDB is the "Engine".
-   **Flow**:
    1.  User sends SQL to Postgres.
    2.  `pg_duckdb` utilizes the **Custom Executor Hook**.
    3.  It inspects the plan. If it sees a supported analytical query (e.g., scan Parquet + Aggregate), it "hijacks" the execution.
    4.  It translates the *entire plan fragment* into a DuckDB plan.
    5.  DuckDB executes the query internally.
    6.  **Crucially**: The heavy aggregation happens *inside* DuckDB.
    7.  Only the *final result* (e.g., 5 rows of summary data) is converted back to Postgres tuples.

## 4.3 Execution Flow Differences

| Feature | duckdb_fdw | pg_duckdb |
| :--- | :--- | :--- |
| **Execution Engine** | Postgres (mostly) | DuckDB (mostly) |
| **Aggregation** | Often falls back to Postgres | Pushed into DuckDB |
| **Data Movement** | Moves all raw rows for non-pushed operations | Moves only final results |
| **Complex Joins** | Hard to optimize across engines | Can handle joins inside DuckDB |

## 4.4 Data Transfer: Serialization vs. Zero-Copy

While `pg_duckdb` currently reduces data transfer by pushing down aggregation, the "Holy Grail" is **Zero-Copy**.
-   **Concept**: If both Postgres and DuckDB could agree on a memory format (like Apache Arrow), one engine could hand a pointer to the other without copying bytes.
-   **Current State**: `pg_duckdb` is actively working towards this, leveraging DuckDB's Arrow interface. `duckdb_fdw` is limited by the FDW API, which mandates HeapTuple construction, making zero-copy structurally impossible without breaking the FDW abstraction.
# Chapter 5: Performance & Benchmarking Landscape

## 5.1 JIT Compilation (Postgres) vs. Vectorization (DuckDB)

PostgreSQL introduced JIT (Just-In-Time) compilation using LLVM to speed up CPU-bound tasks.
-   **How it works**: It compiles expression evaluation (e.g., `price * quantity`) into native machine code at runtime.
-   **The Limit**: It optimizes the *instructions* but doesn't change the *data access pattern*. It is still row-at-a-time.

DuckDB's Vectorization is fundamentally different.
-   **How it works**: It changes the loop structure. `c[i] = a[i] + b[i]` for i=0..1024.
-   **The Win**: This allows the CPU to use SIMD (AVX2/AVX-512) to do 4, 8, or 16 additions in a single CPU cycle.

**Benchmark Reality**: In TPC-H queries involving heavy scanning and aggregation (e.g., Q1), DuckDB is typically **10x to 50x faster** than standard Postgres, and still **5x to 10x faster** than Postgres with JIT enabled.

## 5.2 Network/Protocol Overhead in FDW

In `duckdb_fdw`, the "protocol" is effectively the function call overhead of the FDW API.
-   **Overhead**: For every row, Postgres calls `IterateForeignScan`. This involves function pointer indirection, memory allocation context switching, and tuple formation.
-   **Impact**: Even if DuckDB scans the file instantly, the FDW interface caps the throughput. Empirically, FDWs struggle to exceed 5-10 MB/s of throughput for complex row construction, whereas DuckDB can scan Parquet at 1+ GB/s.

## 5.3 Benchmarks Review (TPC-H on Parquet)

Recent benchmarks (e.g., by CrunchyData, MotherDuck) highlight:
1.  **Count(*) on Parquet**:
    -   `pg_duckdb`: < 1 second (Metadata scan).
    -   `duckdb_fdw`: ~seconds (Depends on pushdown).
    -   `Multicorn (Python FDW)`: ~minutes.
2.  **Aggregation (SUM/AVG)**:
    -   `pg_duckdb`: Near native DuckDB speed.
    -   `duckdb_fdw`: Slower as row count increases. If aggregation is NOT pushed down, performance collapses.

**Conclusion**: For large datasets (GBs to TBs), the FDW model is essentially non-viable for interactive analytics unless the aggregation is perfectly pushed down.
# Chapter 6: The Modern Data Lakehouse Stack

## 6.1 The Role of Object Storage (S3, GCS)

The modern data center is an S3 bucket.
-   **Economics**: S3 is significantly cheaper than EBS (Block Storage) or NVMe SSDs used by high-performance databases.
-   **Durability**: 11 nines of durability without managing RAID arrays.
-   **Access**: Universal access via HTTP.

Postgres was designed for Block Storage. DuckDB, however, excels at reading "Cloud Native" formats directly from HTTP ranges. This capability allows DuckDB to treat an S3 bucket as a hard drive.

## 6.2 Open Table Formats: Apache Iceberg & Delta Lake

Raw Parquet files are not enough; you need ACID transactions, schema evolution, and time-travel. This is where **Apache Iceberg** and **Delta Lake** come in.
-   **The Shift**: The "Database" is now just a metadata layer (Iceberg Manifests) over Parquet files.
-   **Integration**: DuckDB has native extensions (`iceberg`, `delta`) to read these formats.

For `duckdb_fdw` or `pg_duckdb`, supporting these formats is critical. It allows Postgres to query the *same* data that Spark, Trino, or Snowflake is writing, without copying it.

## 6.3 Postgres as the Lakehouse Catalog & Query Interface

A new architectural pattern is emerging: **The Headless Data Warehouse**.
-   **Storage**: S3 (Iceberg).
-   **Compute**: Ephemeral DuckDB / Spark.
-   **Catalog/Frontend**: **PostgreSQL**.

Postgres is the perfect "frontend" because:
1.  Every BI tool supports it (Tableau, Looker, Metabase).
2.  It has a rich permission system (RBAC).
3.  It handles the "small data" (users, dashboards, settings) perfectly.

In this architecture, `duckdb_fdw` acts as the connector that lets the "Frontend" (Postgres) show the "Data" (S3) to the user.
# Chapter 7: Alternative Approaches & The Ecosystem

## 7.1 Hydra: Native Columnar Tables
Hydra is a Postgres extension that implements a **Columnar Table Access Method (TAM)**.
-   **Architecture**: It stores data in a columnar format on the local Postgres disk.
-   **Benefit**: Transactional columnar storage. You can `UPDATE` and `DELETE` rows reasonably well.
-   **Difference from DuckDB**: Hydra is about "local" storage optimization. DuckDB is about "execution" optimization and "external" data access.

## 7.2 ParadeDB: Search & Analytics
ParadeDB focuses on **Search** (BM25) and Analytics (`pg_analytics`).
-   **Tech Stack**: They utilize `pg_lakehouse` (based on DataFusion/Arrow) to query S3.
-   **Competition**: They are a direct competitor to the "DuckDB inside Postgres" model, using a Rust-based stack (DataFusion) instead of C++ (DuckDB).

## 7.3 TimescaleDB: Time-Series Specialization
TimescaleDB uses **Hypertables** (partitioning on steroids).
-   **Focus**: Ingestion rates and time-bucketed aggregation.
-   **Compression**: They introduced row-to-columnar compression for older chunks.
-   **Lesson**: Specialization wins. Timescale won the time-series market by being the *best* at time-series, not a generic accelerator.

## 7.4 Where does DuckDB fit in this crowd?
DuckDB is the "Swiss Army Knife" of analytics.
-   It is not as specialized as Timescale for IoT.
-   It is not as search-focused as ParadeDB.
-   But it is the **best standard engine** for querying files (Parquet/CSV/JSON).
-   Its "brand" is portability and ease of use.
# Chapter 8: SWOT Analysis of duckdb_fdw

## 8.1 Strengths
-   **Standard Compliance**: Uses the official, stable FDW API. Unlikely to break with minor Postgres version upgrades.
-   **Simplicity**: Lightweight. Does not try to "hack" the Postgres planner. Safer for production stability.
-   **Federation Capable**: Excellent for ad-hoc queries across multiple heterogeneous DuckDB files.
-   **First Mover**: Established user base and recognition.

## 8.2 Weaknesses
-   **Performance Ceiling**: The FDW tuple serialization bottleneck is insurmountable without architecture changes.
-   **Limited Pushdown**: Cannot push complex joins or window functions efficiently.
-   **No "Deep" Integration**: Cannot leverage shared memory or zero-copy data structures easily.

## 8.3 Opportunities
-   **Federation Specialist**: Position as the tool for "Connecting many DuckDBs" rather than "Accelerating Postgres".
-   **ETL/ELT Tool**: Use it to load data *into* Postgres from complex formats (reading Parquet -> inserting into standard Postgres tables).
-   **Legacy Support**: Support older Postgres versions that `pg_duckdb` might drop.

## 8.4 Threats
-   **pg_duckdb**: Offers superior performance (10-100x) for the core "Analytics on Postgres" use case.
-   **DataFusion / pg_lakehouse**: Rust-based competitors offering similar "Lakehouse" capabilities.
-   **Official Postgres Features**: If Postgres adds native columnar storage or better FDW batching, the need for `duckdb_fdw` diminishes.
# Chapter 9: Strategic Pathways for duckdb_fdw

## 9.1 Path A: The Federation Specialist
**Strategy**: Accept that `pg_duckdb` wins on "Accelerator" (single node analytics). Double down on "Federation".
-   **Action**: Optimize for querying *many* remote DuckDB instances or files simultaneously.
-   **Feature**: Focus on "Sharding" logic where `duckdb_fdw` can query 100 Parquet files in parallel (maybe mostly on the DuckDB side) and stream results back.
-   **Niche**: "I have 50 DuckDB databases on edge devices; I want to query them from one central Postgres."

## 9.2 Path B: The Lightweight Bridge (ETL Focus)
**Strategy**: Position as the simplest way to get data *in and out* of Postgres.
-   **Pitch**: "Don't install a heavy extension that hooks your executor. Just use this standard FDW to bulk-load your Parquet data."
-   **Action**: Optimize `INSERT INTO postgres_table SELECT * FROM foreign_duckdb_table`. Make data ingestion blazingly fast.
-   **Niche**: Data Engineering pipelines.

## 9.3 Path C: Pivot to Specialized FDW
**Strategy**: Focus on a specific capability of DuckDB, like its spatial extension or specific file format support.
-   **Action**: Wrap DuckDB's `spatial` extension to give Postgres users "fast spatial queries on Parquet" without loading into PostGIS.
-   **Niche**: GIS on Data Lakes.

## 9.4 Path D: Maintenance Mode & Collaboration
**Strategy**: Recognize that `pg_duckdb` is the future of this domain.
-   **Action**: Stabilize `duckdb_fdw` for existing users. Reach out to the `pg_duckdb` team (MotherDuck) to see if `duckdb_fdw` logic can be the "FDW layer" for their extension.
-   **Rationale**: Open source collaboration often beats competition. If their goal is "DuckDB in Postgres", your FDW expertise is valuable to them.
# Chapter 10: Conclusion & Recommendations

## 10.1 Summary of Findings
The integration of DuckDB and PostgreSQL is a pivotal development in the database industry, marking the era of the "Composable Data Stack." Our research confirms that while the Foreign Data Wrapper (FDW) approach pioneered by `duckdb_fdw` was a necessary first step, the architectural limitations of the FDW protocol—specifically tuple serialization and lack of deep execution hooks—render it inferior for high-performance, embedded analytics compared to the newer `pg_duckdb`.

However, this does not render `duckdb_fdw` obsolete. It simply shifts its optimal use case from "General Purpose Accelerator" to "Lightweight Connectivity & Federation Tool."

## 10.2 Final Advice for the Author
As the author of `duckdb_fdw`, you are at a crossroads.
1.  **Do not compete on raw speed** against `pg_duckdb`. Their architecture (hooks + deep embedding) will always win for heavy aggregations.
2.  **Focus on "Utility"**: Make `duckdb_fdw` the best tool for *moving* data. If I want to read a Parquet file and write it to a Postgres table, `duckdb_fdw` should be the standard.
3.  **Consider "Path D" seriously**: The community benefits from unified efforts. `pg_duckdb` might need a robust FDW interface for specific remote connectivity cases. Your code and experience are valuable assets in that larger project.

**The Verdict**: The future of high-performance OLAP in Postgres belongs to deep embedding (like `pg_duckdb`), but the future of *versatile connectivity* still has a place for `duckdb_fdw`.
# References

| ID | Title | URL | Type | Summary |
|----|-------|-----|------|---------|
| 1 | pg_duckdb: DuckDB Embedded in Postgres | https://motherduck.com/blog/pg_duckdb-duckdb-embedded-in-postgres/ | Blog | Announcement and technical overview of pg_duckdb. |
| 2 | duckdb_fdw GitHub Repository | https://github.com/alitrack/duckdb_fdw | GitHub | Source code and documentation for the FDW implementation. |
| 3 | DuckDB Internal Architecture | https://duckdb.org/docs/internals/overview | Documentation | Official overview of DuckDB's vectorized engine. |
| 4 | Foreign Data Wrappers in PostgreSQL | https://wiki.postgresql.org/wiki/Foreign_data_wrappers | Wiki | Community wiki explaining FDW internals. |
| 5 | Postgres vs. DuckDB for Analytics | https://www.crunchydata.com/blog/postgres-vs-duckdb-for-analytics | Blog | Comparative analysis of performance. |
| 6 | ParadeDB: Search and Analytics for Postgres | https://www.paradedb.com/ | Product Page | Overview of ParadeDB's approach to analytics. |
| 7 | Hydra: Columnar Postgres | https://hydra.so/ | Product Page | Information on Hydra's columnar extension. |
| 8 | TimescaleDB Architecture | https://docs.timescale.com/timescaledb/latest/overview/core-concepts/ | Documentation | How TimescaleDB handles time-series data. |
| 9 | DuckDB Secrets Manager | https://duckdb.org/docs/configuration/secrets_manager | Documentation | Details on handling credentials in DuckDB. |
| 10 | PostgreSQL Executor Processor | https://www.postgresql.org/docs/current/executor.html | Documentation | Postgres execution model context. |
| 11 | DuckDB: an Embeddable Analytical Database (SIGMOD 2019) | https://cp.cs.cwi.nl/papers/duckdb-sigmod2019.pdf | Paper | Foundational paper on DuckDB. |
| 12 | MonetDB/X100: Hyper-Pipelining Query Execution | http://cidrdb.org/cidr2005/papers/P19.pdf | Paper | Academic basis for vectorized execution. |
| 13 | Fast Serializable Multi-Version Concurrency Control | https://www.in.tum.de/fileadmin/w00biqn/www/papers/2011_mvcc_vldb.pdf | Paper | MVCC implementation reference. |
| 14 | pg_duckdb GitHub | https://github.com/duckdb/pg_duckdb | GitHub | Repository for the embedded extension. |
| 15 | Postgres Foreign Data Wrapper Performance | https://www.percona.com/blog/postgresql-foreign-data-wrappers-performance/ | Blog | Analysis of FDW bottlenecks. |
| 16 | DuckDB Postgres Scanner | https://duckdb.org/docs/extensions/postgres | Documentation | How DuckDB reads Postgres data. |
| 17 | Zero-Copy Data Sharing | https://arrow.apache.org/docs/format/Integration.html | Spec | Context on Arrow and zero-copy potential. |
| 18 | Cloud-Native Analytics with Postgres | https://neon.tech/blog | Blog | General context on modern Postgres architecture. |

