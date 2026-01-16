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
