## 1. OpenSpec and claim alignment

- [x] 1.1 Write proposal, design, tasks, and spec deltas for the first production-safety slice
- [x] 1.2 Update README/TODO language so current read-path claims match the code-backed chunk-result implementation

## 2. Security and correctness hardening

- [x] 2.1 Revoke default `PUBLIC` execute privileges for `duckdb_execute` and `duckdb_create_s3_secret` in install and upgrade SQL
- [x] 2.2 Replace remaining unsafe interpolation in `duckdb_deparse_analyze()` and derived S3 Tables endpoint construction
- [x] 2.3 Harden DuckDB result cleanup across helper error paths and other immediately adjacent leak sites

## 3. Regression coverage

- [x] 3.1 Add regression checks proving unprivileged roles do not inherit execute rights to admin-like helper functions
- [x] 3.2 Record remaining high-risk but not-yet-verified follow-up items for the next hardening phase
