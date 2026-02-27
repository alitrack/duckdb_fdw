## 1. Arrow scan infrastructure

- [x] 1.1 Add Arrow stream/chunk members to execution state and initialize lifecycle hooks
- [x] 1.2 Implement chunk fetch/advance helpers for Begin/Iterate/End scan lifecycle
- [x] 1.3 Implement typed Arrow-to-PostgreSQL conversion helpers with null handling

## 2. Replace legacy read path

- [x] 2.1 Wire `duckdbBeginForeignScan` to Arrow query startup and first chunk fetch
- [x] 2.2 Wire `duckdbIterateForeignScan` to chunk cursor iteration
- [x] 2.3 Keep controlled fallback path for unsupported type combinations

## 3. Appender and batch write path

- [x] 3.1 Initialize and teardown DuckDB Appender in modify lifecycle hooks
- [x] 3.2 Rework `ExecForeignInsert` to use typed appender calls
- [x] 3.3 Implement `GetForeignModifyBatchSize` and `ExecForeignBatchInsert` for PG14+

## 4. Correctness and performance validation

- [ ] 4.1 Add regression tests covering supported scalar types and null semantics in Arrow path
- [ ] 4.2 Add regression tests for Appender single-row and batch insert behavior
- [ ] 4.3 Add benchmark script comparing legacy vs new path for representative workloads

## 5. Rollout and safeguards

- [ ] 5.1 Add temporary option/flag for emergency disable of vectorized path
- [x] 5.2 Update documentation with explicit support matrix and rollout notes
