## 1. OpenSpec and regression setup

- [x] 1.1 Write proposal, design, tasks, and spec deltas for runtime/planner hardening
- [x] 1.2 Add regression cases for server option refresh and explicit unsupported update/delete behavior

## 2. Runtime lifecycle hardening

- [x] 2.1 Add transaction-end cleanup for cached DuckDB connections
- [x] 2.2 Remove dead unregistered Direct Modify scaffolding while preserving explicit unsupported write behavior

## 3. Planner hardening

- [x] 3.1 Implement shared foreign path costing and use it for base relation paths
- [x] 3.2 Route join and upper foreign paths through the same cost helper
