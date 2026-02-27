## 1. Parameter propagation and execution binding

- [ ] 1.1 Ensure `GetForeignPlan` populates `fdw_exprs` from collected params for all pushdown relation types
- [ ] 1.2 Implement runtime param extraction/evaluation utilities in execution state
- [ ] 1.3 Implement prepared query execution with bound parameters

## 2. Relation-type integration

- [ ] 2.1 Integrate parameter binding for base relation scans
- [ ] 2.2 Integrate parameter binding for join and upper relation pushdown scans
- [ ] 2.3 Verify rescan path refreshes parameter values correctly

## 3. Remote cost estimation

- [ ] 3.1 Implement `use_remote_estimate` path in size/cost estimation functions
- [ ] 3.2 Add safe fallback behavior and diagnostics on estimate errors/timeouts
- [ ] 3.3 Add estimation caching per planning cycle where appropriate

## 4. Regression tests

- [ ] 4.1 Add prepared statement tests for parameterized filters and joins
- [ ] 4.2 Add planner tests validating estimate-path impact when option is toggled
- [ ] 4.3 Add fallback tests for estimation failure paths

## 5. Documentation

- [ ] 5.1 Document parameterized query support boundaries and known limits
- [ ] 5.2 Document `use_remote_estimate` behavior and operational guidance
