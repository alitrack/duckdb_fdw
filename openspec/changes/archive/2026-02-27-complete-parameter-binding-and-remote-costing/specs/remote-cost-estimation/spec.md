## ADDED Requirements

### Requirement: `use_remote_estimate` SHALL influence costing when enabled
If `use_remote_estimate=true`, FDW MUST attempt remote estimation and use results to populate row and cost fields.

#### Scenario: Remote estimate enabled
- **WHEN** server option `use_remote_estimate` is enabled
- **THEN** planner row/cost estimates differ from static fallback values based on remote estimate results

### Requirement: Cost estimation SHALL fallback safely on errors
If remote estimation fails, FDW MUST fallback to deterministic local defaults without failing the query plan.

#### Scenario: Remote estimate call fails
- **WHEN** remote estimation returns an error
- **THEN** planner continues with fallback cost values and query planning does not error
