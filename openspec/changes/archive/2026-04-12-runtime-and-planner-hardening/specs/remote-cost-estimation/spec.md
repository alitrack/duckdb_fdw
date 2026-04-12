## MODIFIED Requirements

### Requirement: `use_remote_estimate` SHALL influence costing when enabled
If `use_remote_estimate=true`, FDW MUST attempt remote estimation and use results to populate row and cost fields for foreign path creation.

#### Scenario: Remote estimate enabled
- **WHEN** server option `use_remote_estimate` is enabled
- **THEN** planner row and foreign path cost estimates differ from the static fallback path and are derived from the remote estimate plus local width/cpu assumptions

### Requirement: Cost estimation SHALL fallback safely on errors
If remote estimation fails, FDW MUST fallback to deterministic local defaults without failing the query plan.

#### Scenario: Remote estimate call fails
- **WHEN** remote estimation returns an error
- **THEN** planner continues with fallback row/cost values and query planning does not error
