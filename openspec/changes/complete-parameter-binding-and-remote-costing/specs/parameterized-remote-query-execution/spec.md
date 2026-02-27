## ADDED Requirements

### Requirement: Pushdown queries with placeholders SHALL bind runtime parameters correctly
For queries deparsed with placeholders, FDW MUST carry parameter expressions through planning and bind runtime values during execution.

#### Scenario: Prepared statement with filter parameter
- **WHEN** a prepared query uses a parameterized WHERE predicate against a foreign table
- **THEN** FDW binds parameter values correctly and returns expected filtered rows

### Requirement: Parameter binding SHALL work for base and pushed-down join/upper plans
Parameterized execution MUST be consistent across base, join, and upper relation pushdown paths.

#### Scenario: Join pushdown with parameterized condition
- **WHEN** a pushed-down join query contains a runtime parameter
- **THEN** FDW executes the remote query with bound parameter value and preserves semantics
