## MODIFIED Requirements

### Requirement: Public feature claims SHALL be evidence-backed
Major feature claims in project documentation MUST include references to implementation and/or regression tests, and MUST distinguish between implemented chunk-result scanning and the still-unimplemented full Arrow C Data path.

#### Scenario: Claim review
- **WHEN** documentation states a high-impact capability
- **THEN** readers can find linked code/test evidence supporting the claim and can see whether the claim refers to the current chunk-result path or a future Arrow path

### Requirement: Documentation SHALL disclose known limitations
For features that are partial or pending, documentation MUST explicitly list known limits and current constraints.

#### Scenario: Partial feature disclosure
- **WHEN** a feature is incomplete
- **THEN** README, TODO, or release notes identify unsupported cases and expected behavior boundaries instead of marking the capability as complete
