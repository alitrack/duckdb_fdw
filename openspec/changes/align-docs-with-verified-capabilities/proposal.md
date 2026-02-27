## Why

Current documentation markets capabilities that are not fully verifiable in current runtime/test paths. This creates expectation mismatch for users and increases support burden.

## What Changes

- Rewrite README claims to reflect verified implementation status.
- Add a capability status matrix (`implemented`, `partial`, `planned`) with links to code/tests.
- Label examples by prerequisites (`offline`, `network`, `credentials`) and stability level.
- Add release notes section for known limitations and pending items.
- Add doc QA checks to prevent unsupported claims in future updates.

## Capabilities

### New Capabilities
- `verifiable-feature-documentation`: Feature claims are tied to current validated behavior.
- `capability-status-matrix`: Documentation clearly communicates feature maturity and prerequisites.

### Modified Capabilities
- None.

## Impact

- Affected docs: `README.md`, `CHANGELOG`, `examples/*`, release notes.
- User expectations become aligned with shipped behavior.
- Lower risk of adoption churn due to mismatch between claims and runtime reality.
