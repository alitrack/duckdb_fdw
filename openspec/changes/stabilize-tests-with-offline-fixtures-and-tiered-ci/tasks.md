## 1. Test profile architecture

- [x] 1.1 Define profile taxonomy (`core`, `integration`, `cloud`) and expected gating behavior
- [x] 1.2 Refactor `run_tests.sh` to select and run profiles explicitly
- [x] 1.3 Keep default profile as deterministic offline core

## 2. Offline fixture coverage

- [x] 2.1 Add/curate local fixtures for baseline query, type, pushdown, and DML paths
- [ ] 2.2 Replace required tests that currently depend on external URLs or credentials
- [x] 2.3 Ensure expected outputs are deterministic and version-tolerant where needed

## 3. CI restructuring

- [x] 3.1 Update GitHub workflow to run required core matrix on every PR/push
- [x] 3.2 Add optional integration/cloud jobs with explicit triggers and credential guards
- [x] 3.3 Mark required vs optional jobs clearly in workflow and docs

## 4. Validation and documentation

- [x] 4.1 Add smoke checks proving core profile works in offline mode
- [x] 4.2 Document profile usage for local development and CI
- [ ] 4.3 Add troubleshooting section for tier-specific failures
