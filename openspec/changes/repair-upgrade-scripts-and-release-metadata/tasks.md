## 1. Migration audit and repair

- [ ] 1.1 Audit all `duckdb_fdw--*.sql` files for signature/return-type transitions
- [ ] 1.2 Replace fragile `CREATE OR REPLACE` transitions with safe drop/create patterns where required
- [ ] 1.3 Validate full migration chain ordering and completeness

## 2. Metadata normalization

- [ ] 2.1 Align version values across control and META files
- [ ] 2.2 Align declared license metadata with repository license
- [ ] 2.3 Update changelog/release notes to reflect corrected metadata

## 3. Validation automation

- [ ] 3.1 Add CI script to check metadata consistency and migration chain integrity
- [ ] 3.2 Add upgrade smoke tests for representative historical version hops
- [ ] 3.3 Add post-upgrade object definition assertions

## 4. Documentation

- [ ] 4.1 Document supported upgrade paths and unsupported legacy hops
- [ ] 4.2 Document migration troubleshooting and rollback guidance
