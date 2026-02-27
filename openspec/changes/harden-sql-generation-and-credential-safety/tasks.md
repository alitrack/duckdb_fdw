## 1. Inventory and helper foundation

- [ ] 1.1 Enumerate all SQL string interpolation sites fed by user/server/table options
- [ ] 1.2 Implement shared escaping and token-validation helper APIs
- [ ] 1.3 Add focused helper tests for quoting and token validation edge cases

## 2. Refactor high-risk execution paths

- [ ] 2.1 Refactor secret creation code paths in `connection.c` and `duckdb_fdw.c` to use helpers
- [ ] 2.2 Refactor ATTACH/catalog and extension loading SQL generation to use helpers and token checks
- [ ] 2.3 Refactor `import.c` dynamic queries to safe literal/identifier handling
- [ ] 2.4 Refactor deparse table-name handling to avoid raw unsafe interpolation for option values

## 3. Error handling and redaction

- [ ] 3.1 Add central redaction utility for secret-bearing errors
- [ ] 3.2 Replace raw error propagation in secret paths with sanitized error surfaces

## 4. Security regression tests

- [ ] 4.1 Add malicious payload tests for table/alias/extension/secret inputs
- [ ] 4.2 Add log/error assertions that secrets never appear in output text

## 5. CI and release notes

- [ ] 5.1 Add static guard checks against unsafe interpolation patterns in changed files
- [ ] 5.2 Document stricter input validation and migration guidance in release notes
