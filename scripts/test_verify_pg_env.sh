#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

FAKE_PATH_DIR="${TMP_DIR}/fake-path"
FAKE_PG_BIN="${TMP_DIR}/pg-bin"
FAKE_PGXS_DIR="${TMP_DIR}/pgxs"
FAKE_INCLUDE_DIR="${TMP_DIR}/include"

mkdir -p "${FAKE_PATH_DIR}" "${FAKE_PG_BIN}" "${FAKE_PGXS_DIR}" "${FAKE_INCLUDE_DIR}"
touch "${FAKE_PGXS_DIR}/pgxs.mk"
touch "${FAKE_INCLUDE_DIR}/postgres.h"

cat > "${FAKE_PATH_DIR}/pg_config" <<EOF
#!/usr/bin/env bash
case "\$1" in
  --version) echo "PostgreSQL 17.9 (test stub)" ;;
  --pgxs) echo "${FAKE_PGXS_DIR}/pgxs.mk" ;;
  --includedir-server) echo "${FAKE_INCLUDE_DIR}" ;;
  --bindir) echo "${FAKE_PG_BIN}" ;;
  *) exit 1 ;;
esac
EOF

cat > "${FAKE_PATH_DIR}/psql" <<'EOF'
#!/usr/bin/env bash
echo "psql (PostgreSQL) 17.9 (test stub)"
EOF

cat > "${FAKE_PG_BIN}/pg_ctl" <<'EOF'
#!/usr/bin/env bash
echo "pg_ctl stub"
EOF

cat > "${FAKE_PG_BIN}/initdb" <<'EOF'
#!/usr/bin/env bash
echo "initdb stub"
EOF

chmod +x "${FAKE_PATH_DIR}/pg_config" "${FAKE_PATH_DIR}/psql" "${FAKE_PG_BIN}/pg_ctl" "${FAKE_PG_BIN}/initdb"

OUTPUT="$(
  cd "${ROOT_DIR}" && \
  PATH="${FAKE_PATH_DIR}:/usr/bin:/bin" bash scripts/verify_pg_env.sh --pg-major 17
)"

echo "${OUTPUT}"

grep -F "[OK] 找到 pg_config:" <<< "${OUTPUT}" >/dev/null
grep -F "[OK] 找到 pg_ctl: ${FAKE_PG_BIN}/pg_ctl" <<< "${OUTPUT}" >/dev/null
grep -F "[OK] 找到 initdb: ${FAKE_PG_BIN}/initdb" <<< "${OUTPUT}" >/dev/null
grep -F "环境检查通过。" <<< "${OUTPUT}" >/dev/null
