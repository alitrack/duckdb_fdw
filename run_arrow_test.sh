#!/bin/bash
set -e
PORT=5434
HOST=/tmp
DB=postgres

echo "Compiling..."
make USE_PGXS=1 -s
sudo make install USE_PGXS=1 -s

if [ ! -d "test_db" ]; then
    echo "Initializing test_db..."
    /usr/lib/postgresql/15/bin/initdb -D test_db
fi

echo "Starting Postgres on $PORT..."
/usr/lib/postgresql/15/bin/pg_ctl -D test_db stop >/dev/null 2>&1 || true
/usr/lib/postgresql/15/bin/pg_ctl -D test_db -l logfile -o "-p $PORT -k $HOST" start
sleep 2

echo "Running Arrow Types Test..."
/usr/lib/postgresql/15/bin/psql -p $PORT -h $HOST -d $DB -f examples/14_arrow_vectorized_types.sql || true

echo "Running High Performance Insert Test..."
/usr/lib/postgresql/15/bin/psql -p $PORT -h $HOST -d $DB -f examples/15_high_performance_insert.sql || true

echo "Displaying Logfile (Errors):"
grep -iE "Error|Fatal|Panic|Seg" logfile || true

echo "Stopping Postgres..."
/usr/lib/postgresql/15/bin/pg_ctl -D test_db stop
