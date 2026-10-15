-- 19 Text vs Array bracket round-trip regression (bug 4, PR #75)
--
-- A DuckDB VARCHAR whose value merely starts with '[' and ends with ']'
-- must be returned UNCHANGED through the FDW. Only a real array-typed
-- column gets the [..] -> {..} rewrite. Before the #75 fix,
-- duckdb_value_to_pg() rewrote the brackets for *every* target type
-- including text, so this text value came back as '{not an array}'.
--
-- The DO block self-verifies both directions and RAISEs on any mismatch;
-- this suite runs with ON_ERROR_STOP=1, so a regression fails CI.

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

DROP SERVER IF EXISTS tb_srv CASCADE;
CREATE SERVER tb_srv FOREIGN DATA WRAPPER duckdb_fdw
  OPTIONS (database '/tmp/duckdb_fdw_text_bracket.db');

-- id, a real DuckDB list, and a text value that LOOKS like a list
SELECT duckdb_execute('tb_srv', 'CREATE OR REPLACE TABLE tb (id INTEGER, a INTEGER[], s VARCHAR)');
SELECT duckdb_execute('tb_srv', 'INSERT INTO tb VALUES (1, [1,2,3], ''[not an array]''), (2, [7], ''[Description: No-op correction applied to line 42]'')');

CREATE FOREIGN TABLE tb_ft (
    id INT4,
    a INT4[],
    s TEXT
) SERVER tb_srv OPTIONS (table 'tb');

DO $$
DECLARE
    v_bracket1 text;
    v_bracket2 text;
    v_array    int4[];
BEGIN
    SELECT s INTO v_bracket1 FROM tb_ft WHERE id = 1;
    IF v_bracket1 IS DISTINCT FROM '[not an array]' THEN
        RAISE EXCEPTION 'BUG (text rewritten): id=1 expected [not an array], got: %', v_bracket1;
    END IF;

    SELECT s INTO v_bracket2 FROM tb_ft WHERE id = 2;
    IF v_bracket2 IS DISTINCT FROM '[Description: No-op correction applied to line 42]' THEN
        RAISE EXCEPTION 'BUG (text rewritten): id=2 expected [Description: ...], got: %', v_bracket2;
    END IF;

    -- the real list column must still convert to a PG array (guard must not break it)
    SELECT a INTO v_array FROM tb_ft WHERE id = 1;
    IF v_array IS DISTINCT FROM ARRAY[1,2,3]::int4[] THEN
        RAISE EXCEPTION 'BUG (array broken): id=1 expected {1,2,3}, got: %', v_array;
    END IF;

    RAISE NOTICE 'text-bracket round-trip OK: text unchanged, array intact';
END $$;

DROP FOREIGN TABLE tb_ft;
DROP SERVER tb_srv CASCADE;
