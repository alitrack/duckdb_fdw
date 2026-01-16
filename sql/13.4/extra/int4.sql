--
-- INT4
--
--Testcase 61:
CREATE EXTENSION duckdb_fdw;
--Testcase 62:
CREATE SERVER duckdb_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test_core.db');
--Testcase 63:
CREATE FOREIGN TABLE INT4_TBL(f1 int4 OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 64:
CREATE FOREIGN TABLE INT4_TMP(f1 int4, f2 int4, id int OPTIONS (key 'true')) SERVER duckdb_svr; 
 
--Testcase 1:
INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

--Testcase 2:
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

--Testcase 3:
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

--Testcase 4:
INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
--Testcase 5:
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

--Testcase 6:
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
--Testcase 7:
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
--Testcase 8:
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
--Testcase 9:
INSERT INTO INT4_TBL(f1) VALUES ('     ');
--Testcase 10:
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
--Testcase 11:
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
--Testcase 12:
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
--Testcase 13:
INSERT INTO INT4_TBL(f1) VALUES ('');


--Testcase 14:
SELECT '' AS five, * FROM INT4_TBL;

--Testcase 15:
SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> int2 '0';

--Testcase 16:
SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> int4 '0';

--Testcase 17:
SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = int2 '0';

--Testcase 18:
SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = int4 '0';

--Testcase 19:
SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < int2 '0';

--Testcase 20:
SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < int4 '0';

--Testcase 21:
SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= int2 '0';

--Testcase 22:
SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= int4 '0';

--Testcase 23:
SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > int2 '0';

--Testcase 24:
SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > int4 '0';

--Testcase 25:
SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= int2 '0';

--Testcase 26:
SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
--Testcase 27:
SELECT '' AS one, i.* FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
--Testcase 28:
SELECT '' AS three, i.* FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

--Testcase 29:
SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;

--Testcase 30:
SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

--Testcase 31:
SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;

--Testcase 32:
SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

--Testcase 33:
SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;

--Testcase 34:
SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

--Testcase 35:
SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;

--Testcase 36:
SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

--Testcase 37:
SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;

--Testcase 38:
SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

--Testcase 39:
SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;

--Testcase 40:
SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

--Testcase 41:
SELECT '' AS five, i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

--Testcase 42:
SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing

--Testcase 65:
DELETE FROM INT4_TMP;
--Testcase 66:
INSERT INTO INT4_TMP VALUES (-2, 3);
--Testcase 67:
SELECT f1 + f2 as one FROM INT4_TMP;

--Testcase 68:
DELETE FROM INT4_TMP;
--Testcase 69:
INSERT INTO INT4_TMP VALUES (4, 2);
--Testcase 70:
SELECT f1 - f2 as two FROM INT4_TMP;

--Testcase 46:
DELETE FROM INT4_TMP;
--Testcase 71:
INSERT INTO INT4_TMP VALUES (2, 1);
--Testcase 72:
SELECT f1- -f2 as three FROM INT4_TMP;

--Testcase 47:
DELETE FROM INT4_TMP;
--Testcase 73:
INSERT INTO INT4_TMP VALUES (2, 2);
--Testcase 74:
SELECT f1 - -f2 as four FROM INT4_TMP;

--Testcase 75:
DELETE FROM INT4_TMP;
--Testcase 76:
INSERT INTO INT4_TMP VALUES ('2'::int2 * '2'::int2, '16'::int2 / '4'::int2);
--Testcase 77:
SELECT f1 = f2 AS true FROM INT4_TMP;

--Testcase 78:
DELETE FROM INT4_TMP;
--Testcase 79:
INSERT INTO INT4_TMP VALUES ('2'::int2 * '2'::int4, '16'::int2 / '4'::int4);
--Testcase 80:
SELECT f1 = f2 AS true FROM INT4_TMP;

--Testcase 81:
DELETE FROM INT4_TMP;
--Testcase 82:
INSERT INTO INT4_TMP VALUES ('2'::int4 * '2'::int2, '16'::int4 / '4'::int2);
--Testcase 83:
SELECT f1 = f2 AS true FROM INT4_TMP;

--Testcase 84:
DELETE FROM INT4_TMP;
--Testcase 85:
INSERT INTO INT4_TMP VALUES ('1000'::int4, '999'::int4);
--Testcase 86:
SELECT f1 < f2 AS false FROM INT4_TMP;

--Testcase 48:
DELETE FROM INT4_TMP;
--Testcase 87:
INSERT INTO INT4_TMP VALUES (4!);
--Testcase 88:
SELECT f1 as twenty_four FROM INT4_TMP;

--Testcase 49:
DELETE FROM INT4_TMP;
--Testcase 89:
INSERT INTO INT4_TMP VALUES (!!3);
--Testcase 90:
SELECT f1 as six FROM INT4_TMP;

--Testcase 50:
DELETE FROM INT4_TMP;
--Testcase 91:
INSERT INTO INT4_TMP VALUES (1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1);
--Testcase 92:
SELECT f1 as ten FROM INT4_TMP;

--Testcase 51:
DELETE FROM INT4_TMP;
--Testcase 93:
INSERT INTO INT4_TMP VALUES (2 , 2);
--Testcase 94:
SELECT f1 + f1/f2 as three FROM INT4_TMP;

--Testcase 52:
DELETE FROM INT4_TMP;
--Testcase 95:
INSERT INTO INT4_TMP VALUES (2 , 2);
--Testcase 96:
SELECT (f1 + f2)/f2 as two FROM INT4_TMP;

-- corner case
--Testcase 54:
DELETE FROM INT4_TMP;
--Testcase 97:
INSERT INTO INT4_TMP VALUES (-1);
--Testcase 98:
SELECT (f1<<31)::text FROM INT4_TMP;

--Testcase 56:
DELETE FROM INT4_TMP;
--Testcase 99:
INSERT INTO INT4_TMP VALUES (-1);
--Testcase 100:
SELECT ((f1<<31)+1)::text FROM INT4_TMP;

-- check sane handling of INT_MIN overflow cases
--Testcase 58:
DELETE FROM INT4_TMP;
--Testcase 101:
INSERT INTO INT4_TMP VALUES ((-2147483648)::int4, (-1)::int4);
--Testcase 102:
SELECT f1 * f2 FROM INT4_TMP;
--Testcase 103:
SELECT f1 / f2 FROM INT4_TMP;
--Testcase 104:
SELECT f1 % f2 FROM INT4_TMP;

--Testcase 60:
DELETE FROM INT4_TMP;
--Testcase 105:
INSERT INTO INT4_TMP VALUES ((-2147483648)::int4, (-1)::int2);
--Testcase 106:
SELECT f1 * f2 FROM INT4_TMP;
--Testcase 107:
SELECT f1 / f2 FROM INT4_TMP;
--Testcase 108:
SELECT f1 % f2 FROM INT4_TMP;

-- check rounding when casting from float
--Testcase 109:
CREATE FOREIGN TABLE FLOAT8_TMP(f1 float8, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 110:
DELETE FROM FLOAT8_TMP;
--Testcase 111:
INSERT INTO FLOAT8_TMP VALUES 
             (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8);

--Testcase 112:
SELECT f1 as x, f1::int4 as int4_value FROM FLOAT8_TMP;

-- check rounding when casting from numeric
--Testcase 113:
CREATE FOREIGN TABLE NUMERIC_TMP(f1 numeric, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 114:
DELETE FROM NUMERIC_TMP;
--Testcase 115:
INSERT INTO NUMERIC_TMP VALUES
             (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric);
--Testcase 116:
SELECT f1 as x, f1::int4 as int4_value FROM NUMERIC_TMP;

-- test gcd()
--Testcase 117:
DELETE FROM INT4_TMP;
--Testcase 118:
INSERT INTO INT4_TMP VALUES
             (0::int4, 0::int4),
             (0::int4, 6410818::int4),
             (61866666::int4, 6410818::int4),
             (-61866666::int4, 6410818::int4),
             ((-2147483648)::int4, 1::int4),
             ((-2147483648)::int4, 2147483647::int4),
             ((-2147483648)::int4, 1073741824::int4);
--Testcase 119:
SELECT f1, f2, gcd(f1, f2), gcd(f1, -f2), gcd(f2, f1), gcd(-f2, f1) FROM INT4_TMP;

--Testcase 120:
DELETE FROM INT4_TMP;
--Testcase 121:
INSERT INTO INT4_TMP VALUES ((-2147483648)::int4, 0::int4);
--Testcase 122:
SELECT gcd(f1, f2) FROM INT4_TMP; -- overflow

--Testcase 123:
DELETE FROM INT4_TMP;
--Testcase 124:
INSERT INTO INT4_TMP VALUES ((-2147483648)::int4, (-2147483648)::int4);
--Testcase 125:
SELECT gcd(f1, f2) FROM INT4_TMP; -- overflow

-- test lcm()
--Testcase 126:
DELETE FROM INT4_TMP;
--Testcase 127:
INSERT INTO INT4_TMP VALUES
             (0::int4, 0::int4),
             (0::int4, 42::int4),
             (42::int4, 42::int4),
             (330::int4, 462::int4),
             (-330::int4, 462::int4),
             ((-2147483648)::int4, 0::int4);
--Testcase 128:
SELECT f1, f2, lcm(f1, f2), lcm(f1, -f2), lcm(f2, f1), lcm(-f2, f1) FROM INT4_TMP;

--Testcase 129:
DELETE FROM INT4_TMP;
--Testcase 130:
INSERT INTO INT4_TMP VALUES ((-2147483648)::int4, 1::int4);
--Testcase 131:
SELECT lcm(f1, f2) FROM INT4_TMP; -- overflow

--Testcase 132:
DELETE FROM INT4_TMP;
--Testcase 133:
INSERT INTO INT4_TMP VALUES (2147483647::int4, 2147483646::int4);
--Testcase 134:
SELECT lcm(f1, f2) FROM INT4_TMP; -- overflow

-- Clean up
DO $d$
declare
  l_rec record;
begin
  for l_rec in (select foreign_table_schema, foreign_table_name 
                from information_schema.foreign_tables) loop
     execute format('drop foreign table %I.%I cascade;', l_rec.foreign_table_schema, l_rec.foreign_table_name);
  end loop;
end;
$d$;

--Testcase 135:
DROP SERVER duckdb_svr;
--Testcase 136:
DROP EXTENSION duckdb_fdw CASCADE;
