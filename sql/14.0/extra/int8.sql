--
-- INT8
-- Test int8 64-bit integers.
--
--Testcase 140:
CREATE EXTENSION duckdb_fdw;
--Testcase 141:
CREATE SERVER duckdb_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test_core.db');
--Testcase 142:
CREATE FOREIGN TABLE INT8_TBL(
	q1 int8 OPTIONS (key 'true'),
	q2 int8 OPTIONS (key 'true')
) SERVER duckdb_svr;
--Testcase 143:
CREATE FOREIGN TABLE INT8_TMP(
	q1 int8,
	q2 int8,
	q3 int4, 
	q4 int2,
	q5 text,
	id int options (key 'true')
) SERVER duckdb_svr;

--Testcase 1:
INSERT INTO INT8_TBL VALUES('  123   ','  456');
--Testcase 2:
INSERT INTO INT8_TBL VALUES('123   ','4567890123456789');
--Testcase 3:
INSERT INTO INT8_TBL VALUES('4567890123456789','123');
--Testcase 4:
INSERT INTO INT8_TBL VALUES(+4567890123456789,'4567890123456789');
--Testcase 5:
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');

-- bad inputs
--Testcase 6:
INSERT INTO INT8_TBL(q1) VALUES ('      ');
--Testcase 7:
INSERT INTO INT8_TBL(q1) VALUES ('xxx');
--Testcase 8:
INSERT INTO INT8_TBL(q1) VALUES ('3908203590239580293850293850329485');
--Testcase 9:
INSERT INTO INT8_TBL(q1) VALUES ('-1204982019841029840928340329840934');
--Testcase 10:
INSERT INTO INT8_TBL(q1) VALUES ('- 123');
--Testcase 11:
INSERT INTO INT8_TBL(q1) VALUES ('  345     5');
--Testcase 12:
INSERT INTO INT8_TBL(q1) VALUES ('');

--Testcase 13:
SELECT * FROM INT8_TBL;

-- int8/int8 cmp
--Testcase 14:
SELECT * FROM INT8_TBL WHERE q2 = 4567890123456789;
--Testcase 15:
SELECT * FROM INT8_TBL WHERE q2 <> 4567890123456789;
--Testcase 16:
SELECT * FROM INT8_TBL WHERE q2 < 4567890123456789;
--Testcase 17:
SELECT * FROM INT8_TBL WHERE q2 > 4567890123456789;
--Testcase 18:
SELECT * FROM INT8_TBL WHERE q2 <= 4567890123456789;
--Testcase 19:
SELECT * FROM INT8_TBL WHERE q2 >= 4567890123456789;

-- int8/int4 cmp
--Testcase 20:
SELECT * FROM INT8_TBL WHERE q2 = 456;
--Testcase 21:
SELECT * FROM INT8_TBL WHERE q2 <> 456;
--Testcase 22:
SELECT * FROM INT8_TBL WHERE q2 < 456;
--Testcase 23:
SELECT * FROM INT8_TBL WHERE q2 > 456;
--Testcase 24:
SELECT * FROM INT8_TBL WHERE q2 <= 456;
--Testcase 25:
SELECT * FROM INT8_TBL WHERE q2 >= 456;

-- int4/int8 cmp
--Testcase 26:
SELECT * FROM INT8_TBL WHERE 123 = q1;
--Testcase 27:
SELECT * FROM INT8_TBL WHERE 123 <> q1;
--Testcase 28:
SELECT * FROM INT8_TBL WHERE 123 < q1;
--Testcase 29:
SELECT * FROM INT8_TBL WHERE 123 > q1;
--Testcase 30:
SELECT * FROM INT8_TBL WHERE 123 <= q1;
--Testcase 31:
SELECT * FROM INT8_TBL WHERE 123 >= q1;

-- int8/int2 cmp
--Testcase 32:
SELECT * FROM INT8_TBL WHERE q2 = '456'::int2;
--Testcase 33:
SELECT * FROM INT8_TBL WHERE q2 <> '456'::int2;
--Testcase 34:
SELECT * FROM INT8_TBL WHERE q2 < '456'::int2;
--Testcase 35:
SELECT * FROM INT8_TBL WHERE q2 > '456'::int2;
--Testcase 36:
SELECT * FROM INT8_TBL WHERE q2 <= '456'::int2;
--Testcase 37:
SELECT * FROM INT8_TBL WHERE q2 >= '456'::int2;

-- int2/int8 cmp
--Testcase 38:
SELECT * FROM INT8_TBL WHERE '123'::int2 = q1;
--Testcase 39:
SELECT * FROM INT8_TBL WHERE '123'::int2 <> q1;
--Testcase 40:
SELECT * FROM INT8_TBL WHERE '123'::int2 < q1;
--Testcase 41:
SELECT * FROM INT8_TBL WHERE '123'::int2 > q1;
--Testcase 42:
SELECT * FROM INT8_TBL WHERE '123'::int2 <= q1;
--Testcase 43:
SELECT * FROM INT8_TBL WHERE '123'::int2 >= q1;


--Testcase 44:
SELECT q1 AS plus, -q1 AS minus FROM INT8_TBL;

--Testcase 45:
SELECT q1, q2, q1 + q2 AS plus FROM INT8_TBL;
--Testcase 46:
SELECT q1, q2, q1 - q2 AS minus FROM INT8_TBL;
--Testcase 47:
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL;
--Testcase 48:
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL
 WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);
--Testcase 49:
SELECT q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;

--Testcase 50:
SELECT q1, float8(q1) FROM INT8_TBL;
--Testcase 51:
SELECT q2, float8(q2) FROM INT8_TBL;

--Testcase 52:
SELECT 37 + q1 AS plus4 FROM INT8_TBL;
--Testcase 53:
SELECT 37 - q1 AS minus4 FROM INT8_TBL;
--Testcase 54:
SELECT 2 * q1 AS "twice int4" FROM INT8_TBL;
--Testcase 55:
SELECT q1 * 2 AS "twice int4" FROM INT8_TBL;

-- int8 op int4
--Testcase 56:
SELECT q1 + 42::int4 AS "8plus4", q1 - 42::int4 AS "8minus4", q1 * 42::int4 AS "8mul4", q1 / 42::int4 AS "8div4" FROM INT8_TBL;
-- int4 op int8
--Testcase 57:
SELECT 246::int4 + q1 AS "4plus8", 246::int4 - q1 AS "4minus8", 246::int4 * q1 AS "4mul8", 246::int4 / q1 AS "4div8" FROM INT8_TBL;

-- int8 op int2
--Testcase 58:
SELECT q1 + 42::int2 AS "8plus2", q1 - 42::int2 AS "8minus2", q1 * 42::int2 AS "8mul2", q1 / 42::int2 AS "8div2" FROM INT8_TBL;
-- int2 op int8
--Testcase 59:
SELECT 246::int2 + q1 AS "2plus8", 246::int2 - q1 AS "2minus8", 246::int2 * q1 AS "2mul8", 246::int2 / q1 AS "2div8" FROM INT8_TBL;

--Testcase 60:
SELECT q2, abs(q2) FROM INT8_TBL;
--Testcase 61:
SELECT min(q1), min(q2) FROM INT8_TBL;
--Testcase 62:
SELECT max(q1), max(q2) FROM INT8_TBL;


-- TO_CHAR()
--
--Testcase 63:
SELECT to_char(q1, '9G999G999G999G999G999'), to_char(q2, '9,999,999,999,999,999')
	FROM INT8_TBL;

--Testcase 64:
SELECT to_char(q1, '9G999G999G999G999G999D999G999'), to_char(q2, '9,999,999,999,999,999.999,999')
	FROM INT8_TBL;

--Testcase 65:
SELECT to_char( (q1 * -1), '9999999999999999PR'), to_char( (q2 * -1), '9999999999999999.999PR')
	FROM INT8_TBL;

--Testcase 66:
SELECT to_char( (q1 * -1), '9999999999999999S'), to_char( (q2 * -1), 'S9999999999999999')
	FROM INT8_TBL;

--Testcase 67:
SELECT to_char(q2, 'MI9999999999999999')     FROM INT8_TBL;
--Testcase 68:
SELECT to_char(q2, 'FMS9999999999999999')    FROM INT8_TBL;
--Testcase 69:
SELECT to_char(q2, 'FM9999999999999999THPR') FROM INT8_TBL;
--Testcase 70:
SELECT to_char(q2, 'SG9999999999999999th')   FROM INT8_TBL;
--Testcase 71:
SELECT to_char(q2, '0999999999999999')       FROM INT8_TBL;
--Testcase 72:
SELECT to_char(q2, 'S0999999999999999')      FROM INT8_TBL;
--Testcase 73:
SELECT to_char(q2, 'FM0999999999999999')     FROM INT8_TBL;
--Testcase 74:
SELECT to_char(q2, 'FM9999999999999999.000') FROM INT8_TBL;
--Testcase 75:
SELECT to_char(q2, 'L9999999999999999.000')  FROM INT8_TBL;
--Testcase 76:
SELECT to_char(q2, 'FM9999999999999999.999') FROM INT8_TBL;
--Testcase 77:
SELECT to_char(q2, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9') FROM INT8_TBL;
--Testcase 78:
SELECT to_char(q2, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM INT8_TBL;
--Testcase 79:
SELECT to_char(q2, '999999SG9999999999')     FROM INT8_TBL;

-- check min/max values and overflow behavior
--Testcase 80:
DELETE FROM INT8_TMP;
--Testcase 144:
INSERT INTO INT8_TMP VALUES ('-9223372036854775808'::int8);
--Testcase 145:
SELECT q1 FROM INT8_TMP;

--Testcase 81:
DELETE FROM INT8_TMP;
--Testcase 146:
INSERT INTO INT8_TMP VALUES ('-9223372036854775809'::int8);
--Testcase 147:
SELECT q1 FROM INT8_TMP;

--Testcase 82:
DELETE FROM INT8_TMP;
--Testcase 148:
INSERT INTO INT8_TMP VALUES ('9223372036854775807'::int8);
--Testcase 149:
SELECT q1 FROM INT8_TMP;

--Testcase 83:
DELETE FROM INT8_TMP;
--Testcase 150:
INSERT INTO INT8_TMP VALUES ('9223372036854775808'::int8);
--Testcase 151:
SELECT q1 FROM INT8_TMP;

--Testcase 84:
DELETE FROM INT8_TMP;
--Testcase 152:
INSERT INTO INT8_TMP VALUES (-('-9223372036854775807'::int8));
--Testcase 153:
SELECT q1 FROM INT8_TMP;

--Testcase 86:
DELETE FROM INT8_TMP;
--Testcase 154:
INSERT INTO INT8_TMP VALUES (-('-9223372036854775808'::int8));
--Testcase 155:
SELECT q1 FROM INT8_TMP;

--Testcase 87:
DELETE FROM INT8_TMP;
--Testcase 156:
INSERT INTO INT8_TMP VALUES ('9223372036854775800'::int8 , '9223372036854775800'::int8);
--Testcase 157:
SELECT q1 + q2 FROM INT8_TMP;
--Testcase 88:
DELETE FROM INT8_TMP;
--Testcase 158:
INSERT INTO INT8_TMP VALUES ('-9223372036854775800'::int8 , '-9223372036854775800'::int8);
--Testcase 159:
SELECT q1 + q2 FROM INT8_TMP;

--Testcase 89:
DELETE FROM INT8_TMP;
--Testcase 160:
INSERT INTO INT8_TMP VALUES ('9223372036854775800'::int8 , '-9223372036854775800'::int8);
--Testcase 161:
SELECT q1-q2 FROM INT8_TMP;
--Testcase 90:
DELETE FROM INT8_TMP;
--Testcase 162:
INSERT INTO INT8_TMP VALUES ('-9223372036854775800'::int8 , '9223372036854775800'::int8);
--Testcase 163:
SELECT q1 - q2 FROM INT8_TMP;

--Testcase 91:
DELETE FROM INT8_TMP;
--Testcase 164:
INSERT INTO INT8_TMP VALUES ('9223372036854775800'::int8 , '9223372036854775800'::int8);
--Testcase 165:
SELECT q1 * q2 FROM INT8_TMP;

--Testcase 92:
DELETE FROM INT8_TMP;
--Testcase 166:
INSERT INTO INT8_TMP VALUES ('9223372036854775800'::int8 , '0'::int8);
--Testcase 167:
SELECT q1 / q2 FROM INT8_TMP;

--Testcase 93:
DELETE FROM INT8_TMP;
--Testcase 168:
INSERT INTO INT8_TMP VALUES ('9223372036854775800'::int8 , '0'::int8);
--Testcase 169:
SELECT q1 % q2 FROM INT8_TMP;

--Testcase 94:
DELETE FROM INT8_TMP;
--Testcase 170:
INSERT INTO INT8_TMP VALUES ('-9223372036854775808'::int8);
--Testcase 171:
SELECT abs(q1) FROM INT8_TMP;

--Testcase 95:
DELETE FROM INT8_TMP;
--Testcase 172:
INSERT INTO INT8_TMP(q1, q3) VALUES ('9223372036854775800'::int8 , '100'::int4);
--Testcase 173:
SELECT q1 + q3 FROM INT8_TMP;
--Testcase 96:
DELETE FROM INT8_TMP;
--Testcase 174:
INSERT INTO INT8_TMP(q1, q3) VALUES ('-9223372036854775800'::int8 , '100'::int4);
--Testcase 175:
SELECT q1 - q3 FROM INT8_TMP;
--Testcase 97:
DELETE FROM INT8_TMP;
--Testcase 176:
INSERT INTO INT8_TMP(q1, q3) VALUES ('9223372036854775800'::int8 , '100'::int4);
--Testcase 177:
SELECT q1 * q3 FROM INT8_TMP;

--Testcase 98:
DELETE FROM INT8_TMP;
--Testcase 178:
INSERT INTO INT8_TMP(q3, q1) VALUES ('100'::int4 , '9223372036854775800'::int8);
--Testcase 179:
SELECT q3 + q1 FROM INT8_TMP;
--Testcase 99:
DELETE FROM INT8_TMP;
--Testcase 180:
INSERT INTO INT8_TMP(q3, q1) VALUES ('-100'::int4 , '9223372036854775800'::int8);
--Testcase 181:
SELECT q3 - q1 FROM INT8_TMP;
--Testcase 100:
DELETE FROM INT8_TMP;
--Testcase 182:
INSERT INTO INT8_TMP(q3, q1) VALUES ('100'::int4 , '9223372036854775800'::int8);
--Testcase 183:
SELECT q3 * q1 FROM INT8_TMP;

--Testcase 101:
DELETE FROM INT8_TMP;
--Testcase 184:
INSERT INTO INT8_TMP(q1, q4) VALUES ('9223372036854775800'::int8 , '100'::int2);
--Testcase 185:
SELECT q1 + q4 FROM INT8_TMP;
--Testcase 102:
DELETE FROM INT8_TMP;
--Testcase 186:
INSERT INTO INT8_TMP(q1, q4) VALUES ('-9223372036854775800'::int8 , '100'::int2);
--Testcase 187:
SELECT q1 - q4 FROM INT8_TMP;
--Testcase 103:
DELETE FROM INT8_TMP;
--Testcase 188:
INSERT INTO INT8_TMP VALUES ('9223372036854775800'::int8 , '100'::int2);
--Testcase 189:
SELECT q1 * q4 FROM INT8_TMP;
--Testcase 104:
DELETE FROM INT8_TMP;
--Testcase 190:
INSERT INTO INT8_TMP(q1, q4) VALUES ('-9223372036854775808'::int8 , '0'::int2);
--Testcase 191:
SELECT q1 / q4 FROM INT8_TMP;

--Testcase 105:
DELETE FROM INT8_TMP;
--Testcase 192:
INSERT INTO INT8_TMP(q4, q1) VALUES ('100'::int2 , '9223372036854775800'::int8);
--Testcase 193:
SELECT q4 + q1 FROM INT8_TMP;
--Testcase 106:
DELETE FROM INT8_TMP;
--Testcase 194:
INSERT INTO INT8_TMP(q4, q1) VALUES ('-100'::int2 , '9223372036854775800'::int8);
--Testcase 195:
SELECT q4 - q1 FROM INT8_TMP;
--Testcase 107:
DELETE FROM INT8_TMP;
--Testcase 196:
INSERT INTO INT8_TMP(q4, q1) VALUES ('100'::int2 , '9223372036854775800'::int8);
--Testcase 197:
SELECT q4 * q1 FROM INT8_TMP;
--Testcase 108:
DELETE FROM INT8_TMP;
--Testcase 198:
INSERT INTO INT8_TMP(q4, q1) VALUES ('100'::int2 , '0'::int8);
--Testcase 199:
SELECT q4 / q1 FROM INT8_TMP;

--Testcase 110:
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456;
--Testcase 111:
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 <> 456;

--Testcase 112:
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 = 456;
--Testcase 113:
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 <> 456;

--Testcase 200:
DELETE FROM INT8_TMP;
--Testcase 201:
INSERT INTO INT8_TMP(q5) VALUES ('42'), ('-37');
--Testcase 202:
SELECT CAST(q5::int2 as int8) FROM INT8_TMP;

--Testcase 114:
SELECT CAST(q1 AS float4), CAST(q2 AS float8) FROM INT8_TBL;

--Testcase 203:
DELETE FROM INT8_TMP;
--Testcase 204:
INSERT INTO INT8_TMP(q5) VALUES ('36854775807.0');
--Testcase 205:
SELECT CAST(q5::float4 AS int8) FROM INT8_TMP;

--Testcase 206:
DELETE FROM INT8_TMP;
--Testcase 207:
INSERT INTO INT8_TMP(q5) VALUES ('922337203685477580700.0');
--Testcase 208:
SELECT CAST(q5::float8 AS int8) FROM INT8_TMP;

--Testcase 115:
SELECT CAST(q1 AS oid) FROM INT8_TBL;
--Testcase 209:
SELECT oid::int8 FROM pg_class WHERE relname = 'pg_class';

-- bit operations

--Testcase 116:
SELECT q1, q2, q1 & q2 AS "and", q1 | q2 AS "or", q1 # q2 AS "xor", ~q1 AS "not" FROM INT8_TBL;
--Testcase 117:
SELECT q1, q1 << 2 AS "shl", q1 >> 3 AS "shr" FROM INT8_TBL;


-- generate_series

--Testcase 118:
DELETE FROM INT8_TMP;
--Testcase 210:
INSERT INTO INT8_TMP SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8) q1;
--Testcase 211:
SELECT q1 FROM INT8_TMP;

--Testcase 120:
DELETE FROM INT8_TMP;
--Testcase 212:
INSERT INTO INT8_TMP SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 0) q1; -- should error
--Testcase 213:
SELECT q1 FROM INT8_TMP;

--Testcase 122:
DELETE FROM INT8_TMP;
--Testcase 214:
INSERT INTO INT8_TMP SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 2) q1;
--Testcase 215:
SELECT q1 FROM INT8_TMP;

-- corner case
--Testcase 216:
DELETE FROM INT8_TMP;
--Testcase 217:
INSERT INTO INT8_TMP VALUES (-1::int8<<63);
--Testcase 218:
SELECT q1::text FROM INT8_TMP;

--Testcase 219:
DELETE FROM INT8_TMP;
--Testcase 220:
INSERT INTO INT8_TMP VALUES ((-1::int8<<63)+1);
--Testcase 221:
SELECT q1::text FROM INT8_TMP;

-- check sane handling of INT64_MIN overflow cases
--Testcase 125:
DELETE FROM INT8_TMP;
--Testcase 222:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 * (-1)::int8, 888);
--Testcase 126:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 / (-1)::int8, 888);
--Testcase 127:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 % (-1)::int8, 888);
--Testcase 128:
SELECT q1 FROM INT8_TMP WHERE q2 = 888;
--Testcase 129:
DELETE FROM INT8_TMP WHERE q2 = 888;
--Testcase 130:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 * (-1)::int4, 888);
--Testcase 131:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 / (-1)::int4, 888);
--Testcase 132:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 % (-1)::int4, 888);
--Testcase 133:
SELECT q1 FROM INT8_TMP WHERE q2 = 888;
--Testcase 134:
DELETE FROM INT8_TMP WHERE q2 = 888;
--Testcase 135:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 * (-1)::int2, 888);
--Testcase 136:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 / (-1)::int2, 888);
--Testcase 137:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8 % (-1)::int2, 888);
--Testcase 138:
SELECT q1 FROM INT8_TMP WHERE q2 = 888;
--Testcase 139:
DELETE FROM INT8_TMP WHERE q2 = 888;

-- check rounding when casting from float
--Testcase 223:
CREATE FOREIGN TABLE FLOAT8_TMP(f1 float8, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 224:
DELETE FROM FLOAT8_TMP;
--Testcase 225:
INSERT INTO FLOAT8_TMP VALUES 
             (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8);

--Testcase 226:
SELECT f1 as x, f1::int8 as int8_value FROM FLOAT8_TMP;

-- check rounding when casting from numeric
--Testcase 227:
CREATE FOREIGN TABLE NUMERIC_TMP(f1 numeric, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 228:
DELETE FROM NUMERIC_TMP;
--Testcase 229:
INSERT INTO NUMERIC_TMP VALUES
             (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric);
--Testcase 230:
SELECT f1 as x, f1::int8 as int8_value FROM NUMERIC_TMP;

-- test gcd()
--Testcase 231:
DELETE FROM INT8_TMP;
--Testcase 232:
INSERT INTO INT8_TMP VALUES
             (0::int8, 0::int8),
             (0::int8, 29893644334::int8),
             (288484263558::int8, 29893644334::int8),
             (-288484263558::int8, 29893644334::int8),
             ((-9223372036854775808)::int8, 1::int8),
             ((-9223372036854775808)::int8, 9223372036854775807::int8),
             ((-9223372036854775808)::int8, 4611686018427387904::int8);
--Testcase 233:
SELECT q1, q2, gcd(q1, q2), gcd(q1, -q2), gcd(q2, q1), gcd(-q2, q1) FROM INT8_TMP;

--Testcase 234:
DELETE FROM INT8_TMP;
--Testcase 235:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8, 0::int8);
--Testcase 236:
SELECT gcd(q1, q2) FROM INT8_TMP; -- overflow

--Testcase 237:
DELETE FROM INT8_TMP;
--Testcase 238:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8, (-9223372036854775808)::int8);
--Testcase 239:
SELECT gcd(q1, q2) FROM INT8_TMP; -- overflow

-- test lcm()
--Testcase 240:
DELETE FROM INT8_TMP;
--Testcase 241:
INSERT INTO INT8_TMP VALUES
             (0::int8, 0::int8),
             (0::int8, 29893644334::int8),
             (29893644334::int8, 29893644334::int8),
             (288484263558::int8, 29893644334::int8),
             (-288484263558::int8, 29893644334::int8),
             ((-9223372036854775808)::int8, 0::int8);
--Testcase 242:
SELECT q1, q2, lcm(q1, q2), lcm(q1, -q2), lcm(q2, q1), lcm(-q2, q1) FROM INT8_TMP;

--Testcase 243:
DELETE FROM INT8_TMP;
--Testcase 244:
INSERT INTO INT8_TMP VALUES ((-9223372036854775808)::int8, 1::int8);
--Testcase 245:
SELECT lcm(q1, q2) FROM INT8_TMP; -- overflow

--Testcase 246:
DELETE FROM INT8_TMP;
--Testcase 247:
INSERT INTO INT8_TMP VALUES ((9223372036854775807)::int8, (9223372036854775806)::int8);
--Testcase 248:
SELECT lcm(q1, q2) FROM INT8_TMP; -- overflow

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

--Testcase 249:
DROP SERVER duckdb_svr;
--Testcase 250:
DROP EXTENSION duckdb_fdw CASCADE;
