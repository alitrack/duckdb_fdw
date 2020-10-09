--
-- FLOAT8
--
--Testcase 113:
CREATE EXTENSION duckdb_fdw;
--Testcase 114:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');
--Testcase 115:
CREATE FOREIGN TABLE FLOAT8_TBL(f1 float8 OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 116:
CREATE FOREIGN TABLE FLOAT8_TMP(f1 float8, f2 float8, id int OPTIONS (key 'true')) SERVER sqlite_svr;

--Testcase 1:
INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
--Testcase 2:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
--Testcase 3:
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
--Testcase 4:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
--Testcase 5:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

-- test for underflow and overflow handling
--Testcase 6:
INSERT INTO FLOAT8_TMP(f1) VALUES ('10e400'::float8);
--Testcase 7:
INSERT INTO FLOAT8_TMP(f1) VALUES ('-10e400'::float8);
--Testcase 8:
INSERT INTO FLOAT8_TMP(f1) VALUES ('10e-400'::float8);
--Testcase 9:
INSERT INTO FLOAT8_TMP(f1) VALUES ('-10e-400'::float8);

-- test smallest normalized input
--Testcase 117:
INSERT INTO FLOAT8_TMP(f1) VALUES ('2.2250738585072014E-308'::float8);
--Testcase 118:
SELECT float8send(f1) FROM FLOAT8_TMP;

-- bad input
--Testcase 10:
INSERT INTO FLOAT8_TBL(f1) VALUES ('');
--Testcase 11:
INSERT INTO FLOAT8_TBL(f1) VALUES ('     ');
--Testcase 12:
INSERT INTO FLOAT8_TBL(f1) VALUES ('xyz');
--Testcase 13:
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.0.0');
--Testcase 14:
INSERT INTO FLOAT8_TBL(f1) VALUES ('5 . 0');
--Testcase 15:
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.   0');
--Testcase 16:
INSERT INTO FLOAT8_TBL(f1) VALUES ('    - 3');
--Testcase 17:
INSERT INTO FLOAT8_TBL(f1) VALUES ('123           5');

-- special inputs
--Testcase 19:
DELETE FROM FLOAT8_TMP;
--Testcase 119:
INSERT INTO FLOAT8_TMP VALUES ('NaN'::float8);
--Testcase 120:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 20:
DELETE FROM FLOAT8_TMP;
--Testcase 121:
INSERT INTO FLOAT8_TMP VALUES ('nan'::float8);
--Testcase 122:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 21:
DELETE FROM FLOAT8_TMP;
--Testcase 123:
INSERT INTO FLOAT8_TMP VALUES ('   NAN  '::float8);
--Testcase 124:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 22:
DELETE FROM FLOAT8_TMP;
--Testcase 125:
INSERT INTO FLOAT8_TMP VALUES ('infinity'::float8);
--Testcase 126:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 23:
DELETE FROM FLOAT8_TMP;
--Testcase 127:
INSERT INTO FLOAT8_TMP VALUES ('          -INFINiTY   '::float8);
--Testcase 128:
SELECT f1 FROM FLOAT8_TMP;

-- bad special inputs
--Testcase 25:
DELETE FROM FLOAT8_TMP;
--Testcase 129:
INSERT INTO FLOAT8_TMP VALUES ('N A N'::float8);
--Testcase 130:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 26:
DELETE FROM FLOAT8_TMP;
--Testcase 131:
INSERT INTO FLOAT8_TMP VALUES ('NaN x'::float8);
--Testcase 132:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 27:
DELETE FROM FLOAT8_TMP;
--Testcase 133:
INSERT INTO FLOAT8_TMP VALUES (' INFINITY    x'::float8);
--Testcase 134:
SELECT f1 FROM FLOAT8_TMP;

--Testcase 28:
DELETE FROM FLOAT8_TMP;
--Testcase 135:
INSERT INTO FLOAT8_TMP VALUES ('Infinity'::float8 + 100.0);
--Testcase 136:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 30:
DELETE FROM FLOAT8_TMP;
--Testcase 137:
INSERT INTO FLOAT8_TMP VALUES ('Infinity'::float8 / 'Infinity'::float8);
--Testcase 138:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 31:
DELETE FROM FLOAT8_TMP;
--Testcase 139:
INSERT INTO FLOAT8_TMP VALUES ('nan'::float8 / 'nan'::float8);
--Testcase 140:
SELECT f1 FROM FLOAT8_TMP;
--Testcase 32:
DELETE FROM FLOAT8_TMP;
--Testcase 141:
INSERT INTO FLOAT8_TMP VALUES ('nan'::numeric::float8);
--Testcase 142:
SELECT f1 FROM FLOAT8_TMP;

--Testcase 34:
SELECT '' AS five, * FROM FLOAT8_TBL;

--Testcase 35:
SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3';

--Testcase 36:
SELECT '' AS one, f.* FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';

--Testcase 37:
SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE '1004.3' > f.f1;

--Testcase 38:
SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE  f.f1 < '1004.3';

--Testcase 39:
SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE '1004.3' >= f.f1;

--Testcase 40:
SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE  f.f1 <= '1004.3';

--Testcase 41:
SELECT '' AS three, f.f1, f.f1 * '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 42:
SELECT '' AS three, f.f1, f.f1 + '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 43:
SELECT '' AS three, f.f1, f.f1 / '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 44:
SELECT '' AS three, f.f1, f.f1 - '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 45:
SELECT '' AS one, f.f1 ^ '2.0' AS square_f1
   FROM FLOAT8_TBL f where f.f1 = '1004.3';

-- absolute value
--Testcase 46:
SELECT '' AS five, f.f1, @f.f1 AS abs_f1
   FROM FLOAT8_TBL f;

-- truncate
--Testcase 47:
SELECT '' AS five, f.f1, trunc(f.f1) AS trunc_f1
   FROM FLOAT8_TBL f;

-- round
--Testcase 48:
SELECT '' AS five, f.f1, round(f.f1) AS round_f1
   FROM FLOAT8_TBL f;

-- ceil / ceiling
--Testcase 49:
select ceil(f1) as ceil_f1 from float8_tbl f;
--Testcase 50:
select ceiling(f1) as ceiling_f1 from float8_tbl f;

-- floor
--Testcase 51:
select floor(f1) as floor_f1 from float8_tbl f;

-- sign
--Testcase 52:
select sign(f1) as sign_f1 from float8_tbl f;

-- avoid bit-exact output here because operations may not be bit-exact.
SET extra_float_digits = 0;

-- square root
BEGIN;
--Testcase 53:
DELETE FROM FLOAT8_TBL;
--Testcase 54:
INSERT INTO FLOAT8_TBL VALUES ('64'::float8);
--Testcase 55:
SELECT sqrt(f1) as eight FROM FLOAT8_TBL;
--Testcase 56:
SELECT |/f1 as eight FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 57:
SELECT '' AS three, f.f1, |/f.f1 AS sqrt_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- power
--Testcase 58:
DELETE FROM FLOAT8_TMP;
--Testcase 143:
INSERT INTO FLOAT8_TMP VALUES ('144'::float8, '0.5'::float8);
--Testcase 144:
SELECT power(f1, f2) FROM FLOAT8_TMP;
--Testcase 60:
DELETE FROM FLOAT8_TMP;
--Testcase 145:
INSERT INTO FLOAT8_TMP VALUES ('NaN'::float8, '0.5'::float8);
--Testcase 146:
SELECT power(f1, f2) FROM FLOAT8_TMP;
--Testcase 61:
DELETE FROM FLOAT8_TMP;
--Testcase 147:
INSERT INTO FLOAT8_TMP VALUES ('144'::float8, 'NaN'::float8);
--Testcase 148:
SELECT power(f1, f2) FROM FLOAT8_TMP;
--Testcase 62:
DELETE FROM FLOAT8_TMP;
--Testcase 149:
INSERT INTO FLOAT8_TMP VALUES ('NaN'::float8, 'NaN'::float8);
--Testcase 150:
SELECT power(f1, f2) FROM FLOAT8_TMP;
--Testcase 63:
DELETE FROM FLOAT8_TMP;
--Testcase 151:
INSERT INTO FLOAT8_TMP VALUES ('-1'::float8, 'NaN'::float8);
--Testcase 152:
SELECT power(f1, f2) FROM FLOAT8_TMP;
--Testcase 64:
DELETE FROM FLOAT8_TMP;
--Testcase 153:
INSERT INTO FLOAT8_TMP VALUES ('1'::float8, 'NaN'::float8);
--Testcase 154:
SELECT power(f1, f2) FROM FLOAT8_TMP;
--Testcase 65:
DELETE FROM FLOAT8_TMP;
--Testcase 155:
INSERT INTO FLOAT8_TMP VALUES ('NaN'::float8 , '0'::float8);
--Testcase 156:
SELECT power(f1, f2) FROM FLOAT8_TMP;

-- take exp of ln(f.f1)
--Testcase 67:
SELECT '' AS three, f.f1, exp(ln(f.f1)) AS exp_ln_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- cube root
BEGIN;
--Testcase 68:
DELETE FROM FLOAT8_TBL;
--Testcase 69:
INSERT INTO FLOAT8_TBL VALUES ('27'::float8);
--Testcase 70:
SELECT ||/f1 as three FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 71:
SELECT '' AS five, f.f1, ||/f.f1 AS cbrt_f1 FROM FLOAT8_TBL f;


--Testcase 72:
SELECT '' AS five, * FROM FLOAT8_TBL;

--Testcase 73:
UPDATE FLOAT8_TBL
   SET f1 = FLOAT8_TBL.f1 * '-1'
   WHERE FLOAT8_TBL.f1 > '0.0';

--Testcase 74:
SELECT '' AS bad, f.f1 * '1e200' from FLOAT8_TBL f;

--Testcase 75:
SELECT '' AS bad, f.f1 ^ '1e200' from FLOAT8_TBL f;

BEGIN;
--Testcase 76:
DELETE FROM FLOAT8_TBL;
--Testcase 77:
INSERT INTO FLOAT8_TBL VALUES (0 ^ 0 + 0 ^ 1 + 0 ^ 0.0 + 0 ^ 0.5);
--Testcase 78:
SELECT * FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 79:
SELECT '' AS bad, ln(f.f1) from FLOAT8_TBL f where f.f1 = '0.0' ;

--Testcase 80:
SELECT '' AS bad, ln(f.f1) from FLOAT8_TBL f where f.f1 < '0.0' ;

--Testcase 81:
SELECT '' AS bad, exp(f.f1) from FLOAT8_TBL f;

--Testcase 82:
SELECT '' AS bad, f.f1 / '0.0' from FLOAT8_TBL f;

--Testcase 83:
SELECT '' AS five, * FROM FLOAT8_TBL;

-- hyperbolic functions
-- we run these with extra_float_digits = 0 too, since different platforms
-- tend to produce results that vary in the last place.
--Testcase 157:
DELETE FROM FLOAT8_TMP;
--Testcase 158:
INSERT INTO FLOAT8_TMP(f1) VALUES (1);
--Testcase 159:
SELECT sinh(f1) FROM FLOAT8_TMP;
--Testcase 160:
SELECT cosh(f1) FROM FLOAT8_TMP;
--Testcase 161:
SELECT tanh(f1) FROM FLOAT8_TMP;
--Testcase 162:
SELECT asinh(f1) FROM FLOAT8_TMP;

--Testcase 163:
DELETE FROM FLOAT8_TMP;
--Testcase 164:
INSERT INTO FLOAT8_TMP(f1) VALUES (2);
--Testcase 165:
SELECT acosh(f1) FROM FLOAT8_TMP;
--Testcase 166:
DELETE FROM FLOAT8_TMP;
--Testcase 167:
INSERT INTO FLOAT8_TMP(f1) VALUES (0.5);
--Testcase 168:
SELECT atanh(f1) FROM FLOAT8_TMP;

-- test Inf/NaN cases for hyperbolic functions
--Testcase 169:
DELETE FROM FLOAT8_TMP;
--Testcase 170:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'infinity');
--Testcase 171:
SELECT sinh(f1) FROM FLOAT8_TMP;

--Testcase 172:
DELETE FROM FLOAT8_TMP;
--Testcase 173:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 '-infinity');
--Testcase 174:
SELECT sinh(f1) FROM FLOAT8_TMP;

--Testcase 175:
DELETE FROM FLOAT8_TMP;
--Testcase 176:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'nan');
--Testcase 177:
SELECT sinh(f1) FROM FLOAT8_TMP;

--Testcase 178:
DELETE FROM FLOAT8_TMP;
--Testcase 179:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'infinity');
--Testcase 180:
SELECT cosh(f1) FROM FLOAT8_TMP;

--Testcase 181:
DELETE FROM FLOAT8_TMP;
--Testcase 182:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 '-infinity');
--Testcase 183:
SELECT cosh(f1) FROM FLOAT8_TMP;

--Testcase 184:
DELETE FROM FLOAT8_TMP;
--Testcase 185:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'nan');
--Testcase 186:
SELECT cosh(f1) FROM FLOAT8_TMP;

--Testcase 187:
DELETE FROM FLOAT8_TMP;
--Testcase 188:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'infinity');
--Testcase 189:
SELECT tanh(f1) FROM FLOAT8_TMP;

--Testcase 190:
DELETE FROM FLOAT8_TMP;
--Testcase 191:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 '-infinity');
--Testcase 192:
SELECT tanh(f1) FROM FLOAT8_TMP;

--Testcase 193:
DELETE FROM FLOAT8_TMP;
--Testcase 194:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'nan');
--Testcase 195:
SELECT tanh(f1) FROM FLOAT8_TMP;

--Testcase 196:
DELETE FROM FLOAT8_TMP;
--Testcase 197:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'infinity');
--Testcase 198:
SELECT asinh(f1) FROM FLOAT8_TMP;

--Testcase 199:
DELETE FROM FLOAT8_TMP;
--Testcase 200:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 '-infinity');
--Testcase 201:
SELECT asinh(f1) FROM FLOAT8_TMP;

--Testcase 202:
DELETE FROM FLOAT8_TMP;
--Testcase 203:
INSERT INTO FLOAT8_TMP(f1) VALUES (float8 'nan');
--Testcase 204:
SELECT asinh(f1) FROM FLOAT8_TMP;

-- acosh(Inf) should be Inf, but some mingw versions produce NaN, so skip test
-- SELECT acosh(float8 'infinity');
--Testcase 205:
DELETE FROM FLOAT8_TMP;
--Testcase 206:
INSERT INTO FLOAT8_TMP VALUES (float8 '-infinity');
--Testcase 207:
SELECT acosh(f1) FROM FLOAT8_TMP;

--Testcase 208:
DELETE FROM FLOAT8_TMP;
--Testcase 209:
INSERT INTO FLOAT8_TMP VALUES ((float8 'nan'));
--Testcase 210:
SELECT acosh(f1) FROM FLOAT8_TMP;

--Testcase 211:
DELETE FROM FLOAT8_TMP;
--Testcase 212:
INSERT INTO FLOAT8_TMP VALUES ((float8 'infinity'));
--Testcase 213:
SELECT atanh(f1) FROM FLOAT8_TMP;

--Testcase 214:
DELETE FROM FLOAT8_TMP;
--Testcase 215:
INSERT INTO FLOAT8_TMP VALUES ((float8 '-infinity'));
--Testcase 216:
SELECT atanh(f1) FROM FLOAT8_TMP;

--Testcase 217:
DELETE FROM FLOAT8_TMP;
--Testcase 218:
INSERT INTO FLOAT8_TMP VALUES ((float8 'nan'));
--Testcase 219:
SELECT atanh(f1) FROM FLOAT8_TMP;

RESET extra_float_digits;

-- test for over- and underflow
--Testcase 84:
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e400');

--Testcase 85:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e400');

--Testcase 86:
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e-400');

--Testcase 87:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e-400');

-- maintain external table consistency across platforms
-- delete all values and reinsert well-behaved ones

--Testcase 88:
DELETE FROM FLOAT8_TBL;

--Testcase 89:
INSERT INTO FLOAT8_TBL(f1) VALUES ('0.0');

--Testcase 90:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-34.84');

--Testcase 91:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-1004.30');

--Testcase 92:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e+200');

--Testcase 93:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e-200');

--Testcase 94:
SELECT '' AS five, * FROM FLOAT8_TBL;

-- test edge-case coercions to integer
--Testcase 220:
DELETE FROM FLOAT8_TMP;
--Testcase 221:
INSERT INTO FLOAT8_TMP VALUES ('32767.4'::float8);
--Testcase 222:
SELECT f1::int2 FROM FLOAT8_TMP;

--Testcase 223:
DELETE FROM FLOAT8_TMP;
--Testcase 224:
INSERT INTO FLOAT8_TMP VALUES ('32767.6'::float8);
--Testcase 225:
SELECT f1::int2 FROM FLOAT8_TMP;

--Testcase 226:
DELETE FROM FLOAT8_TMP;
--Testcase 227:
INSERT INTO FLOAT8_TMP VALUES ('-32768.4'::float8);
--Testcase 228:
SELECT f1::int2 FROM FLOAT8_TMP;

--Testcase 229:
DELETE FROM FLOAT8_TMP;
--Testcase 230:
INSERT INTO FLOAT8_TMP VALUES ('-32768.6'::float8);
--Testcase 231:
SELECT f1::int2 FROM FLOAT8_TMP;

--Testcase 232:
DELETE FROM FLOAT8_TMP;
--Testcase 233:
INSERT INTO FLOAT8_TMP VALUES ('2147483647.4'::float8);
--Testcase 234:
SELECT f1::int4 FROM FLOAT8_TMP;

--Testcase 235:
DELETE FROM FLOAT8_TMP;
--Testcase 236:
INSERT INTO FLOAT8_TMP VALUES ('2147483647.6'::float8);
--Testcase 237:
SELECT f1::int4 FROM FLOAT8_TMP;

--Testcase 238:
DELETE FROM FLOAT8_TMP;
--Testcase 239:
INSERT INTO FLOAT8_TMP VALUES ('-2147483648.4'::float8);
--Testcase 240:
SELECT f1::int4 FROM FLOAT8_TMP;

--Testcase 241:
DELETE FROM FLOAT8_TMP;
--Testcase 242:
INSERT INTO FLOAT8_TMP VALUES ('-2147483648.6'::float8);
--Testcase 243:
SELECT f1::int4 FROM FLOAT8_TMP;

--Testcase 244:
DELETE FROM FLOAT8_TMP;
--Testcase 245:
INSERT INTO FLOAT8_TMP VALUES ('9223372036854773760'::float8);
--Testcase 246:
SELECT f1::int8 FROM FLOAT8_TMP;

--Testcase 247:
DELETE FROM FLOAT8_TMP;
--Testcase 248:
INSERT INTO FLOAT8_TMP VALUES ('9223372036854775807'::float8);
--Testcase 249:
SELECT f1::int8 FROM FLOAT8_TMP;

--Testcase 250:
DELETE FROM FLOAT8_TMP;
--Testcase 251:
INSERT INTO FLOAT8_TMP VALUES ('-9223372036854775808.5'::float8);
--Testcase 252:
SELECT f1::int8 FROM FLOAT8_TMP;

--Testcase 253:
DELETE FROM FLOAT8_TMP;
--Testcase 254:
INSERT INTO FLOAT8_TMP VALUES ('-9223372036854780000'::float8);
--Testcase 255:
SELECT f1::int8 FROM FLOAT8_TMP;

-- test exact cases for trigonometric functions in degrees

BEGIN;
--Testcase 95:
DELETE FROM FLOAT8_TBL;
--Testcase 96:
INSERT INTO FLOAT8_TBL VALUES (0), (30), (90), (150), (180),
      (210), (270), (330), (360);
--Testcase 97:
SELECT f1,
       sind(f1),
       sind(f1) IN (-1,-0.5,0,0.5,1) AS sind_exact
       FROM FLOAT8_TBL;

--Testcase 98:
DELETE FROM FLOAT8_TBL;
--Testcase 99:
INSERT INTO FLOAT8_TBL VALUES (0), (60), (90), (120), (180),
      (240), (270), (300), (360);
--Testcase 100:
SELECT f1,
       cosd(f1),
       cosd(f1) IN (-1,-0.5,0,0.5,1) AS cosd_exact
       FROM FLOAT8_TBL;

--Testcase 101:
DELETE FROM FLOAT8_TBL;
--Testcase 102:
INSERT INTO FLOAT8_TBL VALUES (0), (45), (90), (135), (180),
      (225), (270), (315), (360);
--Testcase 103:
SELECT f1,
       tand(f1),
       tand(f1) IN ('-Infinity'::float8,-1,0,
                   1,'Infinity'::float8) AS tand_exact,
       cotd(f1),
       cotd(f1) IN ('-Infinity'::float8,-1,0,
                   1,'Infinity'::float8) AS cotd_exact
          FROM FLOAT8_TBL;

--Testcase 104:
DELETE FROM FLOAT8_TBL;
--Testcase 105:
INSERT INTO FLOAT8_TBL VALUES (-1), (-0.5), (0), (0.5), (1);
--Testcase 106:
SELECT f1,
       asind(f1),
       asind(f1) IN (-90,-30,0,30,90) AS asind_exact,
       acosd(f1),
       acosd(f1) IN (0,60,90,120,180) AS acosd_exact
          FROM FLOAT8_TBL;

--Testcase 107:
DELETE FROM FLOAT8_TBL;
--Testcase 108:
INSERT INTO FLOAT8_TBL VALUES ('-Infinity'::float8), (-1), (0), (1),
      ('Infinity'::float8);
--Testcase 109:
SELECT f1,
       atand(f1),
       atand(f1) IN (-90,-45,0,45,90) AS atand_exact
          FROM FLOAT8_TBL;

--Testcase 110:
DELETE FROM FLOAT8_TBL;
--Testcase 111:
INSERT INTO FLOAT8_TBL SELECT * FROM generate_series(0, 360, 90);
--Testcase 112:
SELECT x, y,
       atan2d(y, x),
       atan2d(y, x) IN (-90,0,90,180) AS atan2d_exact
FROM (SELECT 10*cosd(f1), 10*sind(f1)
          FROM FLOAT8_TBL) AS t(x,y);

ROLLBACK;

--
-- test output (and round-trip safety) of various values.
-- To ensure we're testing what we think we're testing, start with
-- float values specified by bit patterns (as a useful side effect,
-- this means we'll fail on non-IEEE platforms).

--Testcase 256:
create type xfloat8;
--Testcase 257:
create function xfloat8in(cstring) returns xfloat8 immutable strict
  language internal as 'int8in';
--Testcase 258:
create function xfloat8out(xfloat8) returns cstring immutable strict
  language internal as 'int8out';
--Testcase 259:
create type xfloat8 (input = xfloat8in, output = xfloat8out, like = float8);
--Testcase 260:
create cast (xfloat8 as float8) without function;
--Testcase 261:
create cast (float8 as xfloat8) without function;
--Testcase 262:
create cast (xfloat8 as bigint) without function;
--Testcase 263:
create cast (bigint as xfloat8) without function;

-- float8: seeeeeee eeeeeeee eeeeeeee mmmmmmmm mmmmmmmm(x4)

-- we don't care to assume the platform's strtod() handles subnormals
-- correctly; those are "use at your own risk". However we do test
-- subnormal outputs, since those are under our control.

--Testcase 264:
create foreign table testdata(bits text, id int OPTIONS (key 'true')) server sqlite_svr;
begin;
--Testcase 265:
insert into testdata(bits) values
  -- small subnormals
  (x'0000000000000001'),
  (x'0000000000000002'), (x'0000000000000003'),
  (x'0000000000001000'), (x'0000000100000000'),
  (x'0000010000000000'), (x'0000010100000000'),
  (x'0000400000000000'), (x'0000400100000000'),
  (x'0000800000000000'), (x'0000800000000001'),
  -- these values taken from upstream testsuite
  (x'00000000000f4240'),
  (x'00000000016e3600'),
  (x'0000008cdcdea440'),
  -- borderline between subnormal and normal
  (x'000ffffffffffff0'), (x'000ffffffffffff1'),
  (x'000ffffffffffffe'), (x'000fffffffffffff');
--Testcase 266:
select float8send(flt) as ibits,
       flt
  from (select bits::bit(64)::bigint::xfloat8::float8 as flt
          from testdata
	offset 0) s;
rollback;
-- round-trip tests

begin;
--Testcase 267:
insert into testdata(bits) values
  (x'0000000000000000'),
  -- smallest normal values
  (x'0010000000000000'), (x'0010000000000001'),
  (x'0010000000000002'), (x'0018000000000000'),
  --
  (x'3ddb7cdfd9d7bdba'), (x'3ddb7cdfd9d7bdbb'), (x'3ddb7cdfd9d7bdbc'),
  (x'3e112e0be826d694'), (x'3e112e0be826d695'), (x'3e112e0be826d696'),
  (x'3e45798ee2308c39'), (x'3e45798ee2308c3a'), (x'3e45798ee2308c3b'),
  (x'3e7ad7f29abcaf47'), (x'3e7ad7f29abcaf48'), (x'3e7ad7f29abcaf49'),
  (x'3eb0c6f7a0b5ed8c'), (x'3eb0c6f7a0b5ed8d'), (x'3eb0c6f7a0b5ed8e'),
  (x'3ee4f8b588e368ef'), (x'3ee4f8b588e368f0'), (x'3ee4f8b588e368f1'),
  (x'3f1a36e2eb1c432c'), (x'3f1a36e2eb1c432d'), (x'3f1a36e2eb1c432e'),
  (x'3f50624dd2f1a9fb'), (x'3f50624dd2f1a9fc'), (x'3f50624dd2f1a9fd'),
  (x'3f847ae147ae147a'), (x'3f847ae147ae147b'), (x'3f847ae147ae147c'),
  (x'3fb9999999999999'), (x'3fb999999999999a'), (x'3fb999999999999b'),
  -- values very close to 1
  (x'3feffffffffffff0'), (x'3feffffffffffff1'), (x'3feffffffffffff2'),
  (x'3feffffffffffff3'), (x'3feffffffffffff4'), (x'3feffffffffffff5'),
  (x'3feffffffffffff6'), (x'3feffffffffffff7'), (x'3feffffffffffff8'),
  (x'3feffffffffffff9'), (x'3feffffffffffffa'), (x'3feffffffffffffb'),
  (x'3feffffffffffffc'), (x'3feffffffffffffd'), (x'3feffffffffffffe'),
  (x'3fefffffffffffff'),
  (x'3ff0000000000000'),
  (x'3ff0000000000001'), (x'3ff0000000000002'), (x'3ff0000000000003'),
  (x'3ff0000000000004'), (x'3ff0000000000005'), (x'3ff0000000000006'),
  (x'3ff0000000000007'), (x'3ff0000000000008'), (x'3ff0000000000009'),
  --
  (x'3ff921fb54442d18'),
  (x'4005bf0a8b14576a'),
  (x'400921fb54442d18'),
  --
  (x'4023ffffffffffff'), (x'4024000000000000'), (x'4024000000000001'),
  (x'4058ffffffffffff'), (x'4059000000000000'), (x'4059000000000001'),
  (x'408f3fffffffffff'), (x'408f400000000000'), (x'408f400000000001'),
  (x'40c387ffffffffff'), (x'40c3880000000000'), (x'40c3880000000001'),
  (x'40f869ffffffffff'), (x'40f86a0000000000'), (x'40f86a0000000001'),
  (x'412e847fffffffff'), (x'412e848000000000'), (x'412e848000000001'),
  (x'416312cfffffffff'), (x'416312d000000000'), (x'416312d000000001'),
  (x'4197d783ffffffff'), (x'4197d78400000000'), (x'4197d78400000001'),
  (x'41cdcd64ffffffff'), (x'41cdcd6500000000'), (x'41cdcd6500000001'),
  (x'4202a05f1fffffff'), (x'4202a05f20000000'), (x'4202a05f20000001'),
  (x'42374876e7ffffff'), (x'42374876e8000000'), (x'42374876e8000001'),
  (x'426d1a94a1ffffff'), (x'426d1a94a2000000'), (x'426d1a94a2000001'),
  (x'42a2309ce53fffff'), (x'42a2309ce5400000'), (x'42a2309ce5400001'),
  (x'42d6bcc41e8fffff'), (x'42d6bcc41e900000'), (x'42d6bcc41e900001'),
  (x'430c6bf52633ffff'), (x'430c6bf526340000'), (x'430c6bf526340001'),
  (x'4341c37937e07fff'), (x'4341c37937e08000'), (x'4341c37937e08001'),
  (x'4376345785d89fff'), (x'4376345785d8a000'), (x'4376345785d8a001'),
  (x'43abc16d674ec7ff'), (x'43abc16d674ec800'), (x'43abc16d674ec801'),
  (x'43e158e460913cff'), (x'43e158e460913d00'), (x'43e158e460913d01'),
  (x'4415af1d78b58c3f'), (x'4415af1d78b58c40'), (x'4415af1d78b58c41'),
  (x'444b1ae4d6e2ef4f'), (x'444b1ae4d6e2ef50'), (x'444b1ae4d6e2ef51'),
  (x'4480f0cf064dd591'), (x'4480f0cf064dd592'), (x'4480f0cf064dd593'),
  (x'44b52d02c7e14af5'), (x'44b52d02c7e14af6'), (x'44b52d02c7e14af7'),
  (x'44ea784379d99db3'), (x'44ea784379d99db4'), (x'44ea784379d99db5'),
  (x'45208b2a2c280290'), (x'45208b2a2c280291'), (x'45208b2a2c280292'),
  --
  (x'7feffffffffffffe'), (x'7fefffffffffffff'),
  -- round to even tests (+ve)
  (x'4350000000000002'),
  (x'4350000000002e06'),
  (x'4352000000000003'),
  (x'4352000000000004'),
  (x'4358000000000003'),
  (x'4358000000000004'),
  (x'435f000000000020'),
  -- round to even tests (-ve)
  (x'c350000000000002'),
  (x'c350000000002e06'),
  (x'c352000000000003'),
  (x'c352000000000004'),
  (x'c358000000000003'),
  (x'c358000000000004'),
  (x'c35f000000000020'),
  -- exercise fixed-point memmoves
  (x'42dc12218377de66'),
  (x'42a674e79c5fe51f'),
  (x'4271f71fb04cb74c'),
  (x'423cbe991a145879'),
  (x'4206fee0e1a9e061'),
  (x'41d26580b487e6b4'),
  (x'419d6f34540ca453'),
  (x'41678c29dcd6e9dc'),
  (x'4132d687e3df217d'),
  (x'40fe240c9fcb68c8'),
  (x'40c81cd6e63c53d3'),
  (x'40934a4584fd0fdc'),
  (x'405edd3c07fb4c93'),
  (x'4028b0fcd32f7076'),
  (x'3ff3c0ca428c59f8'),
  -- these cases come from the upstream's testsuite
  -- LotsOfTrailingZeros)
  (x'3e60000000000000'),
  -- Regression
  (x'c352bd2668e077c4'),
  (x'434018601510c000'),
  (x'43d055dc36f24000'),
  (x'43e052961c6f8000'),
  (x'3ff3c0ca2a5b1d5d'),
  -- LooksLikePow5
  (x'4830f0cf064dd592'),
  (x'4840f0cf064dd592'),
  (x'4850f0cf064dd592'),
  -- OutputLength
  (x'3ff3333333333333'),
  (x'3ff3ae147ae147ae'),
  (x'3ff3be76c8b43958'),
  (x'3ff3c083126e978d'),
  (x'3ff3c0c1fc8f3238'),
  (x'3ff3c0c9539b8887'),
  (x'3ff3c0ca2a5b1d5d'),
  (x'3ff3c0ca4283de1b'),
  (x'3ff3c0ca43db770a'),
  (x'3ff3c0ca428abd53'),
  (x'3ff3c0ca428c1d2b'),
  (x'3ff3c0ca428c51f2'),
  (x'3ff3c0ca428c58fc'),
  (x'3ff3c0ca428c59dd'),
  (x'3ff3c0ca428c59f8'),
  (x'3ff3c0ca428c59fb'),
  -- 32-bit chunking
  (x'40112e0be8047a7d'),
  (x'40112e0be815a889'),
  (x'40112e0be826d695'),
  (x'40112e0be83804a1'),
  (x'40112e0be84932ad'),
  -- MinMaxShift
  (x'0040000000000000'),
  (x'007fffffffffffff'),
  (x'0290000000000000'),
  (x'029fffffffffffff'),
  (x'4350000000000000'),
  (x'435fffffffffffff'),
  (x'1330000000000000'),
  (x'133fffffffffffff'),
  (x'3a6fa7161a4d6e0c');
--Testcase 268:
select float8send(flt) as ibits,
       flt,
       flt::text::float8 as r_flt,
       float8send(flt::text::float8) as obits,
       float8send(flt::text::float8) = float8send(flt) as correct
  from (select bits::bit(64)::bigint::xfloat8::float8 as flt
          from testdata
	offset 0) s;
rollback;
-- clean up, lest opr_sanity complain
--Testcase 269:
drop type xfloat8 cascade;

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

--Testcase 270:
DROP SERVER sqlite_svr;
--Testcase 271:
DROP EXTENSION duckdb_fdw CASCADE;
