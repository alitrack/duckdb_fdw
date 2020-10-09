--
-- FLOAT4
--
--Testcase 46:
CREATE EXTENSION duckdb_fdw;
--Testcase 47:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');
--Testcase 48:
CREATE FOREIGN TABLE FLOAT4_TBL(f1 float4 OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 49:
CREATE FOREIGN TABLE FLOAT4_TMP(f1 float4, id int OPTIONS (key 'true')) SERVER sqlite_svr;

--Testcase 1:
INSERT INTO FLOAT4_TBL(f1) VALUES ('    0.0');
--Testcase 2:
INSERT INTO FLOAT4_TBL(f1) VALUES ('1004.30   ');
--Testcase 3:
INSERT INTO FLOAT4_TBL(f1) VALUES ('     -34.84    ');
--Testcase 4:
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e+20');
--Testcase 5:
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e-20');

-- test for over and under flow
--Testcase 6:
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e70');
--Testcase 7:
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e70');
--Testcase 8:
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e-70');
--Testcase 9:
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e-70');

--Testcase 50:
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e70'::float8);
--Testcase 51:
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e70'::float8);
--Testcase 52:
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e-70'::float8);
--Testcase 53:
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e-70'::float8);

--Testcase 54:
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e400');
--Testcase 55:
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e400');
--Testcase 56:
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e-400');
--Testcase 57:
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e-400');

-- bad input
--Testcase 10:
INSERT INTO FLOAT4_TBL(f1) VALUES ('');
--Testcase 11:
INSERT INTO FLOAT4_TBL(f1) VALUES ('       ');
--Testcase 12:
INSERT INTO FLOAT4_TBL(f1) VALUES ('xyz');
--Testcase 13:
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.0.0');
--Testcase 14:
INSERT INTO FLOAT4_TBL(f1) VALUES ('5 . 0');
--Testcase 15:
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.   0');
--Testcase 16:
INSERT INTO FLOAT4_TBL(f1) VALUES ('     - 3.0');
--Testcase 17:
INSERT INTO FLOAT4_TBL(f1) VALUES ('123            5');

-- special inputs
--Testcase 18:
DELETE FROM FLOAT4_TMP;
--Testcase 58:
INSERT INTO FLOAT4_TMP VALUES ('NaN'::float4);
--Testcase 59:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 20:
DELETE FROM FLOAT4_TMP;
--Testcase 60:
INSERT INTO FLOAT4_TMP VALUES ('nan'::float4);
--Testcase 61:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 21:
DELETE FROM FLOAT4_TMP;
--Testcase 62:
INSERT INTO FLOAT4_TMP VALUES ('   NAN  '::float4);
--Testcase 63:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 22:
DELETE FROM FLOAT4_TMP;
--Testcase 64:
INSERT INTO FLOAT4_TMP VALUES ('infinity'::float4);
--Testcase 65:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 23:
DELETE FROM FLOAT4_TMP;
--Testcase 66:
INSERT INTO FLOAT4_TMP VALUES ('          -INFINiTY   '::float4);
--Testcase 67:
SELECT f1 FROM FLOAT4_TMP;

-- bad special inputs
--Testcase 25:
DELETE FROM FLOAT4_TMP;
--Testcase 68:
INSERT INTO FLOAT4_TMP VALUES ('N A N'::float4);
--Testcase 69:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 26:
DELETE FROM FLOAT4_TMP;
--Testcase 70:
INSERT INTO FLOAT4_TMP VALUES ('NaN x'::float4);
--Testcase 71:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 27:
DELETE FROM FLOAT4_TMP;
--Testcase 72:
INSERT INTO FLOAT4_TMP VALUES (' INFINITY    x'::float4);
--Testcase 73:
SELECT f1 FROM FLOAT4_TMP;

--Testcase 28:
DELETE FROM FLOAT4_TMP;
--Testcase 74:
INSERT INTO FLOAT4_TMP VALUES ('Infinity'::float4 + 100.0);
--Testcase 75:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 30:
DELETE FROM FLOAT4_TMP;
--Testcase 76:
INSERT INTO FLOAT4_TMP VALUES ('Infinity'::float4 / 'Infinity'::float4);
--Testcase 77:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 31:
DELETE FROM FLOAT4_TMP;
--Testcase 78:
INSERT INTO FLOAT4_TMP VALUES ('nan'::float4 / 'nan'::float4);
--Testcase 79:
SELECT f1 FROM FLOAT4_TMP;
--Testcase 32:
DELETE FROM FLOAT4_TMP;
--Testcase 80:
INSERT INTO FLOAT4_TMP VALUES ('nan'::numeric::float4);
--Testcase 81:
SELECT f1 FROM FLOAT4_TMP;

--Testcase 34:
SELECT '' AS five, * FROM FLOAT4_TBL;

--SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3';

--SELECT '' AS one, f.* FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';

--SELECT '' AS three, f.* FROM FLOAT4_TBL f WHERE '1004.3' > f.f1;

--SELECT '' AS three, f.* FROM FLOAT4_TBL f WHERE  f.f1 < '1004.3';

--Testcase 35:
SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE '1004.3' >= f.f1;

--Testcase 36:
SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE  f.f1 <= '1004.3';

--Testcase 37:
SELECT '' AS three, f.f1, f.f1 * '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

--Testcase 38:
SELECT '' AS three, f.f1, f.f1 + '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

--Testcase 39:
SELECT '' AS three, f.f1, f.f1 / '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

--Testcase 40:
SELECT '' AS three, f.f1, f.f1 - '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

-- test divide by zero
--Testcase 41:
SELECT '' AS bad, f.f1 / '0.0' from FLOAT4_TBL f;

--Testcase 42:
SELECT '' AS five, * FROM FLOAT4_TBL;

-- test the unary float4abs operator
--Testcase 43:
SELECT '' AS five, f.f1, @f.f1 AS abs_f1 FROM FLOAT4_TBL f;

--Testcase 44:
UPDATE FLOAT4_TBL
   SET f1 = FLOAT4_TBL.f1 * '-1'
   WHERE FLOAT4_TBL.f1 > '0.0';

--Testcase 45:
SELECT '' AS five, * FROM FLOAT4_TBL;

-- test edge-case coercions to integer
--Testcase 82:
DELETE FROM FLOAT4_TMP;
--Testcase 83:
INSERT INTO FLOAT4_TMP VALUES ('32767.4'::float4);
--Testcase 84:
SELECT f1::int2 FROM FLOAT4_TMP;

--Testcase 85:
DELETE FROM FLOAT4_TMP;
--Testcase 86:
INSERT INTO FLOAT4_TMP VALUES ('32767.6'::float4);
--Testcase 87:
SELECT f1::int2 FROM FLOAT4_TMP;

--Testcase 88:
DELETE FROM FLOAT4_TMP;
--Testcase 89:
INSERT INTO FLOAT4_TMP VALUES ('-32768.4'::float4);
--Testcase 90:
SELECT f1::int2 FROM FLOAT4_TMP;

--Testcase 91:
DELETE FROM FLOAT4_TMP;
--Testcase 92:
INSERT INTO FLOAT4_TMP VALUES ('-32768.6'::float4);
--Testcase 93:
SELECT f1::int2 FROM FLOAT4_TMP;

--Testcase 94:
DELETE FROM FLOAT4_TMP;
--Testcase 95:
INSERT INTO FLOAT4_TMP VALUES ('2147483520'::float4);
--Testcase 96:
SELECT f1::int4 FROM FLOAT4_TMP;

--Testcase 97:
DELETE FROM FLOAT4_TMP;
--Testcase 98:
INSERT INTO FLOAT4_TMP VALUES ('2147483647'::float4);
--Testcase 99:
SELECT f1::int4 FROM FLOAT4_TMP;

--Testcase 100:
DELETE FROM FLOAT4_TMP;
--Testcase 101:
INSERT INTO FLOAT4_TMP VALUES ('-2147483648.5'::float4);
--Testcase 102:
SELECT f1::int4 FROM FLOAT4_TMP;

--Testcase 103:
DELETE FROM FLOAT4_TMP;
--Testcase 104:
INSERT INTO FLOAT4_TMP VALUES ('-2147483900'::float4);
--Testcase 105:
SELECT f1::int4 FROM FLOAT4_TMP;

--Testcase 106:
DELETE FROM FLOAT4_TMP;
--Testcase 107:
INSERT INTO FLOAT4_TMP VALUES ('9223369837831520256'::float4);
--Testcase 108:
SELECT f1::int8 FROM FLOAT4_TMP;

--Testcase 109:
DELETE FROM FLOAT4_TMP;
--Testcase 110:
INSERT INTO FLOAT4_TMP VALUES ('9223372036854775807'::float4);
--Testcase 111:
SELECT f1::int8 FROM FLOAT4_TMP;

--Testcase 112:
DELETE FROM FLOAT4_TMP;
--Testcase 113:
INSERT INTO FLOAT4_TMP VALUES ('-9223372036854775808.5'::float4);
--Testcase 114:
SELECT f1::int8 FROM FLOAT4_TMP;

--Testcase 115:
DELETE FROM FLOAT4_TMP;
--Testcase 116:
INSERT INTO FLOAT4_TMP VALUES ('-9223380000000000000'::float4);
--Testcase 117:
SELECT f1::int8 FROM FLOAT4_TMP;


-- Test for correct input rounding in edge cases.
-- These lists are from Paxson 1991, excluding subnormals and
-- inputs of over 9 sig. digits.
--Testcase 118:
DELETE FROM FLOAT4_TMP;
--Testcase 119:
INSERT INTO FLOAT4_TMP VALUES ('5e-20'::float4);
--Testcase 120:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 121:
DELETE FROM FLOAT4_TMP;
--Testcase 122:
INSERT INTO FLOAT4_TMP VALUES ('67e14'::float4);
--Testcase 123:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 124:
DELETE FROM FLOAT4_TMP;
--Testcase 125:
INSERT INTO FLOAT4_TMP VALUES ('985e15'::float4);
--Testcase 126:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 127:
DELETE FROM FLOAT4_TMP;
--Testcase 128:
INSERT INTO FLOAT4_TMP VALUES ('55895e-16'::float4);
--Testcase 129:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 130:
DELETE FROM FLOAT4_TMP;
--Testcase 131:
INSERT INTO FLOAT4_TMP VALUES ('7038531e-32'::float4);
--Testcase 132:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 133:
DELETE FROM FLOAT4_TMP;
--Testcase 134:
INSERT INTO FLOAT4_TMP VALUES ('702990899e-20'::float4);
--Testcase 135:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 136:
DELETE FROM FLOAT4_TMP;
--Testcase 137:
INSERT INTO FLOAT4_TMP VALUES ('3e-23'::float4);
--Testcase 138:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 139:
DELETE FROM FLOAT4_TMP;
--Testcase 140:
INSERT INTO FLOAT4_TMP VALUES ('57e18'::float4);
--Testcase 141:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 142:
DELETE FROM FLOAT4_TMP;
--Testcase 143:
INSERT INTO FLOAT4_TMP VALUES ('789e-35'::float4);
--Testcase 144:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 145:
DELETE FROM FLOAT4_TMP;
--Testcase 146:
INSERT INTO FLOAT4_TMP VALUES ('2539e-18'::float4);
--Testcase 147:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 148:
DELETE FROM FLOAT4_TMP;
--Testcase 149:
INSERT INTO FLOAT4_TMP VALUES ('76173e28'::float4);
--Testcase 150:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 151:
DELETE FROM FLOAT4_TMP;
--Testcase 152:
INSERT INTO FLOAT4_TMP VALUES ('887745e-11'::float4);
--Testcase 153:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 154:
DELETE FROM FLOAT4_TMP;
--Testcase 155:
INSERT INTO FLOAT4_TMP VALUES ('5382571e-37'::float4);
--Testcase 156:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 157:
DELETE FROM FLOAT4_TMP;
--Testcase 158:
INSERT INTO FLOAT4_TMP VALUES ('82381273e-35'::float4);
--Testcase 159:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 160:
DELETE FROM FLOAT4_TMP;
--Testcase 161:
INSERT INTO FLOAT4_TMP VALUES ('750486563e-38'::float4);
--Testcase 162:
SELECT float4send(f1) FROM FLOAT4_TMP;

-- Test that the smallest possible normalized input value inputs
-- correctly, either in 9-significant-digit or shortest-decimal
-- format.
--
-- exact val is             1.1754943508...
-- shortest val is          1.1754944000
-- midpoint to next val is  1.1754944208...

--Testcase 163:
DELETE FROM FLOAT4_TMP;
--Testcase 164:
INSERT INTO FLOAT4_TMP VALUES ('1.17549435e-38'::float4);
--Testcase 165:
SELECT float4send(f1) FROM FLOAT4_TMP;

--Testcase 166:
DELETE FROM FLOAT4_TMP;
--Testcase 167:
INSERT INTO FLOAT4_TMP VALUES('1.1754944e-38'::float4);
--Testcase 168:
SELECT float4send(f1) FROM FLOAT4_TMP;

-- test output (and round-trip safety) of various values.
-- To ensure we're testing what we think we're testing, start with
-- float values specified by bit patterns (as a useful side effect,
-- this means we'll fail on non-IEEE platforms).

--Testcase 169:
create type xfloat4;
--Testcase 170:
create function xfloat4in(cstring) returns xfloat4 immutable strict
  language internal as 'int4in';
--Testcase 171:
create function xfloat4out(xfloat4) returns cstring immutable strict
  language internal as 'int4out';
--Testcase 172:
create type xfloat4 (input = xfloat4in, output = xfloat4out, like = float4);
--Testcase 173:
create cast (xfloat4 as float4) without function;
--Testcase 174:
create cast (float4 as xfloat4) without function;
--Testcase 175:
create cast (xfloat4 as integer) without function;
--Testcase 176:
create cast (integer as xfloat4) without function;

-- float4: seeeeeee emmmmmmm mmmmmmmm mmmmmmmm

-- we don't care to assume the platform's strtod() handles subnormals
-- correctly; those are "use at your own risk". However we do test
-- subnormal outputs, since those are under our control.

--Testcase 177:
create foreign table testdata(bits text, id int OPTIONS (key 'true')) server sqlite_svr;
begin;
--Testcase 178:
insert into testdata(bits) values
  -- small subnormals
  (x'00000001'),
  (x'00000002'), (x'00000003'),
  (x'00000010'), (x'00000011'), (x'00000100'), (x'00000101'),
  (x'00004000'), (x'00004001'), (x'00080000'), (x'00080001'),
  -- stress values
  (x'0053c4f4'),  -- 7693e-42
  (x'006c85c4'),  -- 996622e-44
  (x'0041ca76'),  -- 60419369e-46
  (x'004b7678'),  -- 6930161142e-48
  -- taken from upstream testsuite
  (x'00000007'),
  (x'00424fe2'),
  -- borderline between subnormal and normal
  (x'007ffff0'), (x'007ffff1'), (x'007ffffe'), (x'007fffff');
--Testcase 179:
select float4send(flt) as ibits,
       flt
  from (select bits::bit(32)::integer::xfloat4::float4 as flt
          from testdata
	offset 0) s;
rollback;

begin;

--Testcase 180:
insert into testdata(bits) values
  (x'00000000'),
  -- smallest normal values
  (x'00800000'), (x'00800001'), (x'00800004'), (x'00800005'),
  (x'00800006'),
  -- small normal values chosen for short vs. long output
  (x'008002f1'), (x'008002f2'), (x'008002f3'),
  (x'00800e17'), (x'00800e18'), (x'00800e19'),
  -- assorted values (random mantissae)
  (x'01000001'), (x'01102843'), (x'01a52c98'),
  (x'0219c229'), (x'02e4464d'), (x'037343c1'), (x'03a91b36'),
  (x'047ada65'), (x'0496fe87'), (x'0550844f'), (x'05999da3'),
  (x'060ea5e2'), (x'06e63c45'), (x'07f1e548'), (x'0fc5282b'),
  (x'1f850283'), (x'2874a9d6'),
  -- values around 5e-08
  (x'3356bf94'), (x'3356bf95'), (x'3356bf96'),
  -- around 1e-07
  (x'33d6bf94'), (x'33d6bf95'), (x'33d6bf96'),
  -- around 3e-07 .. 1e-04
  (x'34a10faf'), (x'34a10fb0'), (x'34a10fb1'),
  (x'350637bc'), (x'350637bd'), (x'350637be'),
  (x'35719786'), (x'35719787'), (x'35719788'),
  (x'358637bc'), (x'358637bd'), (x'358637be'),
  (x'36a7c5ab'), (x'36a7c5ac'), (x'36a7c5ad'),
  (x'3727c5ab'), (x'3727c5ac'), (x'3727c5ad'),
  -- format crossover at 1e-04
  (x'38d1b714'), (x'38d1b715'), (x'38d1b716'),
  (x'38d1b717'), (x'38d1b718'), (x'38d1b719'),
  (x'38d1b71a'), (x'38d1b71b'), (x'38d1b71c'),
  (x'38d1b71d'),
  --
  (x'38dffffe'), (x'38dfffff'), (x'38e00000'),
  (x'38efffff'), (x'38f00000'), (x'38f00001'),
  (x'3a83126e'), (x'3a83126f'), (x'3a831270'),
  (x'3c23d709'), (x'3c23d70a'), (x'3c23d70b'),
  (x'3dcccccc'), (x'3dcccccd'), (x'3dccccce'),
  -- chosen to need 9 digits for 3dcccd70
  (x'3dcccd6f'), (x'3dcccd70'), (x'3dcccd71'),
  --
  (x'3effffff'), (x'3f000000'), (x'3f000001'),
  (x'3f333332'), (x'3f333333'), (x'3f333334'),
  -- approach 1.0 with increasing numbers of 9s
  (x'3f666665'), (x'3f666666'), (x'3f666667'),
  (x'3f7d70a3'), (x'3f7d70a4'), (x'3f7d70a5'),
  (x'3f7fbe76'), (x'3f7fbe77'), (x'3f7fbe78'),
  (x'3f7ff971'), (x'3f7ff972'), (x'3f7ff973'),
  (x'3f7fff57'), (x'3f7fff58'), (x'3f7fff59'),
  (x'3f7fffee'), (x'3f7fffef'),
  -- values very close to 1
  (x'3f7ffff0'), (x'3f7ffff1'), (x'3f7ffff2'),
  (x'3f7ffff3'), (x'3f7ffff4'), (x'3f7ffff5'),
  (x'3f7ffff6'), (x'3f7ffff7'), (x'3f7ffff8'),
  (x'3f7ffff9'), (x'3f7ffffa'), (x'3f7ffffb'),
  (x'3f7ffffc'), (x'3f7ffffd'), (x'3f7ffffe'),
  (x'3f7fffff'),
  (x'3f800000'),
  (x'3f800001'), (x'3f800002'), (x'3f800003'),
  (x'3f800004'), (x'3f800005'), (x'3f800006'),
  (x'3f800007'), (x'3f800008'), (x'3f800009'),
  -- values 1 to 1.1
  (x'3f80000f'), (x'3f800010'), (x'3f800011'),
  (x'3f800012'), (x'3f800013'), (x'3f800014'),
  (x'3f800017'), (x'3f800018'), (x'3f800019'),
  (x'3f80001a'), (x'3f80001b'), (x'3f80001c'),
  (x'3f800029'), (x'3f80002a'), (x'3f80002b'),
  (x'3f800053'), (x'3f800054'), (x'3f800055'),
  (x'3f800346'), (x'3f800347'), (x'3f800348'),
  (x'3f8020c4'), (x'3f8020c5'), (x'3f8020c6'),
  (x'3f8147ad'), (x'3f8147ae'), (x'3f8147af'),
  (x'3f8ccccc'), (x'3f8ccccd'), (x'3f8cccce'),
  --
  (x'3fc90fdb'), -- pi/2
  (x'402df854'), -- e
  (x'40490fdb'), -- pi
  --
  (x'409fffff'), (x'40a00000'), (x'40a00001'),
  (x'40afffff'), (x'40b00000'), (x'40b00001'),
  (x'411fffff'), (x'41200000'), (x'41200001'),
  (x'42c7ffff'), (x'42c80000'), (x'42c80001'),
  (x'4479ffff'), (x'447a0000'), (x'447a0001'),
  (x'461c3fff'), (x'461c4000'), (x'461c4001'),
  (x'47c34fff'), (x'47c35000'), (x'47c35001'),
  (x'497423ff'), (x'49742400'), (x'49742401'),
  (x'4b18967f'), (x'4b189680'), (x'4b189681'),
  (x'4cbebc1f'), (x'4cbebc20'), (x'4cbebc21'),
  (x'4e6e6b27'), (x'4e6e6b28'), (x'4e6e6b29'),
  (x'501502f8'), (x'501502f9'), (x'501502fa'),
  (x'51ba43b6'), (x'51ba43b7'), (x'51ba43b8'),
  -- stress values
  (x'1f6c1e4a'),  -- 5e-20
  (x'59be6cea'),  -- 67e14
  (x'5d5ab6c4'),  -- 985e15
  (x'2cc4a9bd'),  -- 55895e-16
  (x'15ae43fd'),  -- 7038531e-32
  (x'2cf757ca'),  -- 702990899e-20
  (x'665ba998'),  -- 25933168707e13
  (x'743c3324'),  -- 596428896559e20
  -- exercise fixed-point memmoves
  (x'47f1205a'),
  (x'4640e6ae'),
  (x'449a5225'),
  (x'42f6e9d5'),
  (x'414587dd'),
  (x'3f9e064b'),
  -- these cases come from the upstream's testsuite
  -- BoundaryRoundEven
  (x'4c000004'),
  (x'50061c46'),
  (x'510006a8'),
  -- ExactValueRoundEven
  (x'48951f84'),
  (x'45fd1840'),
  -- LotsOfTrailingZeros
  (x'39800000'),
  (x'3b200000'),
  (x'3b900000'),
  (x'3bd00000'),
  -- Regression
  (x'63800000'),
  (x'4b000000'),
  (x'4b800000'),
  (x'4c000001'),
  (x'4c800b0d'),
  (x'00d24584'),
  (x'00d90b88'),
  (x'45803f34'),
  (x'4f9f24f7'),
  (x'3a8722c3'),
  (x'5c800041'),
  (x'15ae43fd'),
  (x'5d4cccfb'),
  (x'4c800001'),
  (x'57800ed8'),
  (x'5f000000'),
  (x'700000f0'),
  (x'5f23e9ac'),
  (x'5e9502f9'),
  (x'5e8012b1'),
  (x'3c000028'),
  (x'60cde861'),
  (x'03aa2a50'),
  (x'43480000'),
  (x'4c000000'),
  -- LooksLikePow5
  (x'5D1502F9'),
  (x'5D9502F9'),
  (x'5E1502F9'),
  -- OutputLength
  (x'3f99999a'),
  (x'3f9d70a4'),
  (x'3f9df3b6'),
  (x'3f9e0419'),
  (x'3f9e0610'),
  (x'3f9e064b'),
  (x'3f9e0651'),
  (x'03d20cfe');

--Testcase 181:
select float4send(flt) as ibits,
       flt,
       flt::text::float4 as r_flt,
       float4send(flt::text::float4) as obits,
       float4send(flt::text::float4) = float4send(flt) as correct
  from (select bits::bit(32)::integer::xfloat4::float4 as flt
          from testdata
	offset 0) s;
rollback;

-- clean up, lest opr_sanity complain
--Testcase 182:
drop type xfloat4 cascade;

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

--Testcase 183:
DROP SERVER sqlite_svr;
--Testcase 184:
DROP EXTENSION duckdb_fdw CASCADE;
