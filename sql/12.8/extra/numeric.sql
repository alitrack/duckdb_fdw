--
-- NUMERIC
--
--Testcase 567:
CREATE EXTENSION duckdb_fdw;
--Testcase 568:
CREATE SERVER duckdb_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test_core.db');

--Testcase 569:
CREATE FOREIGN TABLE num_data (id int4 OPTIONS (key 'true'), val numeric(210,10)) SERVER duckdb_svr;
--Testcase 570:
CREATE FOREIGN TABLE num_exp_add (id1 int4 OPTIONS (key 'true'), id2 int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 571:
CREATE FOREIGN TABLE num_exp_sub (id1 int4 OPTIONS (key 'true'), id2 int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 572:
CREATE FOREIGN TABLE num_exp_div (id1 int4 OPTIONS (key 'true'), id2 int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 573:
CREATE FOREIGN TABLE num_exp_mul (id1 int4 OPTIONS (key 'true'), id2 int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 574:
CREATE FOREIGN TABLE num_exp_sqrt (id int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 575:
CREATE FOREIGN TABLE num_exp_ln (id int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 576:
CREATE FOREIGN TABLE num_exp_log10 (id int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;
--Testcase 577:
CREATE FOREIGN TABLE num_exp_power_10_ln (id int4 OPTIONS (key 'true'), expected numeric(210,10)) SERVER duckdb_svr;

--Testcase 578:
CREATE FOREIGN TABLE num_result (id1 int4 OPTIONS (key 'true'), id2 int4 OPTIONS (key 'true'), result numeric(210,10)) SERVER duckdb_svr;


-- ******************************
-- * The following EXPECTED results are computed by bc(1)
-- * with a scale of 200
-- ******************************

BEGIN TRANSACTION;
--Testcase 1:
INSERT INTO num_exp_add VALUES (0,0,'0');
--Testcase 2:
INSERT INTO num_exp_sub VALUES (0,0,'0');
--Testcase 3:
INSERT INTO num_exp_mul VALUES (0,0,'0');
--Testcase 4:
INSERT INTO num_exp_div VALUES (0,0,'NaN');
--Testcase 5:
INSERT INTO num_exp_add VALUES (0,1,'0');
--Testcase 6:
INSERT INTO num_exp_sub VALUES (0,1,'0');
--Testcase 7:
INSERT INTO num_exp_mul VALUES (0,1,'0');
--Testcase 8:
INSERT INTO num_exp_div VALUES (0,1,'NaN');
--Testcase 9:
INSERT INTO num_exp_add VALUES (0,2,'-34338492.215397047');
--Testcase 10:
INSERT INTO num_exp_sub VALUES (0,2,'34338492.215397047');
--Testcase 11:
INSERT INTO num_exp_mul VALUES (0,2,'0');
--Testcase 12:
INSERT INTO num_exp_div VALUES (0,2,'0');
--Testcase 13:
INSERT INTO num_exp_add VALUES (0,3,'4.31');
--Testcase 14:
INSERT INTO num_exp_sub VALUES (0,3,'-4.31');
--Testcase 15:
INSERT INTO num_exp_mul VALUES (0,3,'0');
--Testcase 16:
INSERT INTO num_exp_div VALUES (0,3,'0');
--Testcase 17:
INSERT INTO num_exp_add VALUES (0,4,'7799461.4119');
--Testcase 18:
INSERT INTO num_exp_sub VALUES (0,4,'-7799461.4119');
--Testcase 19:
INSERT INTO num_exp_mul VALUES (0,4,'0');
--Testcase 20:
INSERT INTO num_exp_div VALUES (0,4,'0');
--Testcase 21:
INSERT INTO num_exp_add VALUES (0,5,'16397.038491');
--Testcase 22:
INSERT INTO num_exp_sub VALUES (0,5,'-16397.038491');
--Testcase 23:
INSERT INTO num_exp_mul VALUES (0,5,'0');
--Testcase 24:
INSERT INTO num_exp_div VALUES (0,5,'0');
--Testcase 25:
INSERT INTO num_exp_add VALUES (0,6,'93901.57763026');
--Testcase 26:
INSERT INTO num_exp_sub VALUES (0,6,'-93901.57763026');
--Testcase 27:
INSERT INTO num_exp_mul VALUES (0,6,'0');
--Testcase 28:
INSERT INTO num_exp_div VALUES (0,6,'0');
--Testcase 29:
INSERT INTO num_exp_add VALUES (0,7,'-83028485');
--Testcase 30:
INSERT INTO num_exp_sub VALUES (0,7,'83028485');
--Testcase 31:
INSERT INTO num_exp_mul VALUES (0,7,'0');
--Testcase 32:
INSERT INTO num_exp_div VALUES (0,7,'0');
--Testcase 33:
INSERT INTO num_exp_add VALUES (0,8,'74881');
--Testcase 34:
INSERT INTO num_exp_sub VALUES (0,8,'-74881');
--Testcase 35:
INSERT INTO num_exp_mul VALUES (0,8,'0');
--Testcase 36:
INSERT INTO num_exp_div VALUES (0,8,'0');
--Testcase 37:
INSERT INTO num_exp_add VALUES (0,9,'-24926804.045047420');
--Testcase 38:
INSERT INTO num_exp_sub VALUES (0,9,'24926804.045047420');
--Testcase 39:
INSERT INTO num_exp_mul VALUES (0,9,'0');
--Testcase 40:
INSERT INTO num_exp_div VALUES (0,9,'0');
--Testcase 41:
INSERT INTO num_exp_add VALUES (1,0,'0');
--Testcase 42:
INSERT INTO num_exp_sub VALUES (1,0,'0');
--Testcase 43:
INSERT INTO num_exp_mul VALUES (1,0,'0');
--Testcase 44:
INSERT INTO num_exp_div VALUES (1,0,'NaN');
--Testcase 45:
INSERT INTO num_exp_add VALUES (1,1,'0');
--Testcase 46:
INSERT INTO num_exp_sub VALUES (1,1,'0');
--Testcase 47:
INSERT INTO num_exp_mul VALUES (1,1,'0');
--Testcase 48:
INSERT INTO num_exp_div VALUES (1,1,'NaN');
--Testcase 49:
INSERT INTO num_exp_add VALUES (1,2,'-34338492.215397047');
--Testcase 50:
INSERT INTO num_exp_sub VALUES (1,2,'34338492.215397047');
--Testcase 51:
INSERT INTO num_exp_mul VALUES (1,2,'0');
--Testcase 52:
INSERT INTO num_exp_div VALUES (1,2,'0');
--Testcase 53:
INSERT INTO num_exp_add VALUES (1,3,'4.31');
--Testcase 54:
INSERT INTO num_exp_sub VALUES (1,3,'-4.31');
--Testcase 55:
INSERT INTO num_exp_mul VALUES (1,3,'0');
--Testcase 56:
INSERT INTO num_exp_div VALUES (1,3,'0');
--Testcase 57:
INSERT INTO num_exp_add VALUES (1,4,'7799461.4119');
--Testcase 58:
INSERT INTO num_exp_sub VALUES (1,4,'-7799461.4119');
--Testcase 59:
INSERT INTO num_exp_mul VALUES (1,4,'0');
--Testcase 60:
INSERT INTO num_exp_div VALUES (1,4,'0');
--Testcase 61:
INSERT INTO num_exp_add VALUES (1,5,'16397.038491');
--Testcase 62:
INSERT INTO num_exp_sub VALUES (1,5,'-16397.038491');
--Testcase 63:
INSERT INTO num_exp_mul VALUES (1,5,'0');
--Testcase 64:
INSERT INTO num_exp_div VALUES (1,5,'0');
--Testcase 65:
INSERT INTO num_exp_add VALUES (1,6,'93901.57763026');
--Testcase 66:
INSERT INTO num_exp_sub VALUES (1,6,'-93901.57763026');
--Testcase 67:
INSERT INTO num_exp_mul VALUES (1,6,'0');
--Testcase 68:
INSERT INTO num_exp_div VALUES (1,6,'0');
--Testcase 69:
INSERT INTO num_exp_add VALUES (1,7,'-83028485');
--Testcase 70:
INSERT INTO num_exp_sub VALUES (1,7,'83028485');
--Testcase 71:
INSERT INTO num_exp_mul VALUES (1,7,'0');
--Testcase 72:
INSERT INTO num_exp_div VALUES (1,7,'0');
--Testcase 73:
INSERT INTO num_exp_add VALUES (1,8,'74881');
--Testcase 74:
INSERT INTO num_exp_sub VALUES (1,8,'-74881');
--Testcase 75:
INSERT INTO num_exp_mul VALUES (1,8,'0');
--Testcase 76:
INSERT INTO num_exp_div VALUES (1,8,'0');
--Testcase 77:
INSERT INTO num_exp_add VALUES (1,9,'-24926804.045047420');
--Testcase 78:
INSERT INTO num_exp_sub VALUES (1,9,'24926804.045047420');
--Testcase 79:
INSERT INTO num_exp_mul VALUES (1,9,'0');
--Testcase 80:
INSERT INTO num_exp_div VALUES (1,9,'0');
--Testcase 81:
INSERT INTO num_exp_add VALUES (2,0,'-34338492.215397047');
--Testcase 82:
INSERT INTO num_exp_sub VALUES (2,0,'-34338492.215397047');
--Testcase 83:
INSERT INTO num_exp_mul VALUES (2,0,'0');
--Testcase 84:
INSERT INTO num_exp_div VALUES (2,0,'NaN');
--Testcase 85:
INSERT INTO num_exp_add VALUES (2,1,'-34338492.215397047');
--Testcase 86:
INSERT INTO num_exp_sub VALUES (2,1,'-34338492.215397047');
--Testcase 87:
INSERT INTO num_exp_mul VALUES (2,1,'0');
--Testcase 88:
INSERT INTO num_exp_div VALUES (2,1,'NaN');
--Testcase 89:
INSERT INTO num_exp_add VALUES (2,2,'-68676984.430794094');
--Testcase 90:
INSERT INTO num_exp_sub VALUES (2,2,'0');
--Testcase 91:
INSERT INTO num_exp_mul VALUES (2,2,'1179132047626883.596862135856320209');
--Testcase 92:
INSERT INTO num_exp_div VALUES (2,2,'1.00000000000000000000');
--Testcase 93:
INSERT INTO num_exp_add VALUES (2,3,'-34338487.905397047');
--Testcase 94:
INSERT INTO num_exp_sub VALUES (2,3,'-34338496.525397047');
--Testcase 95:
INSERT INTO num_exp_mul VALUES (2,3,'-147998901.44836127257');
--Testcase 96:
INSERT INTO num_exp_div VALUES (2,3,'-7967167.56737750510440835266');
--Testcase 97:
INSERT INTO num_exp_add VALUES (2,4,'-26539030.803497047');
--Testcase 98:
INSERT INTO num_exp_sub VALUES (2,4,'-42137953.627297047');
--Testcase 99:
INSERT INTO num_exp_mul VALUES (2,4,'-267821744976817.8111137106593');
--Testcase 100:
INSERT INTO num_exp_div VALUES (2,4,'-4.40267480046830116685');
--Testcase 101:
INSERT INTO num_exp_add VALUES (2,5,'-34322095.176906047');
--Testcase 102:
INSERT INTO num_exp_sub VALUES (2,5,'-34354889.253888047');
--Testcase 103:
INSERT INTO num_exp_mul VALUES (2,5,'-563049578578.769242506736077');
--Testcase 104:
INSERT INTO num_exp_div VALUES (2,5,'-2094.18866914563535496429');
--Testcase 105:
INSERT INTO num_exp_add VALUES (2,6,'-34244590.637766787');
--Testcase 106:
INSERT INTO num_exp_sub VALUES (2,6,'-34432393.793027307');
--Testcase 107:
INSERT INTO num_exp_mul VALUES (2,6,'-3224438592470.18449811926184222');
--Testcase 108:
INSERT INTO num_exp_div VALUES (2,6,'-365.68599891479766440940');
--Testcase 109:
INSERT INTO num_exp_add VALUES (2,7,'-117366977.215397047');
--Testcase 110:
INSERT INTO num_exp_sub VALUES (2,7,'48689992.784602953');
--Testcase 111:
INSERT INTO num_exp_mul VALUES (2,7,'2851072985828710.485883795');
--Testcase 112:
INSERT INTO num_exp_div VALUES (2,7,'.41357483778485235518');
--Testcase 113:
INSERT INTO num_exp_add VALUES (2,8,'-34263611.215397047');
--Testcase 114:
INSERT INTO num_exp_sub VALUES (2,8,'-34413373.215397047');
--Testcase 115:
INSERT INTO num_exp_mul VALUES (2,8,'-2571300635581.146276407');
--Testcase 116:
INSERT INTO num_exp_div VALUES (2,8,'-458.57416721727870888476');
--Testcase 117:
INSERT INTO num_exp_add VALUES (2,9,'-59265296.260444467');
--Testcase 118:
INSERT INTO num_exp_sub VALUES (2,9,'-9411688.170349627');
--Testcase 119:
INSERT INTO num_exp_mul VALUES (2,9,'855948866655588.453741509242968740');
--Testcase 120:
INSERT INTO num_exp_div VALUES (2,9,'1.37757299946438931811');
--Testcase 121:
INSERT INTO num_exp_add VALUES (3,0,'4.31');
--Testcase 122:
INSERT INTO num_exp_sub VALUES (3,0,'4.31');
--Testcase 123:
INSERT INTO num_exp_mul VALUES (3,0,'0');
--Testcase 124:
INSERT INTO num_exp_div VALUES (3,0,'NaN');
--Testcase 125:
INSERT INTO num_exp_add VALUES (3,1,'4.31');
--Testcase 126:
INSERT INTO num_exp_sub VALUES (3,1,'4.31');
--Testcase 127:
INSERT INTO num_exp_mul VALUES (3,1,'0');
--Testcase 128:
INSERT INTO num_exp_div VALUES (3,1,'NaN');
--Testcase 129:
INSERT INTO num_exp_add VALUES (3,2,'-34338487.905397047');
--Testcase 130:
INSERT INTO num_exp_sub VALUES (3,2,'34338496.525397047');
--Testcase 131:
INSERT INTO num_exp_mul VALUES (3,2,'-147998901.44836127257');
--Testcase 132:
INSERT INTO num_exp_div VALUES (3,2,'-.00000012551512084352');
--Testcase 133:
INSERT INTO num_exp_add VALUES (3,3,'8.62');
--Testcase 134:
INSERT INTO num_exp_sub VALUES (3,3,'0');
--Testcase 135:
INSERT INTO num_exp_mul VALUES (3,3,'18.5761');
--Testcase 136:
INSERT INTO num_exp_div VALUES (3,3,'1.00000000000000000000');
--Testcase 137:
INSERT INTO num_exp_add VALUES (3,4,'7799465.7219');
--Testcase 138:
INSERT INTO num_exp_sub VALUES (3,4,'-7799457.1019');
--Testcase 139:
INSERT INTO num_exp_mul VALUES (3,4,'33615678.685289');
--Testcase 140:
INSERT INTO num_exp_div VALUES (3,4,'.00000055260225961552');
--Testcase 141:
INSERT INTO num_exp_add VALUES (3,5,'16401.348491');
--Testcase 142:
INSERT INTO num_exp_sub VALUES (3,5,'-16392.728491');
--Testcase 143:
INSERT INTO num_exp_mul VALUES (3,5,'70671.23589621');
--Testcase 144:
INSERT INTO num_exp_div VALUES (3,5,'.00026285234387695504');
--Testcase 145:
INSERT INTO num_exp_add VALUES (3,6,'93905.88763026');
--Testcase 146:
INSERT INTO num_exp_sub VALUES (3,6,'-93897.26763026');
--Testcase 147:
INSERT INTO num_exp_mul VALUES (3,6,'404715.7995864206');
--Testcase 148:
INSERT INTO num_exp_div VALUES (3,6,'.00004589912234457595');
--Testcase 149:
INSERT INTO num_exp_add VALUES (3,7,'-83028480.69');
--Testcase 150:
INSERT INTO num_exp_sub VALUES (3,7,'83028489.31');
--Testcase 151:
INSERT INTO num_exp_mul VALUES (3,7,'-357852770.35');
--Testcase 152:
INSERT INTO num_exp_div VALUES (3,7,'-.00000005190989574240');
--Testcase 153:
INSERT INTO num_exp_add VALUES (3,8,'74885.31');
--Testcase 154:
INSERT INTO num_exp_sub VALUES (3,8,'-74876.69');
--Testcase 155:
INSERT INTO num_exp_mul VALUES (3,8,'322737.11');
--Testcase 156:
INSERT INTO num_exp_div VALUES (3,8,'.00005755799201399553');
--Testcase 157:
INSERT INTO num_exp_add VALUES (3,9,'-24926799.735047420');
--Testcase 158:
INSERT INTO num_exp_sub VALUES (3,9,'24926808.355047420');
--Testcase 159:
INSERT INTO num_exp_mul VALUES (3,9,'-107434525.43415438020');
--Testcase 160:
INSERT INTO num_exp_div VALUES (3,9,'-.00000017290624149854');
--Testcase 161:
INSERT INTO num_exp_add VALUES (4,0,'7799461.4119');
--Testcase 162:
INSERT INTO num_exp_sub VALUES (4,0,'7799461.4119');
--Testcase 163:
INSERT INTO num_exp_mul VALUES (4,0,'0');
--Testcase 164:
INSERT INTO num_exp_div VALUES (4,0,'NaN');
--Testcase 165:
INSERT INTO num_exp_add VALUES (4,1,'7799461.4119');
--Testcase 166:
INSERT INTO num_exp_sub VALUES (4,1,'7799461.4119');
--Testcase 167:
INSERT INTO num_exp_mul VALUES (4,1,'0');
--Testcase 168:
INSERT INTO num_exp_div VALUES (4,1,'NaN');
--Testcase 169:
INSERT INTO num_exp_add VALUES (4,2,'-26539030.803497047');
--Testcase 170:
INSERT INTO num_exp_sub VALUES (4,2,'42137953.627297047');
--Testcase 171:
INSERT INTO num_exp_mul VALUES (4,2,'-267821744976817.8111137106593');
--Testcase 172:
INSERT INTO num_exp_div VALUES (4,2,'-.22713465002993920385');
--Testcase 173:
INSERT INTO num_exp_add VALUES (4,3,'7799465.7219');
--Testcase 174:
INSERT INTO num_exp_sub VALUES (4,3,'7799457.1019');
--Testcase 175:
INSERT INTO num_exp_mul VALUES (4,3,'33615678.685289');
--Testcase 176:
INSERT INTO num_exp_div VALUES (4,3,'1809619.81714617169373549883');
--Testcase 177:
INSERT INTO num_exp_add VALUES (4,4,'15598922.8238');
--Testcase 178:
INSERT INTO num_exp_sub VALUES (4,4,'0');
--Testcase 179:
INSERT INTO num_exp_mul VALUES (4,4,'60831598315717.14146161');
--Testcase 180:
INSERT INTO num_exp_div VALUES (4,4,'1.00000000000000000000');
--Testcase 181:
INSERT INTO num_exp_add VALUES (4,5,'7815858.450391');
--Testcase 182:
INSERT INTO num_exp_sub VALUES (4,5,'7783064.373409');
--Testcase 183:
INSERT INTO num_exp_mul VALUES (4,5,'127888068979.9935054429');
--Testcase 184:
INSERT INTO num_exp_div VALUES (4,5,'475.66281046305802686061');
--Testcase 185:
INSERT INTO num_exp_add VALUES (4,6,'7893362.98953026');
--Testcase 186:
INSERT INTO num_exp_sub VALUES (4,6,'7705559.83426974');
--Testcase 187:
INSERT INTO num_exp_mul VALUES (4,6,'732381731243.745115764094');
--Testcase 188:
INSERT INTO num_exp_div VALUES (4,6,'83.05996138436129499606');
--Testcase 189:
INSERT INTO num_exp_add VALUES (4,7,'-75229023.5881');
--Testcase 190:
INSERT INTO num_exp_sub VALUES (4,7,'90827946.4119');
--Testcase 191:
INSERT INTO num_exp_mul VALUES (4,7,'-647577464846017.9715');
--Testcase 192:
INSERT INTO num_exp_div VALUES (4,7,'-.09393717604145131637');
--Testcase 193:
INSERT INTO num_exp_add VALUES (4,8,'7874342.4119');
--Testcase 194:
INSERT INTO num_exp_sub VALUES (4,8,'7724580.4119');
--Testcase 195:
INSERT INTO num_exp_mul VALUES (4,8,'584031469984.4839');
--Testcase 196:
INSERT INTO num_exp_div VALUES (4,8,'104.15808298366741897143');
--Testcase 197:
INSERT INTO num_exp_add VALUES (4,9,'-17127342.633147420');
--Testcase 198:
INSERT INTO num_exp_sub VALUES (4,9,'32726265.456947420');
--Testcase 199:
INSERT INTO num_exp_mul VALUES (4,9,'-194415646271340.1815956522980');
--Testcase 200:
INSERT INTO num_exp_div VALUES (4,9,'-.31289456112403769409');
--Testcase 201:
INSERT INTO num_exp_add VALUES (5,0,'16397.038491');
--Testcase 202:
INSERT INTO num_exp_sub VALUES (5,0,'16397.038491');
--Testcase 203:
INSERT INTO num_exp_mul VALUES (5,0,'0');
--Testcase 204:
INSERT INTO num_exp_div VALUES (5,0,'NaN');
--Testcase 205:
INSERT INTO num_exp_add VALUES (5,1,'16397.038491');
--Testcase 206:
INSERT INTO num_exp_sub VALUES (5,1,'16397.038491');
--Testcase 207:
INSERT INTO num_exp_mul VALUES (5,1,'0');
--Testcase 208:
INSERT INTO num_exp_div VALUES (5,1,'NaN');
--Testcase 209:
INSERT INTO num_exp_add VALUES (5,2,'-34322095.176906047');
--Testcase 210:
INSERT INTO num_exp_sub VALUES (5,2,'34354889.253888047');
--Testcase 211:
INSERT INTO num_exp_mul VALUES (5,2,'-563049578578.769242506736077');
--Testcase 212:
INSERT INTO num_exp_div VALUES (5,2,'-.00047751189505192446');
--Testcase 213:
INSERT INTO num_exp_add VALUES (5,3,'16401.348491');
--Testcase 214:
INSERT INTO num_exp_sub VALUES (5,3,'16392.728491');
--Testcase 215:
INSERT INTO num_exp_mul VALUES (5,3,'70671.23589621');
--Testcase 216:
INSERT INTO num_exp_div VALUES (5,3,'3804.41728329466357308584');
--Testcase 217:
INSERT INTO num_exp_add VALUES (5,4,'7815858.450391');
--Testcase 218:
INSERT INTO num_exp_sub VALUES (5,4,'-7783064.373409');
--Testcase 219:
INSERT INTO num_exp_mul VALUES (5,4,'127888068979.9935054429');
--Testcase 220:
INSERT INTO num_exp_div VALUES (5,4,'.00210232958726897192');
--Testcase 221:
INSERT INTO num_exp_add VALUES (5,5,'32794.076982');
--Testcase 222:
INSERT INTO num_exp_sub VALUES (5,5,'0');
--Testcase 223:
INSERT INTO num_exp_mul VALUES (5,5,'268862871.275335557081');
--Testcase 224:
INSERT INTO num_exp_div VALUES (5,5,'1.00000000000000000000');
--Testcase 225:
INSERT INTO num_exp_add VALUES (5,6,'110298.61612126');
--Testcase 226:
INSERT INTO num_exp_sub VALUES (5,6,'-77504.53913926');
--Testcase 227:
INSERT INTO num_exp_mul VALUES (5,6,'1539707782.76899778633766');
--Testcase 228:
INSERT INTO num_exp_div VALUES (5,6,'.17461941433576102689');
--Testcase 229:
INSERT INTO num_exp_add VALUES (5,7,'-83012087.961509');
--Testcase 230:
INSERT INTO num_exp_sub VALUES (5,7,'83044882.038491');
--Testcase 231:
INSERT INTO num_exp_mul VALUES (5,7,'-1361421264394.416135');
--Testcase 232:
INSERT INTO num_exp_div VALUES (5,7,'-.00019748690453643710');
--Testcase 233:
INSERT INTO num_exp_add VALUES (5,8,'91278.038491');
--Testcase 234:
INSERT INTO num_exp_sub VALUES (5,8,'-58483.961509');
--Testcase 235:
INSERT INTO num_exp_mul VALUES (5,8,'1227826639.244571');
--Testcase 236:
INSERT INTO num_exp_div VALUES (5,8,'.21897461960978085228');
--Testcase 237:
INSERT INTO num_exp_add VALUES (5,9,'-24910407.006556420');
--Testcase 238:
INSERT INTO num_exp_sub VALUES (5,9,'24943201.083538420');
--Testcase 239:
INSERT INTO num_exp_mul VALUES (5,9,'-408725765384.257043660243220');
--Testcase 240:
INSERT INTO num_exp_div VALUES (5,9,'-.00065780749354660427');
--Testcase 241:
INSERT INTO num_exp_add VALUES (6,0,'93901.57763026');
--Testcase 242:
INSERT INTO num_exp_sub VALUES (6,0,'93901.57763026');
--Testcase 243:
INSERT INTO num_exp_mul VALUES (6,0,'0');
--Testcase 244:
INSERT INTO num_exp_div VALUES (6,0,'NaN');
--Testcase 245:
INSERT INTO num_exp_add VALUES (6,1,'93901.57763026');
--Testcase 246:
INSERT INTO num_exp_sub VALUES (6,1,'93901.57763026');
--Testcase 247:
INSERT INTO num_exp_mul VALUES (6,1,'0');
--Testcase 248:
INSERT INTO num_exp_div VALUES (6,1,'NaN');
--Testcase 249:
INSERT INTO num_exp_add VALUES (6,2,'-34244590.637766787');
--Testcase 250:
INSERT INTO num_exp_sub VALUES (6,2,'34432393.793027307');
--Testcase 251:
INSERT INTO num_exp_mul VALUES (6,2,'-3224438592470.18449811926184222');
--Testcase 252:
INSERT INTO num_exp_div VALUES (6,2,'-.00273458651128995823');
--Testcase 253:
INSERT INTO num_exp_add VALUES (6,3,'93905.88763026');
--Testcase 254:
INSERT INTO num_exp_sub VALUES (6,3,'93897.26763026');
--Testcase 255:
INSERT INTO num_exp_mul VALUES (6,3,'404715.7995864206');
--Testcase 256:
INSERT INTO num_exp_div VALUES (6,3,'21786.90896293735498839907');
--Testcase 257:
INSERT INTO num_exp_add VALUES (6,4,'7893362.98953026');
--Testcase 258:
INSERT INTO num_exp_sub VALUES (6,4,'-7705559.83426974');
--Testcase 259:
INSERT INTO num_exp_mul VALUES (6,4,'732381731243.745115764094');
--Testcase 260:
INSERT INTO num_exp_div VALUES (6,4,'.01203949512295682469');
--Testcase 261:
INSERT INTO num_exp_add VALUES (6,5,'110298.61612126');
--Testcase 262:
INSERT INTO num_exp_sub VALUES (6,5,'77504.53913926');
--Testcase 263:
INSERT INTO num_exp_mul VALUES (6,5,'1539707782.76899778633766');
--Testcase 264:
INSERT INTO num_exp_div VALUES (6,5,'5.72674008674192359679');
--Testcase 265:
INSERT INTO num_exp_add VALUES (6,6,'187803.15526052');
--Testcase 266:
INSERT INTO num_exp_sub VALUES (6,6,'0');
--Testcase 267:
INSERT INTO num_exp_mul VALUES (6,6,'8817506281.4517452372676676');
--Testcase 268:
INSERT INTO num_exp_div VALUES (6,6,'1.00000000000000000000');
--Testcase 269:
INSERT INTO num_exp_add VALUES (6,7,'-82934583.42236974');
--Testcase 270:
INSERT INTO num_exp_sub VALUES (6,7,'83122386.57763026');
--Testcase 271:
INSERT INTO num_exp_mul VALUES (6,7,'-7796505729750.37795610');
--Testcase 272:
INSERT INTO num_exp_div VALUES (6,7,'-.00113095617281538980');
--Testcase 273:
INSERT INTO num_exp_add VALUES (6,8,'168782.57763026');
--Testcase 274:
INSERT INTO num_exp_sub VALUES (6,8,'19020.57763026');
--Testcase 275:
INSERT INTO num_exp_mul VALUES (6,8,'7031444034.53149906');
--Testcase 276:
INSERT INTO num_exp_div VALUES (6,8,'1.25401073209839612184');
--Testcase 277:
INSERT INTO num_exp_add VALUES (6,9,'-24832902.467417160');
--Testcase 278:
INSERT INTO num_exp_sub VALUES (6,9,'25020705.622677680');
--Testcase 279:
INSERT INTO num_exp_mul VALUES (6,9,'-2340666225110.29929521292692920');
--Testcase 280:
INSERT INTO num_exp_div VALUES (6,9,'-.00376709254265256789');
--Testcase 281:
INSERT INTO num_exp_add VALUES (7,0,'-83028485');
--Testcase 282:
INSERT INTO num_exp_sub VALUES (7,0,'-83028485');
--Testcase 283:
INSERT INTO num_exp_mul VALUES (7,0,'0');
--Testcase 284:
INSERT INTO num_exp_div VALUES (7,0,'NaN');
--Testcase 285:
INSERT INTO num_exp_add VALUES (7,1,'-83028485');
--Testcase 286:
INSERT INTO num_exp_sub VALUES (7,1,'-83028485');
--Testcase 287:
INSERT INTO num_exp_mul VALUES (7,1,'0');
--Testcase 288:
INSERT INTO num_exp_div VALUES (7,1,'NaN');
--Testcase 289:
INSERT INTO num_exp_add VALUES (7,2,'-117366977.215397047');
--Testcase 290:
INSERT INTO num_exp_sub VALUES (7,2,'-48689992.784602953');
--Testcase 291:
INSERT INTO num_exp_mul VALUES (7,2,'2851072985828710.485883795');
--Testcase 292:
INSERT INTO num_exp_div VALUES (7,2,'2.41794207151503385700');
--Testcase 293:
INSERT INTO num_exp_add VALUES (7,3,'-83028480.69');
--Testcase 294:
INSERT INTO num_exp_sub VALUES (7,3,'-83028489.31');
--Testcase 295:
INSERT INTO num_exp_mul VALUES (7,3,'-357852770.35');
--Testcase 296:
INSERT INTO num_exp_div VALUES (7,3,'-19264149.65197215777262180974');
--Testcase 297:
INSERT INTO num_exp_add VALUES (7,4,'-75229023.5881');
--Testcase 298:
INSERT INTO num_exp_sub VALUES (7,4,'-90827946.4119');
--Testcase 299:
INSERT INTO num_exp_mul VALUES (7,4,'-647577464846017.9715');
--Testcase 300:
INSERT INTO num_exp_div VALUES (7,4,'-10.64541262725136247686');
--Testcase 301:
INSERT INTO num_exp_add VALUES (7,5,'-83012087.961509');
--Testcase 302:
INSERT INTO num_exp_sub VALUES (7,5,'-83044882.038491');
--Testcase 303:
INSERT INTO num_exp_mul VALUES (7,5,'-1361421264394.416135');
--Testcase 304:
INSERT INTO num_exp_div VALUES (7,5,'-5063.62688881730941836574');
--Testcase 305:
INSERT INTO num_exp_add VALUES (7,6,'-82934583.42236974');
--Testcase 306:
INSERT INTO num_exp_sub VALUES (7,6,'-83122386.57763026');
--Testcase 307:
INSERT INTO num_exp_mul VALUES (7,6,'-7796505729750.37795610');
--Testcase 308:
INSERT INTO num_exp_div VALUES (7,6,'-884.20756174009028770294');
--Testcase 309:
INSERT INTO num_exp_add VALUES (7,7,'-166056970');
--Testcase 310:
INSERT INTO num_exp_sub VALUES (7,7,'0');
--Testcase 311:
INSERT INTO num_exp_mul VALUES (7,7,'6893729321395225');
--Testcase 312:
INSERT INTO num_exp_div VALUES (7,7,'1.00000000000000000000');
--Testcase 313:
INSERT INTO num_exp_add VALUES (7,8,'-82953604');
--Testcase 314:
INSERT INTO num_exp_sub VALUES (7,8,'-83103366');
--Testcase 315:
INSERT INTO num_exp_mul VALUES (7,8,'-6217255985285');
--Testcase 316:
INSERT INTO num_exp_div VALUES (7,8,'-1108.80577182462841041118');
--Testcase 317:
INSERT INTO num_exp_add VALUES (7,9,'-107955289.045047420');
--Testcase 318:
INSERT INTO num_exp_sub VALUES (7,9,'-58101680.954952580');
--Testcase 319:
INSERT INTO num_exp_mul VALUES (7,9,'2069634775752159.035758700');
--Testcase 320:
INSERT INTO num_exp_div VALUES (7,9,'3.33089171198810413382');
--Testcase 321:
INSERT INTO num_exp_add VALUES (8,0,'74881');
--Testcase 322:
INSERT INTO num_exp_sub VALUES (8,0,'74881');
--Testcase 323:
INSERT INTO num_exp_mul VALUES (8,0,'0');
--Testcase 324:
INSERT INTO num_exp_div VALUES (8,0,'NaN');
--Testcase 325:
INSERT INTO num_exp_add VALUES (8,1,'74881');
--Testcase 326:
INSERT INTO num_exp_sub VALUES (8,1,'74881');
--Testcase 327:
INSERT INTO num_exp_mul VALUES (8,1,'0');
--Testcase 328:
INSERT INTO num_exp_div VALUES (8,1,'NaN');
--Testcase 329:
INSERT INTO num_exp_add VALUES (8,2,'-34263611.215397047');
--Testcase 330:
INSERT INTO num_exp_sub VALUES (8,2,'34413373.215397047');
--Testcase 331:
INSERT INTO num_exp_mul VALUES (8,2,'-2571300635581.146276407');
--Testcase 332:
INSERT INTO num_exp_div VALUES (8,2,'-.00218067233500788615');
--Testcase 333:
INSERT INTO num_exp_add VALUES (8,3,'74885.31');
--Testcase 334:
INSERT INTO num_exp_sub VALUES (8,3,'74876.69');
--Testcase 335:
INSERT INTO num_exp_mul VALUES (8,3,'322737.11');
--Testcase 336:
INSERT INTO num_exp_div VALUES (8,3,'17373.78190255220417633410');
--Testcase 337:
INSERT INTO num_exp_add VALUES (8,4,'7874342.4119');
--Testcase 338:
INSERT INTO num_exp_sub VALUES (8,4,'-7724580.4119');
--Testcase 339:
INSERT INTO num_exp_mul VALUES (8,4,'584031469984.4839');
--Testcase 340:
INSERT INTO num_exp_div VALUES (8,4,'.00960079113741758956');
--Testcase 341:
INSERT INTO num_exp_add VALUES (8,5,'91278.038491');
--Testcase 342:
INSERT INTO num_exp_sub VALUES (8,5,'58483.961509');
--Testcase 343:
INSERT INTO num_exp_mul VALUES (8,5,'1227826639.244571');
--Testcase 344:
INSERT INTO num_exp_div VALUES (8,5,'4.56673929509287019456');
--Testcase 345:
INSERT INTO num_exp_add VALUES (8,6,'168782.57763026');
--Testcase 346:
INSERT INTO num_exp_sub VALUES (8,6,'-19020.57763026');
--Testcase 347:
INSERT INTO num_exp_mul VALUES (8,6,'7031444034.53149906');
--Testcase 348:
INSERT INTO num_exp_div VALUES (8,6,'.79744134113322314424');
--Testcase 349:
INSERT INTO num_exp_add VALUES (8,7,'-82953604');
--Testcase 350:
INSERT INTO num_exp_sub VALUES (8,7,'83103366');
--Testcase 351:
INSERT INTO num_exp_mul VALUES (8,7,'-6217255985285');
--Testcase 352:
INSERT INTO num_exp_div VALUES (8,7,'-.00090187120721280172');
--Testcase 353:
INSERT INTO num_exp_add VALUES (8,8,'149762');
--Testcase 354:
INSERT INTO num_exp_sub VALUES (8,8,'0');
--Testcase 355:
INSERT INTO num_exp_mul VALUES (8,8,'5607164161');
--Testcase 356:
INSERT INTO num_exp_div VALUES (8,8,'1.00000000000000000000');
--Testcase 357:
INSERT INTO num_exp_add VALUES (8,9,'-24851923.045047420');
--Testcase 358:
INSERT INTO num_exp_sub VALUES (8,9,'25001685.045047420');
--Testcase 359:
INSERT INTO num_exp_mul VALUES (8,9,'-1866544013697.195857020');
--Testcase 360:
INSERT INTO num_exp_div VALUES (8,9,'-.00300403532938582735');
--Testcase 361:
INSERT INTO num_exp_add VALUES (9,0,'-24926804.045047420');
--Testcase 362:
INSERT INTO num_exp_sub VALUES (9,0,'-24926804.045047420');
--Testcase 363:
INSERT INTO num_exp_mul VALUES (9,0,'0');
--Testcase 364:
INSERT INTO num_exp_div VALUES (9,0,'NaN');
--Testcase 365:
INSERT INTO num_exp_add VALUES (9,1,'-24926804.045047420');
--Testcase 366:
INSERT INTO num_exp_sub VALUES (9,1,'-24926804.045047420');
--Testcase 367:
INSERT INTO num_exp_mul VALUES (9,1,'0');
--Testcase 368:
INSERT INTO num_exp_div VALUES (9,1,'NaN');
--Testcase 369:
INSERT INTO num_exp_add VALUES (9,2,'-59265296.260444467');
--Testcase 370:
INSERT INTO num_exp_sub VALUES (9,2,'9411688.170349627');
--Testcase 371:
INSERT INTO num_exp_mul VALUES (9,2,'855948866655588.453741509242968740');
--Testcase 372:
INSERT INTO num_exp_div VALUES (9,2,'.72591434384152961526');
--Testcase 373:
INSERT INTO num_exp_add VALUES (9,3,'-24926799.735047420');
--Testcase 374:
INSERT INTO num_exp_sub VALUES (9,3,'-24926808.355047420');
--Testcase 375:
INSERT INTO num_exp_mul VALUES (9,3,'-107434525.43415438020');
--Testcase 376:
INSERT INTO num_exp_div VALUES (9,3,'-5783481.21694835730858468677');
--Testcase 377:
INSERT INTO num_exp_add VALUES (9,4,'-17127342.633147420');
--Testcase 378:
INSERT INTO num_exp_sub VALUES (9,4,'-32726265.456947420');
--Testcase 379:
INSERT INTO num_exp_mul VALUES (9,4,'-194415646271340.1815956522980');
--Testcase 380:
INSERT INTO num_exp_div VALUES (9,4,'-3.19596478892958416484');
--Testcase 381:
INSERT INTO num_exp_add VALUES (9,5,'-24910407.006556420');
--Testcase 382:
INSERT INTO num_exp_sub VALUES (9,5,'-24943201.083538420');
--Testcase 383:
INSERT INTO num_exp_mul VALUES (9,5,'-408725765384.257043660243220');
--Testcase 384:
INSERT INTO num_exp_div VALUES (9,5,'-1520.20159364322004505807');
--Testcase 385:
INSERT INTO num_exp_add VALUES (9,6,'-24832902.467417160');
--Testcase 386:
INSERT INTO num_exp_sub VALUES (9,6,'-25020705.622677680');
--Testcase 387:
INSERT INTO num_exp_mul VALUES (9,6,'-2340666225110.29929521292692920');
--Testcase 388:
INSERT INTO num_exp_div VALUES (9,6,'-265.45671195426965751280');
--Testcase 389:
INSERT INTO num_exp_add VALUES (9,7,'-107955289.045047420');
--Testcase 390:
INSERT INTO num_exp_sub VALUES (9,7,'58101680.954952580');
--Testcase 391:
INSERT INTO num_exp_mul VALUES (9,7,'2069634775752159.035758700');
--Testcase 392:
INSERT INTO num_exp_div VALUES (9,7,'.30021990699995814689');
--Testcase 393:
INSERT INTO num_exp_add VALUES (9,8,'-24851923.045047420');
--Testcase 394:
INSERT INTO num_exp_sub VALUES (9,8,'-25001685.045047420');
--Testcase 395:
INSERT INTO num_exp_mul VALUES (9,8,'-1866544013697.195857020');
--Testcase 396:
INSERT INTO num_exp_div VALUES (9,8,'-332.88556569820675471748');
--Testcase 397:
INSERT INTO num_exp_add VALUES (9,9,'-49853608.090094840');
--Testcase 398:
INSERT INTO num_exp_sub VALUES (9,9,'0');
--Testcase 399:
INSERT INTO num_exp_mul VALUES (9,9,'621345559900192.420120630048656400');
--Testcase 400:
INSERT INTO num_exp_div VALUES (9,9,'1.00000000000000000000');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 401:
INSERT INTO num_exp_sqrt VALUES (0,'0');
--Testcase 402:
INSERT INTO num_exp_sqrt VALUES (1,'0');
--Testcase 403:
INSERT INTO num_exp_sqrt VALUES (2,'5859.90547836712524903505');
--Testcase 404:
INSERT INTO num_exp_sqrt VALUES (3,'2.07605394920266944396');
--Testcase 405:
INSERT INTO num_exp_sqrt VALUES (4,'2792.75158435189147418923');
--Testcase 406:
INSERT INTO num_exp_sqrt VALUES (5,'128.05092147657509145473');
--Testcase 407:
INSERT INTO num_exp_sqrt VALUES (6,'306.43364311096782703406');
--Testcase 408:
INSERT INTO num_exp_sqrt VALUES (7,'9111.99676251039939975230');
--Testcase 409:
INSERT INTO num_exp_sqrt VALUES (8,'273.64392922189960397542');
--Testcase 410:
INSERT INTO num_exp_sqrt VALUES (9,'4992.67503899937593364766');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 411:
INSERT INTO num_exp_ln VALUES (0,'NaN');
--Testcase 412:
INSERT INTO num_exp_ln VALUES (1,'NaN');
--Testcase 413:
INSERT INTO num_exp_ln VALUES (2,'17.35177750493897715514');
--Testcase 414:
INSERT INTO num_exp_ln VALUES (3,'1.46093790411565641971');
--Testcase 415:
INSERT INTO num_exp_ln VALUES (4,'15.86956523951936572464');
--Testcase 416:
INSERT INTO num_exp_ln VALUES (5,'9.70485601768871834038');
--Testcase 417:
INSERT INTO num_exp_ln VALUES (6,'11.45000246622944403127');
--Testcase 418:
INSERT INTO num_exp_ln VALUES (7,'18.23469429965478772991');
--Testcase 419:
INSERT INTO num_exp_ln VALUES (8,'11.22365546576315513668');
--Testcase 420:
INSERT INTO num_exp_ln VALUES (9,'17.03145425013166006962');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 421:
INSERT INTO num_exp_log10 VALUES (0,'NaN');
--Testcase 422:
INSERT INTO num_exp_log10 VALUES (1,'NaN');
--Testcase 423:
INSERT INTO num_exp_log10 VALUES (2,'7.53578122160797276459');
--Testcase 424:
INSERT INTO num_exp_log10 VALUES (3,'.63447727016073160075');
--Testcase 425:
INSERT INTO num_exp_log10 VALUES (4,'6.89206461372691743345');
--Testcase 426:
INSERT INTO num_exp_log10 VALUES (5,'4.21476541614777768626');
--Testcase 427:
INSERT INTO num_exp_log10 VALUES (6,'4.97267288886207207671');
--Testcase 428:
INSERT INTO num_exp_log10 VALUES (7,'7.91922711353275546914');
--Testcase 429:
INSERT INTO num_exp_log10 VALUES (8,'4.87437163556421004138');
--Testcase 430:
INSERT INTO num_exp_log10 VALUES (9,'7.39666659961986567059');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 431:
INSERT INTO num_exp_power_10_ln VALUES (0,'NaN');
--Testcase 432:
INSERT INTO num_exp_power_10_ln VALUES (1,'NaN');
--Testcase 433:
INSERT INTO num_exp_power_10_ln VALUES (2,'224790267919917955.13261618583642653184');
--Testcase 434:
INSERT INTO num_exp_power_10_ln VALUES (3,'28.90266599445155957393');
--Testcase 435:
INSERT INTO num_exp_power_10_ln VALUES (4,'7405685069594999.07733999469386277636');
--Testcase 436:
INSERT INTO num_exp_power_10_ln VALUES (5,'5068226527.32127265408584640098');
--Testcase 437:
INSERT INTO num_exp_power_10_ln VALUES (6,'281839893606.99372343357047819067');
--Testcase 438:
INSERT INTO num_exp_power_10_ln VALUES (7,'1716699575118597095.42330819910640247627');
--Testcase 439:
INSERT INTO num_exp_power_10_ln VALUES (8,'167361463828.07491320069016125952');
--Testcase 440:
INSERT INTO num_exp_power_10_ln VALUES (9,'107511333880052007.04141124673540337457');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 441:
INSERT INTO num_data VALUES (0, '0');
--Testcase 442:
INSERT INTO num_data VALUES (1, '0');
--Testcase 443:
INSERT INTO num_data VALUES (2, '-34338492.215397047');
--Testcase 444:
INSERT INTO num_data VALUES (3, '4.31');
--Testcase 445:
INSERT INTO num_data VALUES (4, '7799461.4119');
--Testcase 446:
INSERT INTO num_data VALUES (5, '16397.038491');
--Testcase 447:
INSERT INTO num_data VALUES (6, '93901.57763026');
--Testcase 448:
INSERT INTO num_data VALUES (7, '-83028485');
--Testcase 449:
INSERT INTO num_data VALUES (8, '74881');
--Testcase 450:
INSERT INTO num_data VALUES (9, '-24926804.045047420');

COMMIT TRANSACTION;

-- ******************************
-- * Create indices for faster checks
-- ******************************
-- Skip these setting, creating foreign table with primary key already covered.
--CREATE UNIQUE INDEX num_exp_add_idx ON num_exp_add (id1, id2);
--CREATE UNIQUE INDEX num_exp_sub_idx ON num_exp_sub (id1, id2);
--CREATE UNIQUE INDEX num_exp_div_idx ON num_exp_div (id1, id2);
--CREATE UNIQUE INDEX num_exp_mul_idx ON num_exp_mul (id1, id2);
--CREATE UNIQUE INDEX num_exp_sqrt_idx ON num_exp_sqrt (id);
--CREATE UNIQUE INDEX num_exp_ln_idx ON num_exp_ln (id);
--CREATE UNIQUE INDEX num_exp_log10_idx ON num_exp_log10 (id);
--CREATE UNIQUE INDEX num_exp_power_10_ln_idx ON num_exp_power_10_ln (id);
--VACUUM ANALYZE num_exp_add;
--VACUUM ANALYZE num_exp_sub;
--VACUUM ANALYZE num_exp_div;
--VACUUM ANALYZE num_exp_mul;
--VACUUM ANALYZE num_exp_sqrt;
--VACUUM ANALYZE num_exp_ln;
--VACUUM ANALYZE num_exp_log10;
--VACUUM ANALYZE num_exp_power_10_ln;

-- ******************************
-- * Now check the behaviour of the NUMERIC type
-- ******************************

-- ******************************
-- * Addition check
-- ******************************

--Testcase 451:
DELETE FROM num_result;
--Testcase 452:
INSERT INTO num_result SELECT t1.id, t2.id, t1.val + t2.val
    FROM num_data t1, num_data t2;
--Testcase 453:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;

--Testcase 454:
DELETE FROM num_result;
--Testcase 455:
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val + t2.val, 10)
    FROM num_data t1, num_data t2;
--Testcase 456:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 10) as expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 10);

-- ******************************
-- * Subtraction check
-- ******************************
--Testcase 457:
DELETE FROM num_result;
--Testcase 458:
INSERT INTO num_result SELECT t1.id, t2.id, t1.val - t2.val
    FROM num_data t1, num_data t2;
--Testcase 459:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;

--Testcase 460:
DELETE FROM num_result;
--Testcase 461:
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val - t2.val, 40)
    FROM num_data t1, num_data t2;
--Testcase 462:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 40)
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 40);

-- ******************************
-- * Multiply check
-- ******************************
--Testcase 463:
DELETE FROM num_result;
--Testcase 464:
INSERT INTO num_result SELECT t1.id, t2.id, t1.val * t2.val
    FROM num_data t1, num_data t2;
--Testcase 465:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;

--Testcase 466:
DELETE FROM num_result;
--Testcase 467:
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val * t2.val, 30)
    FROM num_data t1, num_data t2;
--Testcase 468:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 30) as expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 30);

-- ******************************
-- * Division check
-- ******************************
--Testcase 469:
DELETE FROM num_result;
--Testcase 470:
INSERT INTO num_result SELECT t1.id, t2.id, t1.val / t2.val
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
--Testcase 471:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;

--Testcase 472:
DELETE FROM num_result;
--Testcase 473:
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val / t2.val, 80)
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
--Testcase 474:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 80) as expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 80);

-- ******************************
-- * Square root check
-- ******************************
--Testcase 475:
DELETE FROM num_result;
--Testcase 476:
INSERT INTO num_result SELECT id, 0, SQRT(ABS(val))
    FROM num_data;
--Testcase 477:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_sqrt t2
    WHERE t1.id1 = t2.id
    AND t1.result != t2.expected;

-- ******************************
-- * Natural logarithm check
-- ******************************
--Testcase 478:
DELETE FROM num_result;
--Testcase 479:
INSERT INTO num_result SELECT id, 0, LN(ABS(val))
    FROM num_data
    WHERE val != '0.0';
--Testcase 480:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result != t2.expected;

-- ******************************
-- * Logarithm base 10 check
-- ******************************
--Testcase 481:
DELETE FROM num_result;
--Testcase 482:
INSERT INTO num_result SELECT id, 0, LOG(numeric '10', ABS(val))
    FROM num_data
    WHERE val != '0.0';
--Testcase 483:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_log10 t2
    WHERE t1.id1 = t2.id
    AND t1.result != t2.expected;

-- ******************************
-- * POWER(10, LN(value)) check
-- ******************************
--Testcase 484:
DELETE FROM num_result;
--Testcase 485:
INSERT INTO num_result SELECT id, 0, POWER(numeric '10', LN(ABS(round(val,200))))
    FROM num_data
    WHERE val != '0.0';
--Testcase 486:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_power_10_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result != t2.expected;

-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- numeric AVG used to fail on some platforms
--Testcase 487:
SELECT AVG(val) FROM num_data;
--Testcase 488:
SELECT STDDEV(val) FROM num_data;
--Testcase 489:
SELECT VARIANCE(val) FROM num_data;

-- Check for appropriate rounding and overflow
--Testcase 579:
CREATE FOREIGN TABLE fract_only (id int, val numeric(4,4)) SERVER duckdb_svr;
--Testcase 490:
INSERT INTO fract_only VALUES (1, '0.0');
--Testcase 491:
INSERT INTO fract_only VALUES (2, '0.1');
--Testcase 492:
INSERT INTO fract_only VALUES (3, '1.0');	-- should fail
--Testcase 493:
INSERT INTO fract_only VALUES (4, '-0.9999');
--Testcase 494:
INSERT INTO fract_only VALUES (5, '0.99994');
--Testcase 495:
INSERT INTO fract_only VALUES (6, '0.99995');  -- should fail
--Testcase 496:
INSERT INTO fract_only VALUES (7, '0.00001');
--Testcase 497:
INSERT INTO fract_only VALUES (8, '0.00017');
--Testcase 498:
SELECT * FROM fract_only;
--Testcase 580:
DROP FOREIGN TABLE fract_only;

-- Check inf/nan conversion behavior
--Testcase 581:
CREATE FOREIGN TABLE FLOAT8_TMP(f1 float8, f2 float8, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 582:
DELETE FROM FLOAT8_TMP;
--Testcase 583:
INSERT INTO FLOAT8_TMP VALUES ('NaN');
--Testcase 584:
SELECT f1::numeric FROM FLOAT8_TMP;

--Testcase 585:
DELETE FROM FLOAT8_TMP;
--Testcase 586:
INSERT INTO FLOAT8_TMP VALUES ('Infinity');
--Testcase 587:
SELECT f1::numeric FROM FLOAT8_TMP;

--Testcase 588:
DELETE FROM FLOAT8_TMP;
--Testcase 589:
INSERT INTO FLOAT8_TMP VALUES ('-Infinity');
--Testcase 590:
SELECT f1::numeric FROM FLOAT8_TMP;


--Testcase 591:
CREATE FOREIGN TABLE FLOAT4_TMP(f1 float4, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 592:
DELETE FROM FLOAT4_TMP;
--Testcase 593:
INSERT INTO FLOAT4_TMP VALUES ('NaN');
--Testcase 594:
SELECT f1::numeric FROM FLOAT4_TMP;

--Testcase 595:
DELETE FROM FLOAT4_TMP;
--Testcase 596:
INSERT INTO FLOAT4_TMP VALUES ('Infinity');
--Testcase 597:
SELECT f1::numeric FROM FLOAT4_TMP;

--Testcase 598:
DELETE FROM FLOAT4_TMP;
--Testcase 599:
INSERT INTO FLOAT4_TMP VALUES ('-Infinity');
--Testcase 600:
SELECT f1::numeric FROM FLOAT4_TMP;

-- Simple check that ceil(), floor(), and round() work correctly
--Testcase 601:
CREATE FOREIGN TABLE ceil_floor_round (a numeric OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 499:
INSERT INTO ceil_floor_round VALUES ('-5.5');
--Testcase 500:
INSERT INTO ceil_floor_round VALUES ('-5.499999');
--Testcase 501:
INSERT INTO ceil_floor_round VALUES ('9.5');
--Testcase 502:
INSERT INTO ceil_floor_round VALUES ('9.4999999');
--Testcase 503:
INSERT INTO ceil_floor_round VALUES ('0.0');
--Testcase 504:
INSERT INTO ceil_floor_round VALUES ('0.0000001');
--Testcase 505:
INSERT INTO ceil_floor_round VALUES ('-0.000001');
--Testcase 506:
SELECT a, ceil(a), ceiling(a), floor(a), round(a) FROM ceil_floor_round ORDER BY a;

-- Check rounding, it should round ties away from zero.
--Testcase 602:
CREATE FOREIGN TABLE INT4_TMP(f1 int4, f2 int4, id int OPTIONS (key 'true')) SERVER duckdb_svr;
--Testcase 603:
DELETE FROM INT4_TMP;
--Testcase 604:
INSERT INTO INT4_TMP SELECT a FROM generate_series(-5,5) a;
--Testcase 605:
SELECT f1 as pow,
	round((-2.5 * 10 ^ f1)::numeric, -f1),
	round((-1.5 * 10 ^ f1)::numeric, -f1),
	round((-0.5 * 10 ^ f1)::numeric, -f1),
	round((0.5 * 10 ^ f1)::numeric, -f1),
	round((1.5 * 10 ^ f1)::numeric, -f1),
	round((2.5 * 10 ^ f1)::numeric, -f1)
FROM INT4_TMP;

-- Testing for width_bucket(). For convenience, we test both the
-- numeric and float8 versions of the function in this file.
-- errors
--Testcase 606:
CREATE FOREIGN TABLE width_bucket_tbl (
	id1 numeric,
	id2 numeric,
	id3 numeric,
	id4 int,
	id int OPTIONS (key 'true')
) SERVER duckdb_svr;

--Testcase 607:
DELETE FROM width_bucket_tbl;
--Testcase 608:
INSERT INTO width_bucket_tbl VALUES (5.0, 3.0, 4.0, 0);
--Testcase 609:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 610:
DELETE FROM width_bucket_tbl;
--Testcase 611:
INSERT INTO width_bucket_tbl VALUES (5.0, 3.0, 4.0, -5);
--Testcase 612:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 613:
DELETE FROM width_bucket_tbl;
--Testcase 614:
INSERT INTO width_bucket_tbl VALUES (3.5, 3.0, 3.0, 888);
--Testcase 615:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 616:
DELETE FROM width_bucket_tbl;
--Testcase 617:
INSERT INTO width_bucket_tbl VALUES (5.0, 3.0, 4.0, 0);
--Testcase 618:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 619:
DELETE FROM width_bucket_tbl;
--Testcase 620:
INSERT INTO width_bucket_tbl VALUES (5.0, 3.0, 4.0, -5);
--Testcase 621:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 622:
DELETE FROM width_bucket_tbl;
--Testcase 623:
INSERT INTO width_bucket_tbl VALUES (3.5, 3.0, 3.0, 888);
--Testcase 624:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 625:
DELETE FROM width_bucket_tbl;
--Testcase 626:
INSERT INTO width_bucket_tbl VALUES ('NaN'::numeric, 3.0, 4.0, 888);
--Testcase 627:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 628:
DELETE FROM width_bucket_tbl;
--Testcase 629:
INSERT INTO width_bucket_tbl VALUES (0, 'NaN'::numeric, 4.0, 888);
--Testcase 630:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl;


-- normal operation
--Testcase 631:
CREATE FOREIGN TABLE width_bucket_test (
	operand_num numeric OPTIONS (key 'true'),
	operand_f8 float8
) SERVER duckdb_svr;

--COPY width_bucket_test (operand_num) FROM stdin;
--Testcase 507:
INSERT INTO width_bucket_test (operand_num) VALUES
(-5.2),
(-0.0000000001),
(0.000000000001),
(1),
(1.99999999999999),
(2),
(2.00000000000001),
(3),
(4),
(4.5),
(5),
(5.5),
(6),
(7),
(8),
(9),
(9.99999999999999),
(10),
(10.0000000000001);

--Testcase 508:
UPDATE width_bucket_test SET operand_f8 = operand_num::float8;

--Testcase 509:
SELECT
    operand_num,
    width_bucket(operand_num, 0, 10, 5) AS wb_1,
    width_bucket(operand_f8, 0, 10, 5) AS wb_1f,
    width_bucket(operand_num, 10, 0, 5) AS wb_2,
    width_bucket(operand_f8, 10, 0, 5) AS wb_2f,
    width_bucket(operand_num, 2, 8, 4) AS wb_3,
    width_bucket(operand_f8, 2, 8, 4) AS wb_3f,
    width_bucket(operand_num, 5.0, 5.5, 20) AS wb_4,
    width_bucket(operand_f8, 5.0, 5.5, 20) AS wb_4f,
    width_bucket(operand_num, -25, 25, 10) AS wb_5,
    width_bucket(operand_f8, -25, 25, 10) AS wb_5f
    FROM width_bucket_test;

-- for float8 only, check positive and negative infinity: we require
-- finite bucket bounds, but allow an infinite operand
--Testcase 510:
DELETE FROM width_bucket_tbl;
-- postgres does not support insert 'Infinity' and '-Infinity' as numeric.
--Testcase 632:
INSERT INTO width_bucket_tbl VALUES (0.0, 0.0, 5, 10);
--Testcase 633:
SELECT width_bucket(id1::float8, 'Infinity'::float8, id3, id4) FROM width_bucket_tbl;  -- error

--Testcase 511:
DELETE FROM width_bucket_tbl;
--Testcase 634:
INSERT INTO width_bucket_tbl VALUES (0.0, 5, 0.0, 20);
--Testcase 635:
SELECT width_bucket(id1::float8, id2, 'Infinity'::float8, id4) FROM width_bucket_tbl; -- error
--Testcase 512:
DELETE FROM width_bucket_tbl;
--Testcase 636:
INSERT INTO width_bucket_tbl VALUES (0.0, 1, 10, 10);
--Testcase 637:
SELECT width_bucket('Infinity'::float8, id2, id3, id4), width_bucket('-Infinity'::float8, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 638:
DROP FOREIGN TABLE width_bucket_test;
-- TO_CHAR()
--
--Testcase 513:
SELECT '' AS to_char_1, to_char(val, '9G999G999G999G999G999')
	FROM num_data;

--Testcase 514:
SELECT '' AS to_char_2, to_char(val, '9G999G999G999G999G999D999G999G999G999G999')
	FROM num_data;

--Testcase 515:
SELECT '' AS to_char_3, to_char(val, '9999999999999999.999999999999999PR')
	FROM num_data;

--Testcase 516:
SELECT '' AS to_char_4, to_char(val, '9999999999999999.999999999999999S')
	FROM num_data;

--Testcase 517:
SELECT '' AS to_char_5,  to_char(val, 'MI9999999999999999.999999999999999')     FROM num_data;
--Testcase 518:
SELECT '' AS to_char_6,  to_char(val, 'FMS9999999999999999.999999999999999')    FROM num_data;
--Testcase 519:
SELECT '' AS to_char_7,  to_char(val, 'FM9999999999999999.999999999999999THPR') FROM num_data;
--Testcase 520:
SELECT '' AS to_char_8,  to_char(val, 'SG9999999999999999.999999999999999th')   FROM num_data;
--Testcase 521:
SELECT '' AS to_char_9,  to_char(val, '0999999999999999.999999999999999')       FROM num_data;
--Testcase 522:
SELECT '' AS to_char_10, to_char(val, 'S0999999999999999.999999999999999')      FROM num_data;
--Testcase 523:
SELECT '' AS to_char_11, to_char(val, 'FM0999999999999999.999999999999999')     FROM num_data;
--Testcase 524:
SELECT '' AS to_char_12, to_char(val, 'FM9999999999999999.099999999999999') 	FROM num_data;
--Testcase 525:
SELECT '' AS to_char_13, to_char(val, 'FM9999999999990999.990999999999999') 	FROM num_data;
--Testcase 526:
SELECT '' AS to_char_14, to_char(val, 'FM0999999999999999.999909999999999') 	FROM num_data;
--Testcase 527:
SELECT '' AS to_char_15, to_char(val, 'FM9999999990999999.099999999999999') 	FROM num_data;
--Testcase 528:
SELECT '' AS to_char_16, to_char(val, 'L9999999999999999.099999999999999')	FROM num_data;
--Testcase 529:
SELECT '' AS to_char_17, to_char(val, 'FM9999999999999999.99999999999999')	FROM num_data;
--Testcase 530:
SELECT '' AS to_char_18, to_char(val, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
--Testcase 531:
SELECT '' AS to_char_19, to_char(val, 'FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
--Testcase 532:
SELECT '' AS to_char_20, to_char(val, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM num_data;
--Testcase 533:
SELECT '' AS to_char_21, to_char(val, '999999SG9999999999')			FROM num_data;
--Testcase 534:
SELECT '' AS to_char_22, to_char(val, 'FM9999999999999999.999999999999999')	FROM num_data;
--Testcase 535:
SELECT '' AS to_char_23, to_char(val, '9.999EEEE')				FROM num_data;

--Testcase 536:
DELETE FROM ceil_floor_round;
--Testcase 537:
INSERT INTO ceil_floor_round VALUES ('100'::numeric);
--Testcase 538:
SELECT '' AS to_char_24, to_char(a, 'FM999.9') FROM ceil_floor_round;
--Testcase 539:
SELECT '' AS to_char_25, to_char(a, 'FM999.') FROM ceil_floor_round;
--Testcase 540:
SELECT '' AS to_char_26, to_char(a, 'FM999') FROM ceil_floor_round;

-- Check parsing of literal text in a format string
--Testcase 541:
SELECT '' AS to_char_27, to_char(a, 'foo999') FROM ceil_floor_round;
--Testcase 542:
SELECT '' AS to_char_28, to_char(a, 'f\oo999') FROM ceil_floor_round;
--Testcase 543:
SELECT '' AS to_char_29, to_char(a, 'f\\oo999') FROM ceil_floor_round;
--Testcase 544:
SELECT '' AS to_char_30, to_char(a, 'f\"oo999') FROM ceil_floor_round;
--Testcase 545:
SELECT '' AS to_char_31, to_char(a, 'f\\"oo999') FROM ceil_floor_round;
--Testcase 546:
SELECT '' AS to_char_32, to_char(a, 'f"ool"999') FROM ceil_floor_round;
--Testcase 547:
SELECT '' AS to_char_33, to_char(a, 'f"\ool"999') FROM ceil_floor_round;
--Testcase 548:
SELECT '' AS to_char_34, to_char(a, 'f"\\ool"999') FROM ceil_floor_round;
--Testcase 549:
SELECT '' AS to_char_35, to_char(a, 'f"ool\"999') FROM ceil_floor_round;
--Testcase 550:
SELECT '' AS to_char_36, to_char(a, 'f"ool\\"999') FROM ceil_floor_round;

-- TO_NUMBER()
--
--Testcase 639:
create foreign table to_number_tbl (a text, id int options (key 'true')) server duckdb_svr;
SET lc_numeric = 'C';
--Testcase 640:
DELETE FROM to_number_tbl;
--Testcase 641:
INSERT INTO to_number_tbl VALUES ('-34,338,492');
--Testcase 642:
SELECT '' AS to_number_1,  to_number(a, '99G999G999') FROM to_number_tbl;

--Testcase 643:
DELETE FROM to_number_tbl;
--Testcase 644:
INSERT INTO to_number_tbl VALUES ('-34,338,492.654,878');
--Testcase 645:
SELECT '' AS to_number_2,  to_number(a, '99G999G999D999G999') FROM to_number_tbl;

--Testcase 646:
DELETE FROM to_number_tbl;
--Testcase 647:
INSERT INTO to_number_tbl VALUES ('<564646.654564>');
--Testcase 648:
SELECT '' AS to_number_3,  to_number(a, '999999.999999PR') FROM to_number_tbl;

--Testcase 649:
DELETE FROM to_number_tbl;
--Testcase 650:
INSERT INTO to_number_tbl VALUES ('0.00001-');
--Testcase 651:
SELECT '' AS to_number_4,  to_number(a, '9.999999S') FROM to_number_tbl;

--Testcase 652:
DELETE FROM to_number_tbl;
--Testcase 653:
INSERT INTO to_number_tbl VALUES ('5.01-');
--Testcase 654:
SELECT '' AS to_number_5,  to_number(a, 'FM9.999999S') FROM to_number_tbl;

--Testcase 655:
DELETE FROM to_number_tbl;
--Testcase 656:
INSERT INTO to_number_tbl VALUES ('5.01-');
--Testcase 657:
SELECT '' AS to_number_5,  to_number(a, 'FM9.999999MI') FROM to_number_tbl;

--Testcase 658:
DELETE FROM to_number_tbl;
--Testcase 659:
INSERT INTO to_number_tbl VALUES ('5 4 4 4 4 8 . 7 8');
--Testcase 660:
SELECT '' AS to_number_7,  to_number(a, '9 9 9 9 9 9 . 9 9') FROM to_number_tbl;

--Testcase 661:
DELETE FROM to_number_tbl;
--Testcase 662:
INSERT INTO to_number_tbl VALUES ('.01');
--Testcase 663:
SELECT '' AS to_number_8,  to_number(a, 'FM9.99') FROM to_number_tbl;

--Testcase 664:
DELETE FROM to_number_tbl;
--Testcase 665:
INSERT INTO to_number_tbl VALUES ('.0');
--Testcase 666:
SELECT '' AS to_number_9,  to_number(a, '99999999.99999999') FROM to_number_tbl;

--Testcase 667:
DELETE FROM to_number_tbl;
--Testcase 668:
INSERT INTO to_number_tbl VALUES ('0');
--Testcase 669:
SELECT '' AS to_number_10, to_number(a, '99.99') FROM to_number_tbl;

--Testcase 670:
DELETE FROM to_number_tbl;
--Testcase 671:
INSERT INTO to_number_tbl VALUES ('.-01');
--Testcase 672:
SELECT '' AS to_number_11, to_number(a, 'S99.99') FROM to_number_tbl;

--Testcase 673:
DELETE FROM to_number_tbl;
--Testcase 674:
INSERT INTO to_number_tbl VALUES ('.01-');
--Testcase 675:
SELECT '' AS to_number_12, to_number(a, '99.99S') FROM to_number_tbl;

--Testcase 676:
DELETE FROM to_number_tbl;
--Testcase 677:
INSERT INTO to_number_tbl VALUES (' . 0 1-');
--Testcase 678:
SELECT '' AS to_number_13, to_number(a, ' 9 9 . 9 9 S') FROM to_number_tbl;

--Testcase 679:
DELETE FROM to_number_tbl;
--Testcase 680:
INSERT INTO to_number_tbl VALUES ('34,50');
--Testcase 681:
SELECT '' AS to_number_14, to_number(a,'999,99') FROM to_number_tbl;

--Testcase 682:
DELETE FROM to_number_tbl;
--Testcase 683:
INSERT INTO to_number_tbl VALUES ('123,000');
--Testcase 684:
SELECT '' AS to_number_15, to_number(a,'999G') FROM to_number_tbl;

--Testcase 685:
DELETE FROM to_number_tbl;
--Testcase 686:
INSERT INTO to_number_tbl VALUES ('123456');
--Testcase 687:
SELECT '' AS to_number_16, to_number(a,'999G999') FROM to_number_tbl;

--Testcase 688:
DELETE FROM to_number_tbl;
--Testcase 689:
INSERT INTO to_number_tbl VALUES ('$1234.56');
--Testcase 690:
SELECT '' AS to_number_17, to_number(a,'L9,999.99') FROM to_number_tbl;

--Testcase 691:
DELETE FROM to_number_tbl;
--Testcase 692:
INSERT INTO to_number_tbl VALUES ('$1234.56');
--Testcase 693:
SELECT '' AS to_number_18, to_number(a,'L99,999.99') FROM to_number_tbl;

--Testcase 694:
DELETE FROM to_number_tbl;
--Testcase 695:
INSERT INTO to_number_tbl VALUES ('$1,234.56');
--Testcase 696:
SELECT '' AS to_number_19, to_number(a,'L99,999.99') FROM to_number_tbl;

--Testcase 697:
DELETE FROM to_number_tbl;
--Testcase 698:
INSERT INTO to_number_tbl VALUES ('1234.56');
--Testcase 699:
SELECT '' AS to_number_20, to_number(a,'L99,999.99') FROM to_number_tbl;

--Testcase 700:
DELETE FROM to_number_tbl;
--Testcase 701:
INSERT INTO to_number_tbl VALUES ('1,234.56');
--Testcase 702:
SELECT '' AS to_number_21, to_number(a,'L99,999.99') FROM to_number_tbl;

--Testcase 703:
DELETE FROM to_number_tbl;
--Testcase 704:
INSERT INTO to_number_tbl VALUES ('42nd');
--Testcase 705:
SELECT '' AS to_number_22, to_number(a, '99th') FROM to_number_tbl;

RESET lc_numeric;
--
-- Input syntax
--

--Testcase 706:
CREATE FOREIGN TABLE num_input_test (n1 numeric) SERVER duckdb_svr;

-- good inputs
--Testcase 551:
INSERT INTO num_input_test(n1) VALUES (' 123');
--Testcase 552:
INSERT INTO num_input_test(n1) VALUES ('   3245874    ');
--Testcase 553:
INSERT INTO num_input_test(n1) VALUES ('  -93853');
--Testcase 554:
INSERT INTO num_input_test(n1) VALUES ('555.50');
--Testcase 555:
INSERT INTO num_input_test(n1) VALUES ('-555.50');
--Testcase 556:
INSERT INTO num_input_test(n1) VALUES ('NaN ');
--Testcase 557:
INSERT INTO num_input_test(n1) VALUES ('        nan');

-- bad inputs
--Testcase 558:
INSERT INTO num_input_test(n1) VALUES ('     ');
--Testcase 559:
INSERT INTO num_input_test(n1) VALUES ('   1234   %');
--Testcase 560:
INSERT INTO num_input_test(n1) VALUES ('xyz');
--Testcase 561:
INSERT INTO num_input_test(n1) VALUES ('- 1234');
--Testcase 562:
INSERT INTO num_input_test(n1) VALUES ('5 . 0');
--Testcase 563:
INSERT INTO num_input_test(n1) VALUES ('5. 0   ');
--Testcase 564:
INSERT INTO num_input_test(n1) VALUES ('');
--Testcase 565:
INSERT INTO num_input_test(n1) VALUES (' N aN ');

--Testcase 566:
SELECT * FROM num_input_test;

--
-- Test some corner cases for multiplication
--
--Testcase 707:
CREATE FOREIGN TABLE num_tmp (n1 numeric, n2 numeric, id int options (key 'true')) SERVER duckdb_svr;
--Testcase 708:
INSERT INTO num_tmp VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 709:
SELECT n1 * n2 FROM num_tmp;

--Testcase 710:
DELETE FROM num_tmp;
--Testcase 711:
INSERT INTO num_tmp VALUES (4789999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 712:
SELECT n1 * n2 FROM num_tmp;

--Testcase 713:
DELETE FROM num_tmp;
--Testcase 714:
INSERT INTO num_tmp VALUES (4770999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 715:
SELECT n1 * n2 FROM num_tmp;

--Testcase 716:
DELETE FROM num_tmp;
--Testcase 717:
INSERT INTO num_tmp VALUES (4769999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 718:
SELECT n1 * n2 FROM num_tmp;

--
-- Test some corner cases for division
--
--Testcase 719:
DELETE FROM num_tmp;
--Testcase 720:
INSERT INTO num_tmp VALUES (999999999999999999999, 1000000000000000000000);
--Testcase 721:
SELECT n1::numeric / n2 FROM num_tmp;

--Testcase 722:
DELETE FROM num_tmp;
--Testcase 723:
INSERT INTO num_tmp VALUES (999999999999999999999, 1000000000000000000000);
--Testcase 724:
SELECT div(n1::numeric, n2) FROM num_tmp;
--Testcase 725:
SELECT mod(n1::numeric, n2) FROM num_tmp;
--Testcase 726:
SELECT div(-n1::numeric, n2) FROM num_tmp;
--Testcase 727:
SELECT mod(-n1::numeric, n2) FROM num_tmp;
--Testcase 728:
select div(-n1::numeric,n2)*n2 + mod(-n1::numeric,n2) FROM num_tmp;

--Testcase 729:
DELETE FROM num_tmp;
--Testcase 730:
INSERT INTO num_tmp VALUES (70.0,70);
--Testcase 731:
select mod (n1, n2) FROM num_tmp;
--Testcase 732:
select div (n1, n2) FROM num_tmp;
--Testcase 733:
select n1 / n2 FROM num_tmp;

--Testcase 734:
DELETE FROM num_tmp;
--Testcase 735:
INSERT INTO num_tmp VALUES (12345678901234567890, 123);
--Testcase 736:
select n1 % n2 FROM num_tmp;
--Testcase 737:
select n1 / n2 FROM num_tmp;
--Testcase 738:
select div(n1, n2) FROM num_tmp;
--Testcase 739:
select div(n1, n2) * n2 + n1 % n2 FROM num_tmp;

--
-- Test some corner cases for square root
--
--Testcase 740:
DELETE FROM num_tmp;
--Testcase 741:
INSERT INTO num_tmp VALUES (1.000000000000003::numeric);
--Testcase 742:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 743:
DELETE FROM num_tmp;
--Testcase 744:
INSERT INTO num_tmp VALUES (1.000000000000004::numeric);
--Testcase 745:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 746:
DELETE FROM num_tmp;
--Testcase 747:
INSERT INTO num_tmp VALUES (96627521408608.56340355805::numeric);
--Testcase 748:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 749:
DELETE FROM num_tmp;
--Testcase 750:
INSERT INTO num_tmp VALUES (96627521408608.56340355806::numeric);
--Testcase 751:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 752:
DELETE FROM num_tmp;
--Testcase 753:
INSERT INTO num_tmp VALUES (515549506212297735.073688290367::numeric);
--Testcase 754:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 755:
DELETE FROM num_tmp;
--Testcase 756:
INSERT INTO num_tmp VALUES (515549506212297735.073688290368::numeric);
--Testcase 757:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 758:
DELETE FROM num_tmp;
--Testcase 759:
INSERT INTO num_tmp VALUES (8015491789940783531003294973900306::numeric);
--Testcase 760:
SELECT sqrt(n1) FROM num_tmp;

--Testcase 761:
DELETE FROM num_tmp;
--Testcase 762:
INSERT INTO num_tmp VALUES (8015491789940783531003294973900307::numeric);
--Testcase 763:
SELECT sqrt(n1) FROM num_tmp;

--
-- Test code path for raising to integer powers
--
--Testcase 764:
DELETE FROM num_tmp;
--Testcase 765:
INSERT INTO num_tmp VALUES (10.0, -2147483648);
--Testcase 766:
SELECT n1 ^ n2 as rounds_to_zero FROM num_tmp;

--Testcase 767:
DELETE FROM num_tmp;
--Testcase 768:
INSERT INTO num_tmp VALUES (10.0, -2147483647);
--Testcase 769:
SELECT n1 ^ n2 as rounds_to_zero FROM num_tmp;

--Testcase 770:
DELETE FROM num_tmp;
--Testcase 771:
INSERT INTO num_tmp VALUES (10.0, 2147483647);
--Testcase 772:
SELECT n1 ^ n2 as overflows FROM num_tmp;

--Testcase 773:
DELETE FROM num_tmp;
--Testcase 774:
INSERT INTO num_tmp VALUES (117743296169.0, -1000000000);
--Testcase 775:
SELECT n1 ^ n2 as overflows FROM num_tmp;

-- cases that used to return inaccurate results

--Testcase 776:
DELETE FROM num_tmp;
--Testcase 777:
INSERT INTO num_tmp VALUES (3.789, 21);
--Testcase 778:
select n1 ^ n2 FROM num_tmp;

--Testcase 779:
DELETE FROM num_tmp;
--Testcase 780:
INSERT INTO num_tmp VALUES (3.789, 35);
--Testcase 781:
select n1 ^ n2 FROM num_tmp;

--Testcase 782:
DELETE FROM num_tmp;
--Testcase 783:
INSERT INTO num_tmp VALUES (1.2, 345);
--Testcase 784:
select n1 ^ n2 FROM num_tmp;

--Testcase 785:
DELETE FROM num_tmp;
--Testcase 786:
INSERT INTO num_tmp VALUES (0.12, (-20));
--Testcase 787:
select n1 ^ n2 FROM num_tmp;

-- cases that used to error out
--Testcase 788:
DELETE FROM num_tmp;
--Testcase 789:
INSERT INTO num_tmp VALUES (0.12, (-25));
--Testcase 790:
select n1 ^ n2 FROM num_tmp;

--Testcase 791:
DELETE FROM num_tmp;
--Testcase 792:
INSERT INTO num_tmp VALUES (0.5678, (-85));
--Testcase 793:
select n1 ^ n2 FROM num_tmp;

--
-- Tests for raising to non-integer powers
--

-- special cases
--Testcase 794:
DELETE FROM num_tmp;
--Testcase 795:
INSERT INTO num_tmp VALUES (0.0, 0.0);
--Testcase 796:
select n1 ^ n2 FROM num_tmp;

--Testcase 797:
DELETE FROM num_tmp;
--Testcase 798:
INSERT INTO num_tmp VALUES ((-12.34), 0.0);
--Testcase 799:
select n1 ^ n2 FROM num_tmp;

--Testcase 800:
DELETE FROM num_tmp;
--Testcase 801:
INSERT INTO num_tmp VALUES (12.34, 0.0);
--Testcase 802:
select n1 ^ n2 FROM num_tmp;

--Testcase 803:
DELETE FROM num_tmp;
--Testcase 804:
INSERT INTO num_tmp VALUES (0.0, 12.34);
--Testcase 805:
select n1 ^ n2 FROM num_tmp;

-- NaNs
--Testcase 806:
DELETE FROM num_tmp;
--Testcase 807:
INSERT INTO num_tmp VALUES ('NaN'::numeric, 'NaN'::numeric);
--Testcase 808:
select n1 ^ n2 FROM num_tmp;

--Testcase 809:
DELETE FROM num_tmp;
--Testcase 810:
INSERT INTO num_tmp VALUES ('NaN'::numeric, 0);
--Testcase 811:
select n1 ^ n2 FROM num_tmp;

--Testcase 812:
DELETE FROM num_tmp;
--Testcase 813:
INSERT INTO num_tmp VALUES ('NaN'::numeric, 1);
--Testcase 814:
select n1 ^ n2 FROM num_tmp;

--Testcase 815:
DELETE FROM num_tmp;
--Testcase 816:
INSERT INTO num_tmp VALUES (0, 'NaN'::numeric);
--Testcase 817:
select n1 ^ n2 FROM num_tmp;

--Testcase 818:
DELETE FROM num_tmp;
--Testcase 819:
INSERT INTO num_tmp VALUES (1, 'NaN'::numeric);
--Testcase 820:
select n1 ^ n2 FROM num_tmp;

-- invalid inputs
--Testcase 821:
DELETE FROM num_tmp;
--Testcase 822:
INSERT INTO num_tmp VALUES (0.0, (-12.34));
--Testcase 823:
select n1 ^ n2 FROM num_tmp;

--Testcase 824:
DELETE FROM num_tmp;
--Testcase 825:
INSERT INTO num_tmp VALUES ((-12.34), 1.2);
--Testcase 826:
select n1 ^ n2 FROM num_tmp;

-- cases that used to generate inaccurate results
--Testcase 827:
DELETE FROM num_tmp;
--Testcase 828:
INSERT INTO num_tmp VALUES (32.1, 9.8);
--Testcase 829:
select n1 ^ n2 FROM num_tmp;

--Testcase 830:
DELETE FROM num_tmp;
--Testcase 831:
INSERT INTO num_tmp VALUES (32.1, (-9.8));
--Testcase 832:
select n1 ^ n2 FROM num_tmp;

--Testcase 833:
DELETE FROM num_tmp;
--Testcase 834:
INSERT INTO num_tmp VALUES (12.3, 45.6);
--Testcase 835:
select n1 ^ n2 FROM num_tmp;

--Testcase 836:
DELETE FROM num_tmp;
--Testcase 837:
INSERT INTO num_tmp VALUES (12.3, (-45.6));
--Testcase 838:
select n1 ^ n2 FROM num_tmp;

-- big test
--Testcase 839:
DELETE FROM num_tmp;
--Testcase 840:
INSERT INTO num_tmp VALUES (1.234, 5678);
--Testcase 841:
select n1 ^ n2 FROM num_tmp;

--
-- Tests for EXP()
--

-- special cases
--Testcase 842:
DELETE FROM num_tmp;
--Testcase 843:
INSERT INTO num_tmp VALUES (0.0);
--Testcase 844:
select exp(n1) from num_tmp;

--Testcase 845:
DELETE FROM num_tmp;
--Testcase 846:
INSERT INTO num_tmp VALUES (1.0);
--Testcase 847:
select exp(n1) from num_tmp;

--Testcase 848:
DELETE FROM num_tmp;
--Testcase 849:
INSERT INTO num_tmp VALUES (1.0::numeric(71,70));
--Testcase 850:
select exp(n1) from num_tmp;

-- cases that used to generate inaccurate results
--Testcase 851:
DELETE FROM num_tmp;
--Testcase 852:
INSERT INTO num_tmp VALUES (32.999);
--Testcase 853:
select exp(n1) from num_tmp;

--Testcase 854:
DELETE FROM num_tmp;
--Testcase 855:
INSERT INTO num_tmp VALUES (-32.999);
--Testcase 856:
select exp(n1) from num_tmp;

--Testcase 857:
DELETE FROM num_tmp;
--Testcase 858:
INSERT INTO num_tmp VALUES (123.456);
--Testcase 859:
select exp(n1) from num_tmp;

--Testcase 860:
DELETE FROM num_tmp;
--Testcase 861:
INSERT INTO num_tmp VALUES (-123.456);
--Testcase 862:
select exp(n1) from num_tmp;

-- big test
--Testcase 863:
DELETE FROM num_tmp;
--Testcase 864:
INSERT INTO num_tmp VALUES (1234.5678);
--Testcase 865:
select exp(n1) from num_tmp;

--
-- Tests for generate_series
--
--Testcase 866:
DELETE FROM num_tmp;
--Testcase 867:
INSERT INTO num_tmp select * from generate_series(0.0::numeric, 4.0::numeric);
--Testcase 868:
SELECT n1 FROM num_tmp;

--Testcase 869:
DELETE FROM num_tmp;
--Testcase 870:
INSERT INTO num_tmp select * from generate_series(0.1::numeric, 4.0::numeric, 1.3::numeric);
--Testcase 871:
SELECT n1 FROM num_tmp;

--Testcase 872:
DELETE FROM num_tmp;
--Testcase 873:
INSERT INTO num_tmp select * from generate_series(4.0::numeric, -1.5::numeric, -2.2::numeric);
--Testcase 874:
SELECT n1 FROM num_tmp;

-- Trigger errors
--Testcase 875:
DELETE FROM num_tmp;
--Testcase 876:
INSERT INTO num_tmp select * from generate_series(-100::numeric, 100::numeric, 0::numeric);
--Testcase 877:
SELECT n1 FROM num_tmp;

--Testcase 878:
DELETE FROM num_tmp;
--Testcase 879:
INSERT INTO num_tmp select * from generate_series(-100::numeric, 100::numeric, 'nan'::numeric);
--Testcase 880:
SELECT n1 FROM num_tmp;

--Testcase 881:
DELETE FROM num_tmp;
--Testcase 882:
INSERT INTO num_tmp select * from generate_series('nan'::numeric, 100::numeric, 10::numeric);
--Testcase 883:
SELECT n1 FROM num_tmp;

--Testcase 884:
DELETE FROM num_tmp;
--Testcase 885:
INSERT INTO num_tmp select * from generate_series(0::numeric, 'nan'::numeric, 10::numeric);
--Testcase 886:
SELECT n2 FROM num_tmp;
-- Checks maximum, output is truncated
--Testcase 887:
DELETE FROM num_tmp;
--Testcase 888:
INSERT INTO num_tmp select (i / (10::numeric ^ 131071))::numeric(1,0)
        from generate_series(6 * (10::numeric ^ 131071),
                             9 * (10::numeric ^ 131071),
                             10::numeric ^ 131071) i;
--Testcase 889:
SELECT n1 FROM num_tmp;
                            
-- Check usage with variables
--Testcase 890:
DELETE FROM num_tmp;
--Testcase 891:
INSERT INTO num_tmp select * from generate_series(1::numeric, 3::numeric) i, generate_series(i,3) j;
--Testcase 892:
SELECT n1, n2 FROM num_tmp;

--Testcase 893:
DELETE FROM num_tmp;
--Testcase 894:
INSERT INTO num_tmp select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,i) j;
--Testcase 895:
SELECT n1, n2 FROM num_tmp;

--Testcase 896:
DELETE FROM num_tmp;
--Testcase 897:
INSERT INTO num_tmp select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,5,i) j;
--Testcase 898:
SELECT n1, n2 FROM num_tmp;

--
-- Tests for LN()
--

-- Invalid inputs
--Testcase 899:
DELETE FROM num_tmp;
--Testcase 900:
INSERT INTO num_tmp VALUES (-12.34);
--Testcase 901:
select ln(n1) from num_tmp;

--Testcase 902:
DELETE FROM num_tmp;
--Testcase 903:
INSERT INTO num_tmp VALUES (0.0);
--Testcase 904:
select ln(n1) from num_tmp;

-- Some random tests
--Testcase 905:
DELETE FROM num_tmp;
--Testcase 906:
INSERT INTO num_tmp VALUES (1.2345678e-28);
--Testcase 907:
select ln(n1) from num_tmp;

--Testcase 908:
DELETE FROM num_tmp;
--Testcase 909:
INSERT INTO num_tmp VALUES (0.0456789);
--Testcase 910:
select ln(n1) from num_tmp;

--Testcase 911:
DELETE FROM num_tmp;
--Testcase 912:
INSERT INTO num_tmp VALUES (0.349873948359354029493948309745709580730482050975);
--Testcase 913:
select ln(n1) from num_tmp;

--Testcase 914:
DELETE FROM num_tmp;
--Testcase 915:
INSERT INTO num_tmp VALUES (0.99949452);
--Testcase 916:
select ln(n1) from num_tmp;

--Testcase 917:
DELETE FROM num_tmp;
--Testcase 918:
INSERT INTO num_tmp VALUES (1.00049687395);
--Testcase 919:
select ln(n1) from num_tmp;

--Testcase 920:
DELETE FROM num_tmp;
--Testcase 921:
INSERT INTO num_tmp VALUES (1234.567890123456789);
--Testcase 922:
select ln(n1) from num_tmp;

--Testcase 923:
DELETE FROM num_tmp;
--Testcase 924:
INSERT INTO num_tmp VALUES (5.80397490724e5);
--Testcase 925:
select ln(n1) from num_tmp;

--Testcase 926:
DELETE FROM num_tmp;
--Testcase 927:
INSERT INTO num_tmp VALUES (9.342536355e34);
--Testcase 928:
select ln(n1) from num_tmp;

--
-- Tests for LOG() (base 10)
--

-- invalid inputs
--Testcase 929:
DELETE FROM num_tmp;
--Testcase 930:
INSERT INTO num_tmp VALUES (-12.34);
--Testcase 931:
select log(n1) from num_tmp;

--Testcase 932:
DELETE FROM num_tmp;
--Testcase 933:
INSERT INTO num_tmp VALUES (0.0);
--Testcase 934:
select log(n1) from num_tmp;

-- some random tests
--Testcase 935:
DELETE FROM num_tmp;
--Testcase 936:
INSERT INTO num_tmp VALUES (1.234567e-89);
--Testcase 937:
select log(n1) from num_tmp;

--Testcase 938:
DELETE FROM num_tmp;
--Testcase 939:
INSERT INTO num_tmp VALUES (3.4634998359873254962349856073435545);
--Testcase 940:
select log(n1) from num_tmp;

--Testcase 941:
DELETE FROM num_tmp;
--Testcase 942:
INSERT INTO num_tmp VALUES (9.999999999999999999);
--Testcase 943:
select log(n1) from num_tmp;

--Testcase 944:
DELETE FROM num_tmp;
--Testcase 945:
INSERT INTO num_tmp VALUES (10.00000000000000000);
--Testcase 946:
select log(n1) from num_tmp;

--Testcase 947:
DELETE FROM num_tmp;
--Testcase 948:
INSERT INTO num_tmp VALUES (10.00000000000000001);
--Testcase 949:
select log(n1) from num_tmp;

--Testcase 950:
DELETE FROM num_tmp;
--Testcase 951:
INSERT INTO num_tmp VALUES (590489.45235237);
--Testcase 952:
select log(n1) from num_tmp;

--
-- Tests for LOG() (arbitrary base)
--

-- invalid inputs
--Testcase 953:
DELETE FROM num_tmp;
--Testcase 954:
INSERT INTO num_tmp VALUES (-12.34, 56.78);
--Testcase 955:
select log(n1, n2) from num_tmp;

--Testcase 956:
DELETE FROM num_tmp;
--Testcase 957:
INSERT INTO num_tmp VALUES (-12.34, -56.78);
--Testcase 958:
select log(n1, n2) from num_tmp;

--Testcase 959:
DELETE FROM num_tmp;
--Testcase 960:
INSERT INTO num_tmp VALUES (12.34, -56.78);
--Testcase 961:
select log(n1, n2) from num_tmp;

--Testcase 962:
DELETE FROM num_tmp;
--Testcase 963:
INSERT INTO num_tmp VALUES (0.0, 12.34);
--Testcase 964:
select log(n1, n2) from num_tmp;

--Testcase 965:
DELETE FROM num_tmp;
--Testcase 966:
INSERT INTO num_tmp VALUES (12.34, 0.0);
--Testcase 967:
select log(n1, n2) from num_tmp;

--Testcase 968:
DELETE FROM num_tmp;
--Testcase 969:
INSERT INTO num_tmp VALUES (.0, 12.34);
--Testcase 970:
select log(n1, n2) from num_tmp;

-- some random tests
--Testcase 971:
DELETE FROM num_tmp;
--Testcase 972:
INSERT INTO num_tmp VALUES (1.23e-89, 6.4689e45);
--Testcase 973:
select log(n1, n2) from num_tmp;

--Testcase 974:
DELETE FROM num_tmp;
--Testcase 975:
INSERT INTO num_tmp VALUES (0.99923, 4.58934e34);
--Testcase 976:
select log(n1, n2) from num_tmp;

--Testcase 977:
DELETE FROM num_tmp;
--Testcase 978:
INSERT INTO num_tmp VALUES (1.000016, 8.452010e18);
--Testcase 979:
select log(n1, n2) from num_tmp;

--Testcase 980:
DELETE FROM num_tmp;
--Testcase 981:
INSERT INTO num_tmp VALUES (3.1954752e47, 9.4792021e-73);
--Testcase 982:
select log(n1, n2) from num_tmp;

--
-- Tests for scale()
--
--Testcase 983:
DELETE FROM num_tmp;
--Testcase 984:
INSERT INTO num_tmp VALUES (numeric 'NaN');
--Testcase 985:
select scale(n1) from num_tmp;

--Testcase 986:
DELETE FROM num_tmp;
--Testcase 987:
INSERT INTO num_tmp VALUES (NULL::numeric);
--Testcase 988:
select scale(n1) from num_tmp;

--Testcase 989:
DELETE FROM num_tmp;
--Testcase 990:
INSERT INTO num_tmp VALUES (1.12);
--Testcase 991:
select scale(n1) from num_tmp;

--Testcase 992:
DELETE FROM num_tmp;
--Testcase 993:
INSERT INTO num_tmp VALUES (0);
--Testcase 994:
select scale(n1) from num_tmp;

--Testcase 995:
DELETE FROM num_tmp;
--Testcase 996:
INSERT INTO num_tmp VALUES (0.00);
--Testcase 997:
select scale(n1) from num_tmp;

--Testcase 998:
DELETE FROM num_tmp;
--Testcase 999:
INSERT INTO num_tmp VALUES (1.12345);
--Testcase 1000:
select scale(n1) from num_tmp;

--Testcase 1001:
DELETE FROM num_tmp;
--Testcase 1002:
INSERT INTO num_tmp VALUES (110123.12475871856128);
--Testcase 1003:
select scale(n1) from num_tmp;

--Testcase 1004:
DELETE FROM num_tmp;
--Testcase 1005:
INSERT INTO num_tmp VALUES (-1123.12471856128);
--Testcase 1006:
select scale(n1) from num_tmp;

--Testcase 1007:
DELETE FROM num_tmp;
--Testcase 1008:
INSERT INTO num_tmp VALUES (-13.000000000000000);
--Testcase 1009:
select scale(n1) from num_tmp;

--
-- Tests for min_scale()
--
--Testcase 1010:
DELETE FROM num_tmp;
--Testcase 1011:
INSERT INTO num_tmp VALUES (numeric 'NaN');
--Testcase 1012:
select min_scale(n1) is NULL from num_tmp; -- should be true

--Testcase 1013:
DELETE FROM num_tmp;
--Testcase 1014:
INSERT INTO num_tmp VALUES (0);
--Testcase 1015:
select min_scale(n1) from num_tmp;                     -- no digits

--Testcase 1016:
DELETE FROM num_tmp;
--Testcase 1017:
INSERT INTO num_tmp VALUES (0.00);
--Testcase 1018:
select min_scale(n1) from num_tmp;                  -- no digits again

--Testcase 1019:
DELETE FROM num_tmp;
--Testcase 1020:
INSERT INTO num_tmp VALUES (1.0);
--Testcase 1021:
select min_scale(n1) from num_tmp;                   -- no scale

--Testcase 1022:
DELETE FROM num_tmp;
--Testcase 1023:
INSERT INTO num_tmp VALUES (1.1);
--Testcase 1024:
select min_scale(n1) from num_tmp;                   -- scale 1

--Testcase 1025:
DELETE FROM num_tmp;
--Testcase 1026:
INSERT INTO num_tmp VALUES (1.12);
--Testcase 1027:
select min_scale(n1) from num_tmp;                  -- scale 2

--Testcase 1028:
DELETE FROM num_tmp;
--Testcase 1029:
INSERT INTO num_tmp VALUES (1.123);
--Testcase 1030:
select min_scale(n1) from num_tmp;                 -- scale 3

--Testcase 1031:
DELETE FROM num_tmp;
--Testcase 1032:
INSERT INTO num_tmp VALUES (1.1234);
--Testcase 1033:
select min_scale(n1) from num_tmp;                -- scale 4, filled digit

--Testcase 1034:
DELETE FROM num_tmp;
--Testcase 1035:
INSERT INTO num_tmp VALUES (1.12345);
--Testcase 1036:
select min_scale(n1) from num_tmp;               -- scale 5, 2 NDIGITS

--Testcase 1037:
DELETE FROM num_tmp;
--Testcase 1038:
INSERT INTO num_tmp VALUES (1.1000);
--Testcase 1039:
select min_scale(n1) from num_tmp;                -- 1 pos in NDIGITS

--Testcase 1040:
DELETE FROM num_tmp;
--Testcase 1041:
INSERT INTO num_tmp VALUES (1e100);
--Testcase 1042:
select min_scale(n1) from num_tmp;                 -- very big number

--
-- Tests for trim_scale()
--
--Testcase 1043:
DELETE FROM num_tmp;
--Testcase 1044:
INSERT INTO num_tmp VALUES (numeric 'NaN');
--Testcase 1045:
select trim_scale(n1) from num_tmp;

--Testcase 1046:
DELETE FROM num_tmp;
--Testcase 1047:
INSERT INTO num_tmp VALUES (1.120);
--Testcase 1048:
select trim_scale(n1) from num_tmp;

--Testcase 1049:
DELETE FROM num_tmp;
--Testcase 1050:
INSERT INTO num_tmp VALUES (0);
--Testcase 1051:
select trim_scale(n1) from num_tmp;

--Testcase 1052:
DELETE FROM num_tmp;
--Testcase 1053:
INSERT INTO num_tmp VALUES (0.00);
--Testcase 1054:
select trim_scale(n1) from num_tmp;

--Testcase 1055:
DELETE FROM num_tmp;
--Testcase 1056:
INSERT INTO num_tmp VALUES (1.1234500);
--Testcase 1057:
select trim_scale(n1) from num_tmp;

--Testcase 1058:
DELETE FROM num_tmp;
--Testcase 1059:
INSERT INTO num_tmp VALUES (110123.12475871856128000);
--Testcase 1060:
select trim_scale(n1) from num_tmp;

--Testcase 1061:
DELETE FROM num_tmp;
--Testcase 1062:
INSERT INTO num_tmp VALUES (-123.124718561280000000);
--Testcase 1063:
select trim_scale(n1) from num_tmp;

--Testcase 1064:
DELETE FROM num_tmp;
--Testcase 1065:
INSERT INTO num_tmp VALUES (-13.00000000000000000000);
--Testcase 1066:
select trim_scale(n1) from num_tmp;

--Testcase 1067:
DELETE FROM num_tmp;
--Testcase 1068:
INSERT INTO num_tmp VALUES (1e100);
--Testcase 1069:
select trim_scale(n1) from num_tmp;

--
-- Tests for SUM()
--

-- cases that need carry propagation
--Testcase 1070:
DELETE FROM num_tmp;
--Testcase 1071:
INSERT INTO num_tmp SELECT * FROM generate_series(1, 100000);
--Testcase 1072:
SELECT SUM(9999::numeric) FROM num_tmp;
--Testcase 1073:
SELECT SUM((-9999)::numeric) FROM num_tmp;

--
-- Tests for GCD()
--
--Testcase 1074:
DELETE FROM num_tmp;
--Testcase 1075:
INSERT INTO num_tmp VALUES 
             (0::numeric, 0::numeric),
             (0::numeric, numeric 'NaN'),
             (0::numeric, 46375::numeric),
             (433125::numeric, 46375::numeric),
             (43312.5::numeric, 4637.5::numeric),
             (4331.250::numeric, 463.75000::numeric);
--Testcase 1076:
SELECT n1 as a, n2 as b, gcd(n1, n2), gcd(n1, -n2), gcd(-n2, n1), gcd(-n2, -n1) FROM num_tmp;
--
-- Tests for LCM()
--
--Testcase 1077:
DELETE FROM num_tmp;
--Testcase 1078:
INSERT INTO num_tmp VALUES 
             (0::numeric, 0::numeric),
             (0::numeric, numeric 'NaN'),
             (0::numeric, 13272::numeric),
             (13272::numeric, 13272::numeric),
             (423282::numeric, 13272::numeric),
             (42328.2::numeric, 1327.2::numeric),
             (4232.820::numeric, 132.72000::numeric);
--Testcase 1079:
SELECT n1 as a, n2 as b, lcm(n1, n2), lcm(n1, -n2), lcm(-n2, n1), lcm(-n2, -n1) FROM num_tmp;

--Testcase 1080:
DELETE FROM num_tmp;
--Testcase 1081:
INSERT INTO num_tmp VALUES (10::numeric, 131068); 
--Testcase 1082:
SELECT lcm(9999 * (n1)^n2 + (n1^n2 - 1), 2) FROM num_tmp; -- overflow


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
--Testcase 1083:
DROP SERVER duckdb_svr;
--Testcase 1084:
DROP EXTENSION duckdb_fdw CASCADE;
