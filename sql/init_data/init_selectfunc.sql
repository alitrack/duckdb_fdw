
DROP TABLE IF EXISTS s3;
CREATE TABLE s3(id text primary key, time timestamp, tag1 text, value1 float, value2 int, value3 float, value4 int, str1 text, str2 text);
INSERT INTO 's3' VALUES (0, DATETIME('1970-01-01 00:00:00'), 'a', 0.1, 100, -0.1, -100, '---XYZ---', '   XYZ   ');
INSERT INTO 's3' VALUES (1, DATETIME('1970-01-01 00:00:01'), 'a', 0.2, 100, -0.2, -100, '---XYZ---', '   XYZ   ');
INSERT INTO 's3' VALUES (2, DATETIME('1970-01-01 00:00:02'), 'a', 0.3, 100, -0.3, -100, '---XYZ---', '   XYZ   ');
INSERT INTO 's3' VALUES (3, DATETIME('1970-01-01 00:00:03'), 'b', 1.1, 200, -1.1, -200, '---XYZ---', '   XYZ   ');
INSERT INTO 's3' VALUES (4, DATETIME('1970-01-01 00:00:04'), 'b', 2.2, 200, -2.2, -200, '---XYZ---', '   XYZ   ');
INSERT INTO 's3' VALUES (5, DATETIME('1970-01-01 00:00:05'), 'b', 3.3, 200, -3.3, -200, '---XYZ---', '   XYZ   ');

analyze;
