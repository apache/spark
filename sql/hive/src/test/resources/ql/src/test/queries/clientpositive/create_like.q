



CREATE TABLE table1 (a STRING, b STRING) STORED AS TEXTFILE;
DESCRIBE FORMATTED table1;

CREATE TABLE table2 LIKE table1;
DESCRIBE FORMATTED table2;

CREATE TABLE IF NOT EXISTS table2 LIKE table1;

CREATE EXTERNAL TABLE IF NOT EXISTS table2 LIKE table1;

CREATE EXTERNAL TABLE IF NOT EXISTS table3 LIKE table1;
DESCRIBE FORMATTED table3;

INSERT OVERWRITE TABLE table1 SELECT key, value FROM src WHERE key = 86;
INSERT OVERWRITE TABLE table2 SELECT key, value FROM src WHERE key = 100;

SELECT * FROM table1;
SELECT * FROM table2;

CREATE EXTERNAL TABLE table4 (a INT) LOCATION '${system:test.src.data.dir}/files/ext_test';
CREATE EXTERNAL TABLE table5 LIKE table4 LOCATION '${system:test.src.data.dir}/files/ext_test';

SELECT * FROM table4;
SELECT * FROM table5;

DROP TABLE table5;
SELECT * FROM table4;
DROP TABLE table4;

CREATE EXTERNAL TABLE table4 (a INT) LOCATION '${system:test.src.data.dir}/files/ext_test';
SELECT * FROM table4;
