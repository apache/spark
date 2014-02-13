DROP TABLE IF EXISTS timestamp_null;
CREATE TABLE timestamp_null (t1 TIMESTAMP);
LOAD DATA LOCAL INPATH '../data/files/test.dat' OVERWRITE INTO TABLE timestamp_null;

SELECT * FROM timestamp_null LIMIT 1;

SELECT t1 FROM timestamp_null LIMIT 1;
