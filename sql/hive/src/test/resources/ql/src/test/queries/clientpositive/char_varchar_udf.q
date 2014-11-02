DROP TABLE IF EXISTS  char_varchar_udf;

CREATE TABLE char_varchar_udf (c char(8), vc varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '../../data/files/char_varchar_udf.txt' INTO TABLE char_varchar_udf;

SELECT ROUND(c, 2), ROUND(vc, 3) FROM char_varchar_udf;
SELECT AVG(c), AVG(vc), SUM(c), SUM(vc) FROM char_varchar_udf;

DROP TABLE char_varchar_udf;