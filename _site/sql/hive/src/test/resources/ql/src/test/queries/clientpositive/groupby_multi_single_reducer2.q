set hive.multigroupby.singlereducer=true;

CREATE TABLE dest_g2(key STRING, c1 INT) STORED AS TEXTFILE;
CREATE TABLE dest_g3(key STRING, c1 INT, c2 INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT src.key) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT src.key), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT src.key) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT src.key), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1);

SELECT * FROM dest_g2;
SELECT * FROM dest_g3;

DROP TABLE dest_g2;
DROP TABLE dest_g3;
