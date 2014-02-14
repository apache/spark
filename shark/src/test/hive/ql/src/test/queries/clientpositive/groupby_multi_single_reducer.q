set hive.multigroupby.singlereducer=true;

CREATE TABLE dest_g2(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_g3(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_g4(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_h2(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_h3(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

SELECT * FROM dest_g2 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;
SELECT * FROM dest_g3 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;
SELECT * FROM dest_g4 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_h2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1), substr(src.key,2,1) LIMIT 10
INSERT OVERWRITE TABLE dest_h3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1), substr(src.key,2,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_h2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1), substr(src.key,2,1) LIMIT 10
INSERT OVERWRITE TABLE dest_h3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1), substr(src.key,2,1);

SELECT * FROM dest_g2 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;
SELECT * FROM dest_g3 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;
SELECT * FROM dest_g4 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;
SELECT * FROM dest_h2 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;
SELECT * FROM dest_h3 ORDER BY key ASC, c1 ASC, c2 ASC, c3 ASC, c4 ASC;

DROP TABLE dest_g2;
DROP TABLE dest_g3;
DROP TABLE dest_g4;
DROP TABLE dest_h2;
DROP TABLE dest_h3;
