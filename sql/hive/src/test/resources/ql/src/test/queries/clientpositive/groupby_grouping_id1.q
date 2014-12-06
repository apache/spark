CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

SELECT key, val, GROUPING__ID from T1 group by key, val with cube;

SELECT GROUPING__ID, key, val from T1 group by key, val with rollup;

SELECT key, val, GROUPING__ID, CASE WHEN GROUPING__ID == 0 THEN "0" WHEN GROUPING__ID == 1 THEN "1" WHEN GROUPING__ID == 2 THEN "2" WHEN GROUPING__ID == 3 THEN "3" ELSE "nothing" END from T1 group by key, val with cube;

