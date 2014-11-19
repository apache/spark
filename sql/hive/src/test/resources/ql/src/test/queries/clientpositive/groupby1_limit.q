set mapred.reduce.tasks=31;

CREATE TABLE dest1(key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key LIMIT 5;

FROM src INSERT OVERWRITE TABLE dest1 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key ORDER BY src.key LIMIT 5;

SELECT dest1.* FROM dest1 ORDER BY dest1.key ASC , dest1.value ASC;
