set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT DISTINCT substr(src.value,5,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT DISTINCT substr(src.value,5,1);

SELECT dest1.* FROM dest1;


