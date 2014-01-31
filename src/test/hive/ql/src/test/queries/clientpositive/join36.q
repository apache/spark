set hive.mapjoin.numrows = 2;





CREATE TABLE tmp1(key INT, cnt INT);
CREATE TABLE tmp2(key INT, cnt INT);
CREATE TABLE dest_j1(key INT, value INT, val2 INT);

INSERT OVERWRITE TABLE tmp1
SELECT key, count(1) from src group by key;

INSERT OVERWRITE TABLE tmp2
SELECT key, count(1) from src group by key;

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, x.cnt, y.cnt
FROM tmp1 x JOIN tmp2 y ON (x.key = y.key);

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, x.cnt, y.cnt
FROM tmp1 x JOIN tmp2 y ON (x.key = y.key);

select * from dest_j1 x order by x.key;



