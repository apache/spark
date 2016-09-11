CREATE TABLE dest_j1(key INT, cnt INT);

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

select * from dest_j1 x order by x.key;
