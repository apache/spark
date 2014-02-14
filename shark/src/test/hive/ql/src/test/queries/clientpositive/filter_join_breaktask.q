
CREATE TABLE filter_join_breaktask(key int, value string) partitioned by (ds string);

INSERT OVERWRITE TABLE filter_join_breaktask PARTITION(ds='2008-04-08')
SELECT key, value from src1;


EXPLAIN EXTENDED  
SELECT f.key, g.value 
FROM filter_join_breaktask f JOIN filter_join_breaktask m ON( f.key = m.key AND f.ds='2008-04-08' AND m.ds='2008-04-08' AND f.key is not null) 
JOIN filter_join_breaktask g ON(g.value = m.value AND g.ds='2008-04-08' AND m.ds='2008-04-08' AND m.value is not null AND m.value !='');

SELECT f.key, g.value 
FROM filter_join_breaktask f JOIN filter_join_breaktask m ON( f.key = m.key AND f.ds='2008-04-08' AND m.ds='2008-04-08' AND f.key is not null) 
JOIN filter_join_breaktask g ON(g.value = m.value AND g.ds='2008-04-08' AND m.ds='2008-04-08' AND m.value is not null AND m.value !='');

