
-- test analyze table compute statistiscs [noscan] on external table 
-- 1 test table
CREATE EXTERNAL TABLE anaylyze_external (a INT) LOCATION '${system:test.src.data.dir}/files/ext_test';
SELECT * FROM anaylyze_external;
analyze table anaylyze_external compute statistics;
describe formatted anaylyze_external;
analyze table anaylyze_external compute statistics noscan;
describe formatted anaylyze_external;
drop table anaylyze_external;

-- 2 test partition
-- prepare data
create table texternal(key string, val string) partitioned by (insertdate string);
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/texternal/2008-01-01;
alter table texternal add partition (insertdate='2008-01-01') location 'pfile://${system:test.tmp.dir}/texternal/2008-01-01';
from src insert overwrite table texternal partition (insertdate='2008-01-01') select *;
select count(*) from texternal where insertdate='2008-01-01';
-- create external table
CREATE EXTERNAL TABLE anaylyze_external (key string, val string) partitioned by (insertdate string) LOCATION "pfile://${system:test.tmp.dir}/texternal"; 
ALTER TABLE anaylyze_external ADD PARTITION (insertdate='2008-01-01') location 'pfile://${system:test.tmp.dir}/texternal/2008-01-01';
select count(*) from anaylyze_external where insertdate='2008-01-01';
-- analyze
analyze table anaylyze_external PARTITION (insertdate='2008-01-01') compute statistics;
describe formatted anaylyze_external PARTITION (insertdate='2008-01-01');
analyze table anaylyze_external PARTITION (insertdate='2008-01-01') compute statistics noscan;
describe formatted anaylyze_external PARTITION (insertdate='2008-01-01');
dfs -rmr ${system:test.tmp.dir}/texternal;
drop table anaylyze_external;



