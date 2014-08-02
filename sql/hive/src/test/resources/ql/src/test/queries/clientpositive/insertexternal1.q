

create table texternal(key string, val string) partitioned by (insertdate string);

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/texternal/temp;
dfs -rmr ${system:test.tmp.dir}/texternal;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/texternal/2008-01-01;

alter table texternal add partition (insertdate='2008-01-01') location 'pfile://${system:test.tmp.dir}/texternal/2008-01-01';
from src insert overwrite table texternal partition (insertdate='2008-01-01') select *;

select * from texternal where insertdate='2008-01-01';

dfs -rmr ${system:test.tmp.dir}/texternal;
