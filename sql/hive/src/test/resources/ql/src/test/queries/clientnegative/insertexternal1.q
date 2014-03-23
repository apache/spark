set hive.insert.into.external.tables=false;


create external table texternal(key string, val string) partitioned by (insertdate string);

alter table texternal add partition (insertdate='2008-01-01') location 'pfile://${system:test.tmp.dir}/texternal/2008-01-01';
from src insert overwrite table texternal partition (insertdate='2008-01-01') select *;

