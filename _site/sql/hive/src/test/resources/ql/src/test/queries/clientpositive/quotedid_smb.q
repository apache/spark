
set hive.support.quoted.identifiers=column;


set hive.enforce.bucketing = true;  
set hive.enforce.sorting = true;  
create table src_b(`x+1` string, `!@#$%^&*()_q` string)  
clustered by (`!@#$%^&*()_q`) sorted by (`!@#$%^&*()_q`) into 2 buckets
;

insert overwrite table src_b
select * from src
;

create table src_b2(`x+1` string, `!@#$%^&*()_q` string)  
clustered by (`!@#$%^&*()_q`) sorted by (`!@#$%^&*()_q`) into 2 buckets
;

insert overwrite table src_b2
select * from src
;

set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

set hive.auto.convert.sortmerge.join.to.mapjoin=false;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

select a.`x+1`, a.`!@#$%^&*()_q`, b.`x+1`, b.`!@#$%^&*()_q`
from src_b a join src_b2 b on a.`!@#$%^&*()_q` = b.`!@#$%^&*()_q`
where a.`x+1` < '11'
;