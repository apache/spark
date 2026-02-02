
set hive.support.quoted.identifiers=column;

create table src_b3(`x+1` string, `!@#$%^&*()_q` string) ;

alter table src_b3 
clustered by (`!@#$%^&*()_q`) sorted by (`!@#$%^&*()_q`) into 2 buckets
;


-- alter partition
create table src_p3(`x+1` string, `y&y` string) partitioned by (`!@#$%^&*()_q` string);

insert overwrite table src_p3 partition(`!@#$%^&*()_q`='a') select * from src;
show partitions src_p3;

alter table src_p3 add if not exists partition(`!@#$%^&*()_q`='b');
show partitions src_p3;

alter table src_p3 partition(`!@#$%^&*()_q`='b') rename to partition(`!@#$%^&*()_q`='c');
show partitions src_p3;