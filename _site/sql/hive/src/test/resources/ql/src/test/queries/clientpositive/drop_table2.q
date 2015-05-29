SET hive.metastore.batch.retrieve.max=1;
create table if not exists temp(col STRING) partitioned by (p STRING);
alter table temp add if not exists partition (p ='p1');
alter table temp add if not exists partition (p ='p2');
alter table temp add if not exists partition (p ='p3');

show partitions temp;

drop table temp;

create table if not exists temp(col STRING) partitioned by (p STRING);

show partitions temp;

drop table temp;
