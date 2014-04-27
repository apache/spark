create database dtf3;
use dtf3; 

create table drop_table_failure_temp(col STRING) partitioned by (p STRING);

alter table drop_table_failure_temp add partition (p ='p1');
alter table drop_table_failure_temp add partition (p ='p2');
alter table drop_table_failure_temp add partition (p ='p3');

alter table drop_table_failure_temp partition (p ='p3') ENABLE NO_DROP;

drop table drop_table_failure_temp;
