drop table if exists decimal_1_1;

create table decimal_1_1 (d decimal(1,1)) stored as textfile;
load data local inpath '../../data/files/decimal_1_1.txt' into table decimal_1_1;
select * from decimal_1_1;

select d from decimal_1_1 order by d desc;

drop table decimal_1_1;
