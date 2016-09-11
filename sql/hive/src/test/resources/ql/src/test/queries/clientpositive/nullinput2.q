

create table nulltbl(key int) partitioned by (ds string);
select key from nulltbl where ds='101';

select count(1) from nulltbl where ds='101';


