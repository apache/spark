create table ppr_test (key string) partitioned by (ds string);

insert overwrite table ppr_test partition(ds='2') select '2' from src limit 1;
insert overwrite table ppr_test partition(ds='22') select '22' from src limit 1;

select * from ppr_test where ds = '2';
select * from ppr_test where ds = '22';


create table ppr_test2 (key string) partitioned by (ds string, s string);
insert overwrite table ppr_test2 partition(ds='1', s='2') select '1' from src limit 1;
insert overwrite table ppr_test2 partition(ds='2', s='1') select '2' from src limit 1;

select * from ppr_test2 where s = '1';
select * from ppr_test2 where ds = '1';


create table ppr_test3 (key string) partitioned by (col string, ol string, l string);
insert overwrite table ppr_test3 partition(col='1', ol='2', l = '3') select '1' from src limit 1;
insert overwrite table ppr_test3 partition(col='1', ol='1', l = '2') select '2' from src limit 1;
insert overwrite table ppr_test3 partition(col='1', ol='2', l = '1') select '3' from src limit 1;

select * from ppr_test3 where l = '1';
select * from ppr_test3 where l = '2';
select * from ppr_test3 where ol = '1';
select * from ppr_test3 where ol = '2';
select * from ppr_test3 where col = '1';
select * from ppr_test3 where ol = '2' and l = '1';
select * from ppr_test3 where col='1' and ol = '2' and l = '1';
