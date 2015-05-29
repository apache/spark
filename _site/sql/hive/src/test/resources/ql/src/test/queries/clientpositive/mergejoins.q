create table a (val1 int, val2 int);
create table b (val1 int, val2 int);
create table c (val1 int, val2 int);
create table d (val1 int, val2 int);
create table e (val1 int, val2 int);

explain select * from a join b on a.val1=b.val1 join c on a.val1=c.val1 join d on a.val1=d.val1 join e on a.val2=e.val2;

--HIVE-3070 filter on outer join condition removed while merging join tree
explain select * from src a join src b on a.key=b.key left outer join src c on b.key=c.key and b.key<10;
