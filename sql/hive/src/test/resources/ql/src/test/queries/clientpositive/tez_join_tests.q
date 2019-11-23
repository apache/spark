explain
select * from (select b.key, b.value from src1 a left outer join src b on (a.key = b.key) order by b.key) x right outer join src c on (x.value = c.value) order by x.key;

select * from (select b.key, b.value from src1 a left outer join src b on (a.key = b.key) order by b.key) x right outer join src c on (x.value = c.value) order by x.key;
select * from (select b.key, b.value from src1 a left outer join src b on (a.key = b.key)) x right outer join src c on (x.value = c.value) order by x.key;
select * from src1 a left outer join src b on (a.key = b.key) right outer join src c on (a.value = c.value) order by a.key;
select * from src1 a left outer join src b on (a.key = b.key) left outer join src c on (a.value = c.value) order by a.key;
select * from src1 a left outer join src b on (a.key = b.key) join src c on (a.key = c.key);
select * from src1 a join src b on (a.key = b.key) join src c on (a.key = c.key);

select count(*) from src1 a join src b on (a.key = b.key) join src c on (a.key = c.key);

