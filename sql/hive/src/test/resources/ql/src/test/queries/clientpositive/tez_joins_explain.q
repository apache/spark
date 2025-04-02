explain
select * from (select b.key, b.value from src1 a left outer join src b on (a.key = b.key) order by b.key) x right outer join src c on (x.value = c.value) order by x.key;

select * from (select b.key, b.value from src1 a left outer join src b on (a.key = b.key) order by b.key) x right outer join src c on (x.value = c.value) order by x.key;

