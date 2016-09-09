-- UDTF forwards nothing, OUTER LV add null for that
explain
select * from src LATERAL VIEW OUTER explode(array()) C AS a limit 10;
select * from src LATERAL VIEW OUTER explode(array()) C AS a limit 10;

-- backward compatible (UDTF forwards something for OUTER LV)
explain
select * from src LATERAL VIEW OUTER explode(array(4,5)) C AS a limit 10;
select * from src LATERAL VIEW OUTER explode(array(4,5)) C AS a limit 10;

create table array_valued as select key, if (key > 300, array(value, value), null) as value from src;

explain
select * from array_valued LATERAL VIEW OUTER explode(value) C AS a limit 10;
select * from array_valued LATERAL VIEW OUTER explode(value) C AS a limit 10;
