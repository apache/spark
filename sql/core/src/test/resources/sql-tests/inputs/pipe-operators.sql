create table t(x int, y string) using csv;
insert into t values (0, 'abc'), (1, 'def');

-- Selection operators.
table t
|> select 1 as x;

table t
|> select x, y;

table t
|> select x, y
|> select x + length(y) as z;

values (0), (1) tab(col)
|> select col * 2 as result;

(select * from t union all select * from t)
|> select x + length(y) as result;

table t
|> select sum(x) as result;

drop table t;