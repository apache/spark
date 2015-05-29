

create table if not exists union2_t1(r string, c string, v array<string>);
create table if not exists union2_t2(s string, c string, v string);

explain
SELECT s.r, s.c, sum(s.v)
FROM (
  SELECT a.r AS r, a.c AS c, a.v AS v FROM union2_t1 a
  UNION ALL
  SELECT b.s AS r, b.c AS c, 0 + b.v AS v FROM union2_t2 b
) s
GROUP BY s.r, s.c;
