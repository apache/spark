--ONLY_IF spark
drop table if exists x;
drop table if exists y;

create table x(xm map<int, int>, x2 int) using parquet;
insert into x values (map(1, 2), 3), (map(1, 4), 5), (map(2, 3), 4), (map(5, 6), 7);
create table y(ym map<int, int>, y2 int) using parquet;
insert into y values (map(1, 2), 10), (map(1, 3), 20), (map(2, 3), 20), (map(8, 3), 20);

select * from x where (select sum(y2) from y where xm[1] = ym[1]) > 2;

-- exists, in, lateral
select * from x where exists (select * from y where xm[1] = ym[1]);
select * from x where exists (select * from y where xm[1] = ym[1] limit 1);
select * from x join lateral (select * from y where xm[1] = ym[1]);
select * from x join lateral (select * from y where xm[1] = ym[1] union all select * from y where xm[1] = ym[1] + 1);
select * from x join lateral (select * from y where xm[1] = ym[1] limit 1);
select * from x join lateral (select count(*) from y where xm[1] = ym[1] group by y2);
select * from x where xm[1] in (select ym[1] from y);
select * from x where xm[1] in (select sum(ym[1]) from y group by y2);

-- Multiple uses of the same outer expr
select * from x where (select sum(y2) from y where xm[1] = ym[1] and xm[1] >= 1) > 2;

-- Multiple different extracts from same map
select * from x where (select sum(y2) from y where xm[1] = ym[1] and xm[2] >= ym[2]) > 2;

-- Multiple subqueries using same outer expr
select * from x where (select sum(y2) from y where xm[1] = ym[1]) > 2 and (select count(y2) from y where xm[1] = ym[1]) < 3;

-- In project/aggregate/group-by
select * from x join lateral (select xm[1] - ym[1] from y);
select * from x join lateral (select xm[1], xm[1] as s1, xm[1] - ym[1] as s2 from y);
select * from x join lateral (select xm[1], sum(ym[1]), xm[1] - sum(ym[1]) from y group by xm[1]);

-- Complex key expressions
select * from x where (select sum(y2) from y where xm[x2] = ym[1]) > 2;
select * from x where (select sum(y2) from y where xm[x2+1] = ym[1]) > 2;
select * from x where (select sum(y2) from y where xm[x2+1] = ym[1] and xm[1+x2] = ym[2]) > 2; -- Two key expressions that are semantically equal

-- Cannot pull out expression because it references both outer and inner
select * from x where (select sum(y2) from y where xm[y2] = ym[1]) > 2;

-- Unsupported when disabled due to DomainJoin over map
set spark.sql.optimizer.pullOutNestedDataOuterRefExpressions.enabled = false;
select * from x where (select sum(y2) from y where xm[1] = ym[1]) > 2;
reset spark.sql.optimizer.rewriteNestedDataCorrelation.enabled;

drop table x;
drop table y;
