create table intable (b boolean, d double, f float, i int, l bigint, s string, t tinyint);
insert overwrite table intable select 0, 29098519.0, 1410.0, 996, 40408519555, "test_string", 12 from src limit 1;
select * from intable where d in (29098519.0) and f in (1410.0) and i in (996) and l in (40408519555) and s in ('test_string') and t in (12);
drop table intable;