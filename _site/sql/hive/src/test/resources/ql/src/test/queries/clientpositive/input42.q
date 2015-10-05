explain extended
select * from srcpart a where a.ds='2008-04-08' order by a.key, a.hr;

select * from srcpart a where a.ds='2008-04-08' order by a.key, a.hr;


explain extended
select * from srcpart a where a.ds='2008-04-08' and key < 200 order by a.key, a.hr;

select * from srcpart a where a.ds='2008-04-08' and key < 200 order by a.key, a.hr;


explain extended
select * from srcpart a where a.ds='2008-04-08' and rand(100) < 0.1 order by a.key, a.hr;

select * from srcpart a where a.ds='2008-04-08' and rand(100) < 0.1 order by a.key, a.hr;
