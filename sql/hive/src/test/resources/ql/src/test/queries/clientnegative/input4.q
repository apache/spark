set hive.mapred.mode=strict;

select * from srcpart a join
  (select b.key, count(1) as count from srcpart b where b.ds = '2008-04-08' and b.hr = '14' group by b.key) subq
  where a.ds = '2008-04-08' and a.hr = '11' limit 10;
