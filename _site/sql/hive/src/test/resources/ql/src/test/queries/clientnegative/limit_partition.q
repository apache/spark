set hive.limit.query.max.table.partition=1;

explain select * from srcpart limit 1;
select * from srcpart limit 1;

explain select * from srcpart;
select * from srcpart;
