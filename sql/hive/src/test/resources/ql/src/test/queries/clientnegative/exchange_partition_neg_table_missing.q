-- t1 does not exist and the query fails
alter table t1 exchange partition (ds='2013-04-05') with table t2;
