set hive.mapred.mode=strict;

SELECT x.* FROM SRCPART x WHERE key = '2008-04-08';
