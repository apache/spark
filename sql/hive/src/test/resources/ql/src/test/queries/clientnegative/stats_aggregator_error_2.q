-- In this test, the stats aggregator does not exists.
-- If stats need not be reliable, the statement succeeds. However, if stats are supposed
-- to be reliable (by setting hive.stats.reliable to true), the insert statement fails
-- because stats cannot be collected for this statement

create table tmptable(key string, value string);

set hive.stats.dbclass=custom;
set hive.stats.default.publisher=org.apache.hadoop.hive.ql.stats.DummyStatsPublisher;
set hive.stats.default.aggregator="";

set hive.stats.reliable=false;
INSERT OVERWRITE TABLE tmptable select * from src;

set hive.stats.reliable=true;
INSERT OVERWRITE TABLE tmptable select * from src;
