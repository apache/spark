-- In this test, there is a dummy stats aggregator which throws an error when the
-- method connect is called (as indicated by the parameter hive.test.dummystats.aggregator)
-- If stats need not be reliable, the statement succeeds. However, if stats are supposed
-- to be reliable (by setting hive.stats.reliable to true), the insert statement fails
-- because stats cannot be collected for this statement

create table tmptable(key string, value string);

set hive.stats.dbclass=dummy;
set hive.stats.default.publisher=org.apache.hadoop.hive.ql.stats.DummyStatsPublisher;
set hive.stats.default.aggregator=org.apache.hadoop.hive.ql.stats.DummyStatsAggregator;
set hive.test.dummystats.aggregator=connect;

set hive.stats.reliable=false;
INSERT OVERWRITE TABLE tmptable select * from src;

set hive.stats.reliable=true;
INSERT OVERWRITE TABLE tmptable select * from src;
