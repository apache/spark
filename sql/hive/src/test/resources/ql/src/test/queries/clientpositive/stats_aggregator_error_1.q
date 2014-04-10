-- In this test, there is a dummy stats aggregator which throws an error when various
-- methods are called (as indicated by the parameter hive.test.dummystats.agregator)
-- Since stats need not be reliable (by setting hive.stats.reliable to false), the 
-- insert statements succeed. The insert statement succeeds even if the stats aggregator
-- is set to null, since stats need not be reliable.

create table tmptable(key string, value string);

set hive.stats.dbclass=dummy;
set hive.stats.default.publisher=org.apache.hadoop.hive.ql.stats.DummyStatsPublisher;
set hive.stats.default.aggregator=org.apache.hadoop.hive.ql.stats.DummyStatsAggregator;
set hive.stats.reliable=false;

set hive.test.dummystats.aggregator=connect;

INSERT OVERWRITE TABLE tmptable select * from src;
select count(1) from tmptable;

set hive.test.dummystats.aggregator=closeConnection;
INSERT OVERWRITE TABLE tmptable select * from src;
select count(1) from tmptable;

set hive.test.dummystats.aggregator=cleanUp;
INSERT OVERWRITE TABLE tmptable select * from src;
select count(1) from tmptable;

set hive.stats.default.aggregator="";
INSERT OVERWRITE TABLE tmptable select * from src;
select count(1) from tmptable;
