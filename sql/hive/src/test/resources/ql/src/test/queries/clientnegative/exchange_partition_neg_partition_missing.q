CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING);
SHOW PARTITIONS exchange_part_test1;

-- exchange_part_test2 partition (ds='2013-04-05') does not exist thus this query will fail
alter table exchange_part_test1 exchange partition (ds='2013-04-05') with table exchange_part_test2;
