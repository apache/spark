CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING);
SHOW PARTITIONS exchange_part_test1;

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='2013-04-05');
SHOW PARTITIONS exchange_part_test1;

-- exchange_part_test2 table does not exist thus this query will fail
alter table exchange_part_test1 exchange partition (ds='2013-04-05') with table exchange_part_test2;
