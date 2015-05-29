CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING);
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='2013-04-05');
ALTER TABLE exchange_part_test2 ADD PARTITION (ds='2013-04-05');
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

-- exchange_part_test1 table partition (ds='2013-04-05') already exists thus this query will fail
alter table exchange_part_test1 exchange partition (ds='2013-04-05') with table exchange_part_test2;
