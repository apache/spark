CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='2013-04-05', hr='1');
ALTER TABLE exchange_part_test1 ADD PARTITION (ds='2013-04-05', hr='2');
ALTER TABLE exchange_part_test2 ADD PARTITION (ds='2013-04-05', hr='3');
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

-- exchange_part_test2 table partition (ds='2013-04-05', hr='3') already exists thus this query will fail
alter table exchange_part_test1 exchange partition (ds='2013-04-05') with table exchange_part_test2;
