CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='2014-01-03', hr='1');
ALTER TABLE exchange_part_test2 ADD PARTITION (ds='2013-04-05', hr='1');
ALTER TABLE exchange_part_test2 ADD PARTITION (ds='2013-04-05', hr='2');
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

-- This will exchange both partitions hr=1 and hr=2
ALTER TABLE exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05') WITH TABLE exchange_part_test2;
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;
