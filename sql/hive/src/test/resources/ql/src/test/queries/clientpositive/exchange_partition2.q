CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='2013-04-05', hr='1');
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

ALTER TABLE exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05', hr='1') WITH TABLE exchange_part_test2;
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;
