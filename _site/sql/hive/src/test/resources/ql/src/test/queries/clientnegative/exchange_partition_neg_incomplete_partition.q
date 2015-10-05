CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

ALTER TABLE exchange_part_test2 ADD PARTITION (ds='2013-04-05', hr='h1');
ALTER TABLE exchange_part_test2 ADD PARTITION (ds='2013-04-05', hr='h2');
SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;

-- for exchange_part_test1 the value of ds is not given and the value of hr is given, thus this query will fail
alter table exchange_part_test1 exchange partition (hr='h1') with table exchange_part_test2;
