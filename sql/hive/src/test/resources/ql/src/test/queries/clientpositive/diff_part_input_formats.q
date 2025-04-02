-- Tests the case where a table is changed from sequence file to a RC file,
-- resulting in partitions in both file formats. If no valid partitions are
-- selected, then it should still use RC file for reading the dummy partition.
CREATE TABLE part_test (key STRING, value STRING) PARTITIONED BY (ds STRING) STORED AS SEQUENCEFILE;
ALTER TABLE part_test ADD PARTITION(ds='1');
ALTER TABLE part_test SET FILEFORMAT RCFILE;
ALTER TABLE part_test ADD PARTITION(ds='2');
SELECT count(1) FROM part_test WHERE ds='3';

