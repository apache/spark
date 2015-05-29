set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION length;
DESCRIBE FUNCTION EXTENDED length;

CREATE TABLE dest1(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1 SELECT length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1 SELECT length(src1.value);
SELECT dest1.* FROM dest1;
DROP TABLE dest1;

-- Test with non-ascii characters. 
CREATE TABLE dest1(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1;
EXPLAIN SELECT length(dest1.name) FROM dest1;
SELECT length(dest1.name) FROM dest1;
