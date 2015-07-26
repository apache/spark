DESCRIBE FUNCTION reverse;
DESCRIBE FUNCTION EXTENDED reverse;

CREATE TABLE dest1(len STRING);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1 SELECT reverse(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1 SELECT reverse(src1.value);
SELECT dest1.* FROM dest1;
DROP TABLE dest1;

-- Test with non-ascii characters
-- kv4.txt contains the text 0xE982B5E993AE, which should be reversed to
-- 0xE993AEE982B5
CREATE TABLE dest1(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1;
SELECT count(1) FROM dest1 WHERE reverse(dest1.name) = _UTF-8 0xE993AEE982B5;
