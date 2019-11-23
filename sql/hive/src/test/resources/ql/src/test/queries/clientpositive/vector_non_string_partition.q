SET hive.vectorized.execution.enabled=true;
CREATE TABLE non_string_part(cint INT, cstring1 STRING, cdouble DOUBLE, ctimestamp1 TIMESTAMP) PARTITIONED BY (ctinyint tinyint) STORED AS ORC;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE non_string_part PARTITION(ctinyint) SELECT cint, cstring1, cdouble, ctimestamp1, ctinyint fROM alltypesorc 
WHERE ctinyint IS NULL AND cdouble IS NOT NULL ORDER BY cdouble;

SHOW PARTITIONS non_string_part;

EXPLAIN SELECT cint, ctinyint FROM non_string_part WHERE cint > 0 ORDER BY cint LIMIT 10;

SELECT cint, ctinyint FROM non_string_part WHERE cint > 0 ORDER BY cint LIMIT 10;

EXPLAIN SELECT cint, cstring1 FROM non_string_part WHERE cint > 0 ORDER BY cint, cstring1 LIMIT 10;

SELECT cint, cstring1 FROM non_string_part WHERE cint > 0 ORDER BY cint, cstring1 LIMIT 10;
