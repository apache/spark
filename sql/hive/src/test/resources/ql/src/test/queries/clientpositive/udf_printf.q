use default;
-- Test printf() UDF

DESCRIBE FUNCTION printf;
DESCRIBE FUNCTION EXTENDED printf;

set hive.fetch.task.conversion=more;

EXPLAIN
SELECT printf("Hello World %d %s", 100, "days") FROM src tablesample (1 rows);

-- Test Primitive Types
SELECT printf("Hello World %d %s", 100, "days") FROM src tablesample (1 rows);
SELECT printf("All Type Test: %b, %c, %d, %e, %+10.4f, %g, %h, %s, %a", false, 65, 15000, 12.3400, 27183.240051, 2300.41, 50, "corret", 256.125) FROM src tablesample (1 rows);

-- Test NULL Values
SELECT printf("Color %s, String Null: %s, number1 %d, number2 %05d, Integer Null: %d, hex %#x, float %5.2f Double Null: %f\n", "red", NULL, 123456, 89, NULL, 255, 3.14159, NULL) FROM src tablesample (1 rows);

-- Test Timestamp
create table timestamp_udf (t timestamp);
from (select * from src tablesample (1 rows)) s
  insert overwrite table timestamp_udf
    select '2011-05-06 07:08:09.1234567';
select printf("timestamp: %s", t) from timestamp_udf;
drop table timestamp_udf;

-- Test Binary
CREATE TABLE binay_udf(key binary, value int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '9'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/string.txt' INTO TABLE binay_udf;
create table dest1 (key binary, value int);
insert overwrite table dest1 select transform(*) using 'cat' as key binary, value int from binay_udf;
select value, printf("format key: %s", key) from dest1;
drop table dest1;
drop table binary_udf;
