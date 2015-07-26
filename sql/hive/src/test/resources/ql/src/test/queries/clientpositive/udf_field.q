set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION field;
DESCRIBE FUNCTION EXTENDED field;

SELECT
  field("x", "a", "b", "c", "d"),
  field(NULL, "a", "b", "c", "d"),
  field(0, 1, 2, 3, 4)
FROM src tablesample (1 rows);

SELECT
  field("a", "a", "b", "c", "d"),
  field("b", "a", "b", "c", "d"),
  field("c", "a", "b", "c", "d"),
  field("d", "a", "b", "c", "d"),
  field("d", "a", "b", NULL, "d")
FROM src tablesample (1 rows);

SELECT
  field(1, 1, 2, 3, 4),
  field(2, 1, 2, 3, 4),
  field(3, 1, 2, 3, 4),
  field(4, 1, 2, 3, 4),
  field(4, 1, 2, NULL, 4)
FROM src tablesample (1 rows);


CREATE TABLE test_table(col1 STRING, col2 STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE test_table;

select col1,col2,
  field("66",col1),
  field("66",col1, col2),
  field("val_86",col1, col2),
  field(NULL, col1, col2),
  field(col1, 66, 88),
  field(col1, "66", "88"),
  field(col1, "666", "888"),
  field(col2, "66", "88"),
  field(col1, col2, col1),
  field(col1, col2, "66")
from test_table where col1="86" or col1="66";


CREATE TABLE test_table1(col1 int, col2 string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE test_table1;

select col1,col2,
  field(66,col1),
  field(66,col1, col2),
  field(86, col2, col1),
  field(86, col1, col1),
  field(86,col1,n,col2),
  field(NULL,col1,n,col2),
  field(col1, col2)
from (select col1, col2, NULL as n from test_table1 where col1=86 or col1=66) t;
