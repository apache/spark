-- HIVE-5122 locations for 2nd, 3rd... partition are ignored

CREATE TABLE add_part_test (key STRING, value STRING) PARTITIONED BY (ds STRING);

explain
ALTER TABLE add_part_test ADD IF NOT EXISTS
PARTITION (ds='2010-01-01') location 'A'
PARTITION (ds='2010-02-01') location 'B'
PARTITION (ds='2010-03-01')
PARTITION (ds='2010-04-01') location 'C';

ALTER TABLE add_part_test ADD IF NOT EXISTS
PARTITION (ds='2010-01-01') location 'A'
PARTITION (ds='2010-02-01') location 'B'
PARTITION (ds='2010-03-01')
PARTITION (ds='2010-04-01') location 'C';

from src TABLESAMPLE (1 ROWS)
insert into table add_part_test PARTITION (ds='2010-01-01') select 100,100
insert into table add_part_test PARTITION (ds='2010-02-01') select 200,200
insert into table add_part_test PARTITION (ds='2010-03-01') select 400,300
insert into table add_part_test PARTITION (ds='2010-04-01') select 500,400;

select * from add_part_test;
