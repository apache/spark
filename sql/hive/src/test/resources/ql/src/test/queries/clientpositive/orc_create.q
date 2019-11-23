DROP TABLE orc_create;
DROP TABLE orc_create_complex;
DROP TABLE orc_create_staging;
DROP TABLE orc_create_people_staging;
DROP TABLE orc_create_people;

CREATE TABLE orc_create_staging (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':';

DESCRIBE FORMATTED orc_create_staging;

CREATE TABLE orc_create (key INT, value STRING)
   PARTITIONED BY (ds string)
   STORED AS ORC;

DESCRIBE FORMATTED orc_create;

DROP TABLE orc_create;

CREATE TABLE orc_create (key INT, value STRING)
   PARTITIONED BY (ds string);

DESCRIBE FORMATTED orc_create;

ALTER TABLE orc_create SET FILEFORMAT ORC;

DESCRIBE FORMATTED orc_create;

DROP TABLE orc_create;

set hive.default.fileformat=orc;

CREATE TABLE orc_create (key INT, value STRING)
   PARTITIONED BY (ds string);

set hive.default.fileformat=TextFile;

DESCRIBE FORMATTED orc_create;

CREATE TABLE orc_create_complex (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) STORED AS ORC;

DESCRIBE FORMATTED orc_create_complex;

LOAD DATA LOCAL INPATH '../../data/files/orc_create.txt' OVERWRITE INTO TABLE orc_create_staging;

SELECT * from orc_create_staging;

INSERT OVERWRITE TABLE orc_create_complex SELECT * FROM orc_create_staging;

SELECT * from orc_create_complex;
SELECT str from orc_create_complex;
SELECT mp from orc_create_complex;
SELECT lst from orc_create_complex;
SELECT strct from orc_create_complex;

CREATE TABLE orc_create_people_staging (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp,
  state string);

LOAD DATA LOCAL INPATH '../../data/files/orc_create_people.txt'
  OVERWRITE INTO TABLE orc_create_people_staging;

CREATE TABLE orc_create_people (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp)
PARTITIONED BY (state string)
STORED AS orc;

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE orc_create_people PARTITION (state)
  SELECT * FROM orc_create_people_staging;

SET hive.optimize.index.filter=true;
-- test predicate push down with partition pruning
SELECT COUNT(*) FROM orc_create_people where id < 10 and state = 'Ca';

-- test predicate push down
SELECT COUNT(*) FROM orc_create_people where id = 50;
SELECT COUNT(*) FROM orc_create_people where id between 10 and 20;
SELECT COUNT(*) FROM orc_create_people where id > 10 and id < 100;
SELECT COUNT(*) FROM orc_create_people where (id + 1) = 20;
SELECT COUNT(*) FROM orc_create_people where (id + 10) < 200;
SELECT COUNT(*) FROM orc_create_people where id < 30  or first_name = "Rafael";
SELECT COUNT(*) FROM orc_create_people 
   where length(substr(first_name, 1, 2)) <= 2 and last_name like '%';
SELECT COUNT(*) FROM orc_create_people where salary = 200.00;
SELECT COUNT(*) FROM orc_create_people WHERE start_date IS NULL;
SELECT COUNT(*) FROM orc_create_people WHERE YEAR(start_date) = 2014;

-- test predicate push down with partition pruning
SELECT COUNT(*) FROM orc_create_people where salary = 200.00 and state = 'Ca';

-- test predicate push down with no column projection
SELECT id, first_name, last_name, address
  FROM orc_create_people WHERE id > 90
  ORDER BY id, first_name, last_name;

DROP TABLE orc_create;
DROP TABLE orc_create_complex;
DROP TABLE orc_create_staging;
DROP TABLE orc_create_people_staging;
DROP TABLE orc_create_people;
