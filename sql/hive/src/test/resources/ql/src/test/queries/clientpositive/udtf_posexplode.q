CREATE TABLE employees (
name STRING,
salary FLOAT,
subordinates ARRAY<STRING>,
deductions MAP<STRING, FLOAT>,
address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>);

LOAD DATA LOCAL INPATH '../../data/files/posexplode_data.txt' INTO TABLE employees;

SELECT
  name, pos, sub
FROM
  employees
LATERAL VIEW
  posexplode(subordinates) subView AS pos, sub;
