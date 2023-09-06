CREATE TABLE i1_tbl (
  t1a integer,
  t1b integer,
  t1c integer,
  t1d integer,
  t1e integer,
  t1f integer
) USING parquet;

CREATE TABLE i2_tbl (
  t2a integer,
  t2b integer,
  t2c integer,
  t2d integer,
  t2e integer,
  t2f integer
) USING parquet;

CREATE TABLE i3_tbl (
  t3a integer,
  t3b integer,
  t3c integer,
  t3d integer,
  t3e integer,
  t3f integer
) USING parquet;


select i3_tbl.* from (
    select * from i1_tbl left semi join (select * from i2_tbl where t2a > 1000) tmp2 on i1_tbl.t1a = tmp2.t2a 
) tmp1 join i3_tbl on tmp1.t1a = i3_tbl.t3a;


select * from i1_tbl join i2_tbl on t1a = t2a and t2a = t1b and t1b = t2b and t2b = t1c and t1c = t2c and t2c = t1d and t1d = t2d and t2d = t1e and t1e = t2e and t2e = t1f;

DROP TABLE i1_tbl;
DROP TABLE i2_tbl;
DROP TABLE i3_tbl;
