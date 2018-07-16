create temporary view t1 as select * from values
  ("t1a", 6S, 8, 10L, float(15.0), 20D),
  ("t1b", 8S, 16, 19L, float(17.0), 25D),
  ("t1a", 16S, 12, 21L, float(15.0), 20D),
  ("t1a", 16S, 12, 10L, float(15.0), 20D),
  ("t1c", 8S, 16, 19L, float(17.0), 25D),
  ("t1d", null, 16, 22L, float(17.0), 25D),
  ("t1d", null, 16, 19L, float(17.0), 25D),
  ("t1e", 10S, null, 25L, float(17.0), 25D)
  as t1(t1a, t1b, t1c, t1d, t1e, t1f);

select * from t1 DISTRIBUTE BY t1a SORT BY t1a;

select * from t1 RANGE PARTITION BY t1a SORT BY t1a;

select * from t1 RANGE PARTITION BY t1a SORT BY t1d desc;

select * from t1 DISTRIBUTE BY t1c SORT BY t1c;

select * from t1 RANGE PARTITION BY t1c SORT BY t1c;

select * from t1 RANGE PARTITION BY t1c SORT BY t1d desc;
