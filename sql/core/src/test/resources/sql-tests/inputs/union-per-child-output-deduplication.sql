DROP TABLE IF EXISTS t1;
DROP VIEW  IF EXISTS v1;

CREATE TABLE t1 (col1 STRING, col2 STRING, col3 STRING);

CREATE VIEW v1 as SELECT * FROM t1;

SELECT *
FROM (
         SELECT col1, col2 FROM t1
         UNION
         SELECT col3, col2 FROM t1
         UNION
         SELECT col2, col2 FROM t1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM v1
         UNION
         SELECT col3, col2 FROM v1
         UNION
         SELECT col2, col2 FROM v1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM t1
         UNION
         SELECT col3, col2 FROM t1
         UNION
         SELECT col2, col2 FROM v1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM t1
         UNION
         SELECT col3, col2 FROM v1
         UNION
         SELECT col2, col2 FROM t1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM v1
         UNION
         SELECT col3, col2 FROM t1
         UNION
         SELECT col2, col2 FROM t1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM v1
         UNION
         SELECT col3, col2 FROM v1
         UNION
         SELECT col2, col2 FROM t1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM v1
         UNION
         SELECT col3, col2 FROM t1
         UNION
         SELECT col2, col2 FROM v1
     );

SELECT *
FROM (
         SELECT col1, col2 FROM t1
         UNION
         SELECT col3, col2 FROM v1
         UNION
         SELECT col2, col2 FROM v1
     );

DROP VIEW v1;
DROP TABLE t1;
