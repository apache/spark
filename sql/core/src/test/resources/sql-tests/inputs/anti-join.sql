-- SPARK-18597: Do not push down predicates to left hand side in an anti-join
CREATE OR REPLACE TEMPORARY VIEW tbl_a AS VALUES (1, 1), (2, 1), (3, 6) AS T(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW tbl_b AS VALUES 1 AS T(c1);

SELECT *
FROM   tbl_a
       LEFT ANTI JOIN tbl_b ON ((tbl_a.c1 = tbl_a.c2) IS NULL OR tbl_a.c1 = tbl_a.c2);
