CREATE OR REPLACE TEMPORARY VIEW tbl_a AS VALUES (1, 1), (2, 1), (3, 6) AS T(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW tbl_b AS VALUES 1 AS T(c1);

-- SPARK-18597: Do not push down predicates to left hand side in an anti-join
SELECT *
FROM   tbl_a
       LEFT ANTI JOIN tbl_b ON ((tbl_a.c1 = tbl_a.c2) IS NULL OR tbl_a.c1 = tbl_a.c2);

-- SPARK-18614: Do not push down predicates on left table below ExistenceJoin
SELECT l.c1, l.c2
FROM   tbl_a l
WHERE  EXISTS (SELECT 1 FROM tbl_b r WHERE l.c1 = l.c2) OR l.c2 < 2;
