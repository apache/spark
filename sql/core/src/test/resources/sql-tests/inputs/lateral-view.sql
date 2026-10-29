CREATE TABLE t1(col1 ARRAY<INT>, col2 INT);
INSERT INTO t1 VALUES (array(1, 2), 0), (array(3), 1);

-- Generator (LATERAL VIEW) outputs that are not in the SELECT output must stay resolvable in
-- ORDER BY and HAVING via hidden output.
SELECT t1.* FROM t1 LATERAL VIEW EXPLODE(col1) AS g ORDER BY g;
SELECT g AS f, COUNT(*) AS cnt FROM t1 LATERAL VIEW EXPLODE(col1) unknown AS g WHERE col2 = 0
GROUP BY g ORDER BY g;
SELECT COUNT(*) AS cnt FROM t1 LATERAL VIEW EXPLODE(col1) AS g GROUP BY g HAVING g > 1;
SELECT t1.* FROM t1 LATERAL VIEW EXPLODE(col1) AS g WHERE g > 1 ORDER BY g;

DROP TABLE t1;
