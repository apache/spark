CREATE TABLE t1(col1 ARRAY<INT>, col2 INT);
INSERT INTO t1 VALUES (array(1, 2), 0), (array(3), 1), (array(), 2), (NULL, 3);

-- Generator (LATERAL VIEW) outputs that are not in the SELECT output must stay resolvable in
-- ORDER BY and HAVING via hidden output.
SELECT t1.* FROM t1 LATERAL VIEW EXPLODE(col1) AS g ORDER BY g;
SELECT g AS f, COUNT(*) AS cnt FROM t1 LATERAL VIEW EXPLODE(col1) unknown AS g WHERE col2 = 0
GROUP BY g ORDER BY g;
SELECT COUNT(*) AS cnt FROM t1 LATERAL VIEW EXPLODE(col1) AS g GROUP BY g HAVING g > 1;
SELECT t1.* FROM t1 LATERAL VIEW EXPLODE(col1) AS g WHERE g > 1 ORDER BY g;

-- LATERAL VIEW OUTER, POSEXPLODE with two outputs, a qualified generator output, multiple LATERAL
-- VIEWs, an expression over the hidden output, and INLINE.
SELECT t1.* FROM t1 LATERAL VIEW OUTER EXPLODE(col1) AS g ORDER BY g, col2;
SELECT col2 FROM t1 LATERAL VIEW POSEXPLODE(col1) AS p, g ORDER BY g DESC, p;
SELECT col2 FROM t1 LATERAL VIEW EXPLODE(col1) x AS g ORDER BY x.g;
SELECT col2 FROM t1 LATERAL VIEW EXPLODE(col1) AS g LATERAL VIEW EXPLODE(array(g, g * 10)) AS h
ORDER BY h;
SELECT col2 FROM t1 LATERAL VIEW EXPLODE(col1) AS g ORDER BY g * -1;
SELECT s FROM t1 LATERAL VIEW INLINE(array(struct(col2, 'x'))) AS i, s ORDER BY i;

-- Hidden output in aggregate functions in HAVING and ORDER BY.
SELECT col2 FROM t1 LATERAL VIEW EXPLODE(col1) AS g GROUP BY col2 HAVING max(g) > 2;
SELECT col2 FROM t1 LATERAL VIEW EXPLODE(col1) AS g GROUP BY col2 ORDER BY max(g) DESC;

-- In a subquery, and with DISTINCT, where the ORDER BY key has to be in the SELECT list.
SELECT * FROM (SELECT t1.* FROM t1 LATERAL VIEW EXPLODE(col1) AS g ORDER BY g);
SELECT DISTINCT col2 FROM t1 LATERAL VIEW EXPLODE(col1) AS g ORDER BY g;

DROP TABLE t1;
