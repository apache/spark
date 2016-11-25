CREATE TEMPORARY VIEW t1 AS SELECT * FROM VALUES 1, 2 AS t1(a);

CREATE TEMPORARY VIEW t2 AS SELECT * FROM VALUES 1 AS t2(b);

-- IN with correlated predicate
SELECT a FROM t1 WHERE a IN (SELECT b FROM t2 WHERE a=b);

-- NOT IN with correlated predicate
SELECT a FROM t1 WHERE a NOT IN (SELECT b FROM t2 WHERE a=b);

-- IN with correlated projection
SELECT a FROM t1 WHERE a IN (SELECT a FROM t2);

-- IN with correlated projection
SELECT a FROM t1 WHERE a NOT IN (SELECT a FROM t2);

-- IN with expressions
SELECT a FROM t1 WHERE a*1 IN (SELECT a%2 FROM t2);
