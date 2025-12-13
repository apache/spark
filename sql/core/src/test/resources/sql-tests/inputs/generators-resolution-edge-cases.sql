-- nested generator should fail
SELECT explode(explode(array(array(1, 2, 3))));

-- not top level generator should fail
SELECT 1 + explode(array(1, 2, 3));

-- multiple generators should work
SELECT explode(array(0, 1, 2)), explode(array(10, 20));

-- multiple generators' order is not fixed and depends on rule ordering
SELECT explode(array(sin(0), 1, 2)), explode(array(10, 20));

-- multiple generators in aggregate should fail
SELECT explode(array(1, 2)), explode(array(3, 4)), count(*);

-- multiple generators in project with multi-alias should work
SELECT explode(array(1, 2)) AS a, posexplode(array('x', 'y', 'z')) AS (pos, b);

-- generator with aggregate function as argument should work
SELECT explode(collect_list(id)) AS val FROM range(5);

-- generator with aggregate function and other aggregates should work
SELECT explode(collect_list(id)) AS val, count(*) AS cnt FROM range(3);

-- GROUP BY generator alias should fail
SELECT id, explode(array(1, 2, 3)) AS array_element
FROM (VALUES (1), (2)) AS t(id)
GROUP BY id, array_element;

-- posexplode_outer with null arrays via CASE WHEN should work
SELECT id, posexplode_outer(CASE WHEN id = 1 THEN array('a', 'b') ELSE null END) AS (pos, val)
FROM range(3);

-- generator in CTE with union
WITH cte AS (SELECT explode(array(1, 2, 3)) AS col)
SELECT * FROM cte
UNION ALL
SELECT * FROM cte;

-- generator in CTE with join
WITH cte AS (SELECT explode(array(1, 2)) AS col)
SELECT t1.col, t2.col FROM cte t1 JOIN cte t2 ON t1.col = t2.col;

-- generator with subquery
SELECT * FROM (SELECT explode(array(1, 2, 3)) AS val) t WHERE val > 1;

-- nested subqueries with generators
SELECT *
FROM (SELECT * FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS val) WHERE val > 1)
WHERE val < 5;

-- generator with GROUP BY ALL should respect generators arguments
SELECT explode(arr) AS package
FROM (VALUES(array('a', 'b'))) AS t(arr)
GROUP BY ALL;

-- generator with GROUP BY ALL should skip generators arguments with aggregate function
SELECT explode(collect_list(a)) AS val
FROM (VALUES ('a'), ('b')) AS t(a)
GROUP BY ALL;

-- generator with GROUP BY ALL should use only non-aggregate arguments for grouping
SELECT stack(2, id + 10L, count(val))
FROM (VALUES (1,'a'), (1,'b'), (2, 'c')) AS t(id, val)
GROUP BY ALL;

-- generator with GROUP BY ordinal should count generators arguments
SELECT id, explode(arr)
FROM (VALUES(42, array('a', 'b', 'c'))) AS t(id, arr)
GROUP BY id, 2;

-- generator's generated aliases should be visible for ordinals
SELECT stack(2, 'first', count(*), arr,
             'second', 0L, array('1', '2', '3')) AS (x1, x2, x3), id
FROM (VALUES (1, array('a', 'b'))) AS t(id, arr)
GROUP BY 2, 3;

-- generator's generated aliases should be visible in GROUP BY.
SELECT stack(2, 'first', count(*), arr,
             'second', 0L, array('1', '2', '3')) AS (x1, x2, x3), id
FROM (VALUES (1, array('a', 'b'))) AS t(id, arr)
GROUP BY _gen_input_3, id;

-- generator's generated aliases should not be visible in HAVING.
SELECT stack(2, 'first', count(*), arr,
             'second', 0L, array('1', '2', '3')) AS (x1, x2, x3), id
FROM (VALUES (1, array('a', 'b'))) AS t(id, arr)
GROUP BY _gen_input_3, id
HAVING size (_gen_input_3) > 0;

-- generator should be able to reference columns from hidden output
SELECT explode(array(t1.c1, t2.c1)) AS x1
FROM (VALUES (1), (2)) AS t1(c1)
         FULL OUTER JOIN (VALUES (2), (3)) AS t2(c1)
                         USING (c1);

-- multiple generators should be able to reference columns from hidden output
SELECT explode(array(t1.c1, t2.c1)) AS x1, explode(array(t1.c1)) AS x2
FROM (VALUES (1), (2), (3)) AS t1(c1)
         FULL OUTER JOIN (VALUES (2), (3), (4)) AS t2(c1)
                         USING (c1);

-- generator with multi-alias and column references should work
SELECT id, posexplode(arr) AS (index, value), index, value
FROM (VALUES (42, array('a', 'b', 'c')), (42, array('t'))) AS t(id, arr);

-- generator with LCA in argument should fail
SELECT array(1, 2, 3) as arr, explode(arr) as col;

-- generator with LCA in argument + aggregate should work
SELECT array(1, 2, 3) as arr, explode(arr) as col, count(*);

-- generator with LCA in argument + aggregate + LCA from generator output should fail
SELECT array(1, 2, 3) as arr, explode(arr) as col, count(*), col + 1 as col2;

-- generator output LCA + aggregate should fail
SELECT explode(array(1, 2, 3)) as col, col + 1 as col2, count(*);

-- generator with LCA in argument + generator output LCA should fail
SELECT array(1, 2, 3) as arr, explode(arr) as col, col + 1 as col2;

-- generator output LCA should work
SELECT explode(array(1, 2, 3)) as col, col + 1 as col2;

-- generator with window function resolves in order Generator -> Window
SELECT explode(array(1, 2, 3)) as col, count(*) OVER ();

-- generator with window function and aggregate together resolves in order Aggregate -> Window -> Generator
SELECT explode(array(1, 2, 3)), count(*) OVER (), count(*);
