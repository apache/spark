CACHE TABLE cached_cte AS WITH cte1 AS ( SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2 AS id, 'Bob' AS name ), cte2 AS ( SELECT 1 AS id, 10 AS score UNION ALL SELECT 2 AS id, 20 AS score ) SELECT cte1.id, cte1.name, cte2.score FROM cte1 JOIN cte2 ON cte1.id = cte2.id;

SELECT * FROM cached_cte;

EXPLAIN EXTENDED SELECT * FROM cached_cte;
