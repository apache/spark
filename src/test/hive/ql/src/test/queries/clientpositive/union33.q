set hive.groupby.skewindata=true;

-- This tests that a union all with a map only subquery on one side and a 
-- subquery involving two map reduce jobs on the other runs correctly.

CREATE TABLE test_src (key STRING, value STRING);

EXPLAIN INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, value FROM src 
	WHERE key = 0
UNION ALL
 	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
)a;
 
INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, value FROM src 
	WHERE key = 0
UNION ALL
 	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
)a;
 
SELECT COUNT(*) FROM test_src;
 
EXPLAIN INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
UNION ALL
 	SELECT key, value FROM src 
	WHERE key = 0
)a;
 
INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
UNION ALL
 	SELECT key, value FROM src 
	WHERE key = 0
)a;
 
SELECT COUNT(*) FROM test_src;
 