-- rand with the seed 0
SELECT rand(0);
SELECT rand(cast(3 / 7 AS int));
SELECT rand(NULL);
SELECT rand(cast(NULL AS int));

-- rand unsupported data type
SELECT rand(1.0);

-- randn with the seed 0
SELECT randn(0L);
SELECT randn(cast(3 / 7 AS long));
SELECT randn(NULL);
SELECT randn(cast(NULL AS long));

-- randn unsupported data type
SELECT rand('1');

-- The uniform random number generation function supports generating random numbers within a
-- specified range. We use a seed of zero for these queries to keep tests deterministic.
SELECT uniform(0, 1, 0) AS result;
SELECT uniform(0, 10, 0) AS result;
SELECT uniform(0L, 10L, 0) AS result;
SELECT uniform(0, 10L, 0) AS result;
SELECT uniform(0, cast(10 as tinyint), 0) AS result;
SELECT uniform(0, cast(10 as smallint), 0) AS result;
SELECT uniform(0, 10S, 0) AS result;
SELECT uniform(10, 20, 0) AS result;
SELECT uniform(10.0F, 20.0F, 0) AS result;
SELECT uniform(cast(10 as decimal(10, 3)), cast(20 as decimal(10, 3)), 0) AS result;
SELECT uniform(cast(10 as decimal(10, 3)), cast(20 as decimal(11, 4)), 0) AS result;
SELECT uniform(10, cast(20 as decimal(10, 3)), 0) AS result;
SELECT uniform(cast(10 as decimal(10, 3)), 20, 0) AS result;
SELECT uniform(10.0D, 20.0D, CAST(3 / 7 AS LONG)) AS result;
SELECT uniform(10, 20.0F, 0) AS result;
SELECT uniform(10, 20, 0) AS result FROM VALUES (0), (1), (2) tab(col);
SELECT uniform(10, 20.0F) IS NOT NULL AS result;
SELECT uniform(-10L, 10L, 0) AS result;
SELECT uniform(-20L, -10L, 0) AS result;
SELECT uniform(-20L, -10L, -10) AS result;
SELECT uniform(NULL, 1, 0) AS result;
SELECT uniform(cast(NULL AS int), 1, 0) AS result;
SELECT uniform(cast(NULL AS float), 1, 0) AS result;
SELECT uniform(0, NULL, 0) AS result;
SELECT uniform(0, cast(NULL AS int), 0) AS result;
SELECT uniform(0, cast(NULL AS float), 0) AS result;
SELECT uniform(0, 1, NULL) AS result;
SELECT uniform(NULL, NULL, 0) AS result;
SELECT uniform(NULL, NULL, NULL) AS result;
-- Negative test cases for the uniform random number generator.
SELECT uniform(0, 1, cast(NULL as int)) AS result;
SELECT uniform(0, 1, cast(NULL as float)) AS result;
SELECT uniform(10, 20, col) AS result FROM VALUES (0), (1), (2) tab(col);
SELECT uniform(col, 10, 0) AS result FROM VALUES (0), (1), (2) tab(col);
SELECT uniform(10) AS result;
SELECT uniform(10, 20, 30, 40) AS result;
SELECT uniform(10.0F, 20.0F, 0.0F) AS result;
SELECT uniform(10.0F, 20.0F, 0.0D) AS result;
SELECT uniform(cast(10 as decimal(10, 3)), cast(20 as decimal(10, 3)), cast(0 as decimal(10, 3)));
SELECT uniform('abc', 10, 0) AS result;
SELECT uniform(0, 'def', 0) AS result;
SELECT uniform(0, 10, 'ghi') AS result;

-- The randstr random string generation function supports generating random strings within a
-- specified length. We use a seed of zero for most queries to keep tests deterministic.
SELECT randstr(1, 0) AS result;
SELECT randstr(5, 0) AS result;
SELECT randstr(10, 0) AS result;
SELECT randstr(10S, 0) AS result;
SELECT randstr(CAST(10 AS TINYINT), 0) AS result;
SELECT randstr(CAST(10 AS BIGINT), 0) AS result;
SELECT randstr(1.0F, 0) AS result;
SELECT randstr(1.0D, 0) AS result;
SELECT randstr(cast(1 AS DECIMAL(10, 2)), 0) AS result;
SELECT randstr(10, 0) AS result FROM VALUES (0), (1), (2) tab(col);
SELECT randstr(10) IS NOT NULL AS result;
SELECT randstr(1, -1) AS result;
-- Negative test cases for the randstr random number generator.
SELECT randstr(10L, 0) AS result;
SELECT randstr(10.0F, 0) AS result;
SELECT randstr(10.0D, 0) AS result;
SELECT randstr(NULL, 0) AS result;
SELECT randstr(0, NULL) AS result;
SELECT randstr(col, 0) AS result FROM VALUES (0), (1), (2) tab(col);
SELECT randstr(10, col) AS result FROM VALUES (0), (1), (2) tab(col);
SELECT randstr(10, 0, 1) AS result;
SELECT randstr(-1, 0) AS result;
SELECT randstr(10, "a") AS result FROM VALUES (0) tab(a);
SELECT randstr(10, 1.5) AS result FROM VALUES (0) tab(a);
