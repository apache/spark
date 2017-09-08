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
SELECT rand('1')
