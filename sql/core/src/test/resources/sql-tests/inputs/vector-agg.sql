-- Tests for vector aggregation functions: vector_sum, vector_avg

-- Basic functionality tests

-- vector_sum: basic test with VALUES clause
SELECT vector_sum(col) FROM VALUES (array(1.0F, 2.0F, 3.0F)), (array(4.0F, 5.0F, 6.0F)) AS tab(col);

-- vector_avg: basic test with VALUES clause
SELECT vector_avg(col) FROM VALUES (array(1.0F, 2.0F, 3.0F)), (array(3.0F, 4.0F, 5.0F)) AS tab(col);

-- vector_sum: single vector returns the same vector
SELECT vector_sum(col) FROM VALUES (array(1.0F, 2.0F, 3.0F)) AS tab(col);

-- vector_avg: single vector returns the same vector
SELECT vector_avg(col) FROM VALUES (array(1.0F, 2.0F, 3.0F)) AS tab(col);

-- vector_sum: three vectors
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 1.0F)),
    (array(2.0F, 2.0F)),
    (array(3.0F, 3.0F)) AS tab(col);

-- vector_avg: three vectors - average is (1+2+3)/3 = 2
SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 1.0F)),
    (array(2.0F, 2.0F)),
    (array(3.0F, 3.0F)) AS tab(col);

-- Mathematical correctness

-- vector_sum: verify element-wise sum
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F)),
    (array(10.0F, 20.0F, 30.0F)) AS tab(col);

-- vector_avg: verify element-wise average
SELECT vector_avg(col) FROM VALUES
    (array(0.0F, 0.0F)),
    (array(10.0F, 20.0F)) AS tab(col);

-- vector_avg: verify with negative values
SELECT vector_avg(col) FROM VALUES
    (array(-5.0F, 10.0F)),
    (array(5.0F, -10.0F)) AS tab(col);

-- GROUP BY aggregation tests

-- Create test data using VALUES and GROUP BY
SELECT category, vector_sum(embedding) AS sum_vector
FROM VALUES
    ('A', array(1.0F, 2.0F, 3.0F)),
    ('A', array(4.0F, 5.0F, 6.0F)),
    ('B', array(2.0F, 1.0F, 4.0F)),
    ('B', array(3.0F, 2.0F, 1.0F))
AS tab(category, embedding)
GROUP BY category
ORDER BY category;

SELECT category, vector_avg(embedding) AS avg_vector
FROM VALUES
    ('A', array(2.0F, 4.0F, 6.0F)),
    ('A', array(4.0F, 8.0F, 12.0F)),
    ('B', array(1.0F, 2.0F, 3.0F)),
    ('B', array(3.0F, 6.0F, 9.0F))
AS tab(category, embedding)
GROUP BY category
ORDER BY category;

-- Edge cases

-- Empty vectors: sum returns empty array
SELECT vector_sum(col) FROM VALUES
    (CAST(array() AS ARRAY<FLOAT>)),
    (CAST(array() AS ARRAY<FLOAT>)) AS tab(col);

-- Empty vectors: avg returns empty array
SELECT vector_avg(col) FROM VALUES
    (CAST(array() AS ARRAY<FLOAT>)),
    (CAST(array() AS ARRAY<FLOAT>)) AS tab(col);

-- NULL vector handling: NULL vectors are skipped
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F)),
    (CAST(NULL AS ARRAY<FLOAT>)),
    (array(3.0F, 4.0F)) AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 2.0F)),
    (CAST(NULL AS ARRAY<FLOAT>)),
    (array(3.0F, 4.0F)) AS tab(col);

-- All NULL vectors: returns NULL
SELECT vector_sum(col) FROM VALUES
    (CAST(NULL AS ARRAY<FLOAT>)),
    (CAST(NULL AS ARRAY<FLOAT>)) AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (CAST(NULL AS ARRAY<FLOAT>)),
    (CAST(NULL AS ARRAY<FLOAT>)) AS tab(col);

-- Empty table: returns NULL
SELECT vector_sum(col) FROM VALUES (CAST(NULL AS ARRAY<FLOAT>)) AS tab(col) WHERE col IS NOT NULL;

SELECT vector_avg(col) FROM VALUES (CAST(NULL AS ARRAY<FLOAT>)) AS tab(col) WHERE col IS NOT NULL;

-- Vectors with NULL elements are skipped (treated as NULL)
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F)),
    (array(CAST(NULL AS FLOAT), 10.0F)),
    (array(3.0F, 4.0F)) AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 2.0F)),
    (array(CAST(NULL AS FLOAT), 10.0F)),
    (array(3.0F, 4.0F)) AS tab(col);

-- Only vectors with NULL elements: returns NULL
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, CAST(NULL AS FLOAT))),
    (array(CAST(NULL AS FLOAT), 2.0F)) AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (array(1.0F, CAST(NULL AS FLOAT))),
    (array(CAST(NULL AS FLOAT), 2.0F)) AS tab(col);

-- Mix of NULL vectors and vectors with NULL elements
SELECT vector_sum(col) FROM VALUES
    (CAST(NULL AS ARRAY<FLOAT>)),
    (array(1.0F, CAST(NULL AS FLOAT))),
    (array(1.0F, 2.0F)),
    (array(3.0F, 4.0F)) AS tab(col);

-- GROUP BY with NULL handling
SELECT category, vector_sum(embedding) AS sum_vector
FROM VALUES
    ('A', array(1.0F, 2.0F)),
    ('A', CAST(NULL AS ARRAY<FLOAT>)),
    ('A', array(3.0F, 4.0F)),
    ('B', CAST(NULL AS ARRAY<FLOAT>)),
    ('B', CAST(NULL AS ARRAY<FLOAT>))
AS tab(category, embedding)
GROUP BY category
ORDER BY category;

SELECT category, vector_avg(embedding) AS avg_vector
FROM VALUES
    ('A', array(1.0F, 2.0F)),
    ('A', CAST(NULL AS ARRAY<FLOAT>)),
    ('A', array(3.0F, 4.0F)),
    ('B', CAST(NULL AS ARRAY<FLOAT>)),
    ('B', CAST(NULL AS ARRAY<FLOAT>))
AS tab(category, embedding)
GROUP BY category
ORDER BY category;

-- Dimension mismatch errors

-- vector_sum: dimension mismatch
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F)),
    (array(1.0F, 2.0F)) AS tab(col);

-- vector_avg: dimension mismatch
SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F)),
    (array(1.0F, 2.0F)) AS tab(col);

-- Dimension mismatch with NULL in between (NULL skipped, then mismatch detected)
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F)),
    (CAST(NULL AS ARRAY<FLOAT>)),
    (array(1.0F, 2.0F)) AS tab(col);

-- Type mismatch errors (only ARRAY<FLOAT> is accepted)

-- vector_sum: ARRAY<DOUBLE> not accepted
SELECT vector_sum(col) FROM VALUES (array(1.0D, 2.0D)), (array(3.0D, 4.0D)) AS tab(col);

-- vector_sum: ARRAY<INT> not accepted
SELECT vector_sum(col) FROM VALUES (array(1, 2)), (array(3, 4)) AS tab(col);

-- vector_avg: ARRAY<DOUBLE> not accepted
SELECT vector_avg(col) FROM VALUES (array(1.0D, 2.0D)), (array(3.0D, 4.0D)) AS tab(col);

-- vector_avg: ARRAY<INT> not accepted
SELECT vector_avg(col) FROM VALUES (array(1, 2)), (array(3, 4)) AS tab(col);

-- Non-array argument errors

-- vector_sum: argument is not an array
SELECT vector_sum(col) FROM VALUES ('not an array'), ('also not an array') AS tab(col);

-- vector_sum: argument is a scalar
SELECT vector_sum(col) FROM VALUES (1.0F), (2.0F) AS tab(col);

-- vector_avg: argument is not an array
SELECT vector_avg(col) FROM VALUES ('not an array'), ('also not an array') AS tab(col);

-- vector_avg: argument is a scalar
SELECT vector_avg(col) FROM VALUES (1.0F), (2.0F) AS tab(col);

-- Large vectors (test potential SIMD unroll path with 16 elements)
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)),
    (array(16.0F, 15.0F, 14.0F, 13.0F, 12.0F, 11.0F, 10.0F, 9.0F, 8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F))
AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)),
    (array(16.0F, 15.0F, 14.0F, 13.0F, 12.0F, 11.0F, 10.0F, 9.0F, 8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F))
AS tab(col);

-- Large vector with NULL element
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, CAST(NULL AS FLOAT), 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)),
    (array(16.0F, 15.0F, 14.0F, 13.0F, 12.0F, 11.0F, 10.0F, 9.0F, 8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F))
AS tab(col);

-- Many vectors to test numerical stability
SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 1.0F)),
    (array(2.0F, 2.0F)),
    (array(3.0F, 3.0F)),
    (array(4.0F, 4.0F)),
    (array(5.0F, 5.0F)),
    (array(6.0F, 6.0F)),
    (array(7.0F, 7.0F)),
    (array(8.0F, 8.0F)),
    (array(9.0F, 9.0F)),
    (array(10.0F, 10.0F))
AS tab(col);
