-- Tests for vector aggregation functions: vector_sum, vector_avg

-- Basic functionality
SELECT vector_sum(col) FROM VALUES (array(1.0F, 2.0F, 3.0F)), (array(4.0F, 5.0F, 6.0F)) AS tab(col);
SELECT vector_avg(col) FROM VALUES (array(1.0F, 2.0F, 3.0F)), (array(3.0F, 4.0F, 5.0F)) AS tab(col);

-- GROUP BY aggregation
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

-- NULL vector handling
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F)),
    (CAST(NULL AS ARRAY<FLOAT>)),
    (array(3.0F, 4.0F)) AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 2.0F)),
    (CAST(NULL AS ARRAY<FLOAT>)),
    (array(3.0F, 4.0F)) AS tab(col);

-- All NULL vectors returns NULL
SELECT vector_sum(col) FROM VALUES
    (CAST(NULL AS ARRAY<FLOAT>)),
    (CAST(NULL AS ARRAY<FLOAT>)) AS tab(col);

-- Empty vectors
SELECT vector_sum(col) FROM VALUES
    (CAST(array() AS ARRAY<FLOAT>)),
    (CAST(array() AS ARRAY<FLOAT>)) AS tab(col);

-- Window function
SELECT key, vector_sum(vec) OVER (PARTITION BY grp) as sum_vec
FROM VALUES
    ('a', 'g1', array(1.0F, 2.0F)),
    ('b', 'g1', array(3.0F, 4.0F)),
    ('c', 'g2', array(5.0F, 6.0F))
AS tab(key, grp, vec)
ORDER BY key;

SELECT key, vector_avg(vec) OVER (PARTITION BY grp) as avg_vec
FROM VALUES
    ('a', 'g1', array(1.0F, 2.0F)),
    ('b', 'g1', array(3.0F, 4.0F)),
    ('c', 'g2', array(5.0F, 6.0F))
AS tab(key, grp, vec)
ORDER BY key;

-- Error cases: dimension mismatch
SELECT vector_sum(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F)),
    (array(1.0F, 2.0F)) AS tab(col);

SELECT vector_avg(col) FROM VALUES
    (array(1.0F, 2.0F, 3.0F)),
    (array(1.0F, 2.0F)) AS tab(col);

-- Error cases: wrong element type (only ARRAY<FLOAT> accepted)
SELECT vector_sum(col) FROM VALUES (array(1.0D, 2.0D)), (array(3.0D, 4.0D)) AS tab(col);
SELECT vector_avg(col) FROM VALUES (array(1, 2)), (array(3, 4)) AS tab(col);

-- Error cases: non-array argument
SELECT vector_sum(col) FROM VALUES ('not an array'), ('also not an array') AS tab(col);
SELECT vector_avg(col) FROM VALUES (1.0F), (2.0F) AS tab(col);
