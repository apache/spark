-- Tests for vector norm functions: vector_norm, vector_normalize

-- Basic functionality tests

-- vector_norm: L2 norm (default) - sqrt(3^2 + 4^2) = 5
SELECT vector_norm(array(3.0F, 4.0F));

-- vector_norm: L2 norm explicit - sqrt(3^2 + 4^2) = 5
SELECT vector_norm(array(3.0F, 4.0F), 2.0F);

-- vector_norm: L1 norm - |3| + |4| = 7
SELECT vector_norm(array(3.0F, 4.0F), 1.0F);

-- vector_norm: L1 norm with negative values - |3| + |-4| = 7
SELECT vector_norm(array(3.0F, -4.0F), 1.0F);

-- vector_norm: infinity norm - max(|3|, |4|) = 4
SELECT vector_norm(array(3.0F, 4.0F), float('inf'));

-- vector_norm: infinity norm with negative values - max(|3|, |-5|) = 5
SELECT vector_norm(array(3.0F, -5.0F), float('inf'));

-- vector_normalize: L2 normalization (default) - [3/5, 4/5] = [0.6, 0.8]
SELECT vector_normalize(array(3.0F, 4.0F));

-- vector_normalize: L2 normalization explicit
SELECT vector_normalize(array(3.0F, 4.0F), 2.0F);

-- vector_normalize: L1 normalization - [3/7, 4/7]
SELECT vector_normalize(array(3.0F, 4.0F), 1.0F);

-- vector_normalize: infinity normalization - [3/4, 4/4] = [0.75, 1.0]
SELECT vector_normalize(array(3.0F, 4.0F), float('inf'));

-- vector_normalize: verify result has unit norm
SELECT vector_norm(vector_normalize(array(3.0F, 4.0F, 5.0F), 2.0F), 2.0F);

-- Edge cases

-- Empty vectors: norm returns 0.0
SELECT vector_norm(CAST(array() AS ARRAY<FLOAT>), 2.0F);
SELECT vector_norm(CAST(array() AS ARRAY<FLOAT>), 1.0F);
SELECT vector_norm(CAST(array() AS ARRAY<FLOAT>), float('inf'));

-- Empty vectors: normalize returns empty array
SELECT vector_normalize(CAST(array() AS ARRAY<FLOAT>), 2.0F);

-- Zero vector: norm returns 0.0
SELECT vector_norm(array(0.0F, 0.0F, 0.0F), 2.0F);

-- Zero vector: normalize returns NULL (division by zero)
SELECT vector_normalize(array(0.0F, 0.0F, 0.0F), 2.0F);

-- NULL array input: norm returns NULL
SELECT vector_norm(NULL, 2.0F);

-- NULL array input: normalize returns NULL
SELECT vector_normalize(NULL, 2.0F);

-- NULL degree: returns NULL
SELECT vector_norm(array(3.0F, 4.0F), CAST(NULL AS FLOAT));
SELECT vector_normalize(array(3.0F, 4.0F), CAST(NULL AS FLOAT));

-- Array containing NULL element: returns NULL
SELECT vector_norm(array(1.0F, CAST(NULL AS FLOAT), 3.0F), 2.0F);
SELECT vector_normalize(array(1.0F, CAST(NULL AS FLOAT), 3.0F), 2.0F);

-- Invalid degree errors

-- vector_norm: invalid degree (0.0)
SELECT vector_norm(array(3.0F, 4.0F), 0.0F);

-- vector_norm: invalid degree (3.0)
SELECT vector_norm(array(3.0F, 4.0F), 3.0F);

-- vector_norm: invalid degree (negative infinity)
SELECT vector_norm(array(3.0F, 4.0F), float('-inf'));

-- vector_norm: invalid degree (negative)
SELECT vector_norm(array(3.0F, 4.0F), -1.0F);

-- vector_normalize: invalid degree (0.0)
SELECT vector_normalize(array(3.0F, 4.0F), 0.0F);

-- vector_normalize: invalid degree (3.0)
SELECT vector_normalize(array(3.0F, 4.0F), 3.0F);

-- Type mismatch errors (only ARRAY<FLOAT> is accepted for vector)

-- vector_norm: ARRAY<DOUBLE> not accepted
SELECT vector_norm(array(3.0D, 4.0D), 2.0F);

-- vector_norm: ARRAY<INT> not accepted
SELECT vector_norm(array(3, 4), 2.0F);

-- vector_normalize: ARRAY<DOUBLE> not accepted
SELECT vector_normalize(array(3.0D, 4.0D), 2.0F);

-- vector_normalize: ARRAY<INT> not accepted
SELECT vector_normalize(array(3, 4), 2.0F);

-- Type mismatch errors (degree must be FLOAT)

-- vector_norm: degree as DOUBLE not accepted
SELECT vector_norm(array(3.0F, 4.0F), 2.0D);

-- vector_norm: degree as INT not accepted
SELECT vector_norm(array(3.0F, 4.0F), 2);

-- vector_normalize: degree as DOUBLE not accepted
SELECT vector_normalize(array(3.0F, 4.0F), 2.0D);

-- vector_normalize: degree as INT not accepted
SELECT vector_normalize(array(3.0F, 4.0F), 2);

-- Non-array argument errors

-- vector_norm: first argument is not an array
SELECT vector_norm('not an array', 2.0F);

-- vector_norm: first argument is a scalar
SELECT vector_norm(3.0F, 2.0F);

-- vector_normalize: first argument is not an array
SELECT vector_normalize('not an array', 2.0F);

-- vector_normalize: first argument is a scalar
SELECT vector_normalize(3.0F, 2.0F);

-- Large vectors (test SIMD unroll path with 16 elements)
SELECT vector_norm(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    2.0F
);

SELECT vector_norm(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    1.0F
);

SELECT vector_norm(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    float('inf')
);

SELECT vector_normalize(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    2.0F
);

-- Large vector with NULL element in unrolled section
SELECT vector_norm(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, CAST(NULL AS FLOAT), 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    2.0F
);
