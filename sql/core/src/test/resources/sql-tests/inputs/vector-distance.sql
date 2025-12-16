-- Tests for vector distance functions: vector_cosine_similarity, vector_inner_product, vector_l2_distance

-- Basic functionality tests

-- vector_cosine_similarity: basic test
SELECT vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F));

-- vector_cosine_similarity: identical vectors (similarity = 1.0)
SELECT vector_cosine_similarity(array(1.0F, 0.0F, 0.0F), array(1.0F, 0.0F, 0.0F));

-- vector_cosine_similarity: orthogonal vectors (similarity = 0.0)
SELECT vector_cosine_similarity(array(1.0F, 0.0F), array(0.0F, 1.0F));

-- vector_cosine_similarity: opposite vectors (similarity = -1.0)
SELECT vector_cosine_similarity(array(1.0F, 0.0F), array(-1.0F, 0.0F));

-- vector_inner_product: basic test (1*4 + 2*5 + 3*6 = 32)
SELECT vector_inner_product(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F));

-- vector_inner_product: orthogonal vectors (product = 0)
SELECT vector_inner_product(array(1.0F, 0.0F), array(0.0F, 1.0F));

-- vector_inner_product: self product (squared L2 norm: 3^2 + 4^2 = 25)
SELECT vector_inner_product(array(3.0F, 4.0F), array(3.0F, 4.0F));

-- vector_l2_distance: basic test (sqrt((4-1)^2 + (5-2)^2 + (6-3)^2) = sqrt(27))
SELECT vector_l2_distance(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F));

-- vector_l2_distance: identical vectors (distance = 0)
SELECT vector_l2_distance(array(1.0F, 2.0F), array(1.0F, 2.0F));

-- vector_l2_distance: 3-4-5 triangle (distance = 5)
SELECT vector_l2_distance(array(0.0F, 0.0F), array(3.0F, 4.0F));

-- Edge cases

-- Empty vectors: cosine similarity returns NULL
SELECT vector_cosine_similarity(array(), array());

-- Empty vectors: inner product returns 0.0
SELECT vector_inner_product(CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>));

-- Empty vectors: L2 distance returns 0.0
SELECT vector_l2_distance(CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>));

-- Zero magnitude vector: cosine similarity returns NULL
SELECT vector_cosine_similarity(array(0.0F, 0.0F, 0.0F), array(1.0F, 2.0F, 3.0F));

-- NULL array input: cosine similarity returns NULL
SELECT vector_cosine_similarity(NULL, array(1.0F, 2.0F, 3.0F));
SELECT vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), NULL);

-- NULL array input: inner product returns NULL
SELECT vector_inner_product(NULL, array(1.0F, 2.0F, 3.0F));
SELECT vector_inner_product(array(1.0F, 2.0F, 3.0F), NULL);

-- NULL array input: L2 distance returns NULL
SELECT vector_l2_distance(NULL, array(1.0F, 2.0F, 3.0F));
SELECT vector_l2_distance(array(1.0F, 2.0F, 3.0F), NULL);

-- Array containing NULL element: returns NULL
SELECT vector_cosine_similarity(array(1.0F, CAST(NULL AS FLOAT), 3.0F), array(1.0F, 2.0F, 3.0F));
SELECT vector_inner_product(array(1.0F, CAST(NULL AS FLOAT), 3.0F), array(1.0F, 2.0F, 3.0F));
SELECT vector_l2_distance(array(1.0F, CAST(NULL AS FLOAT), 3.0F), array(1.0F, 2.0F, 3.0F));

-- Dimension mismatch errors

-- vector_cosine_similarity: dimension mismatch
SELECT vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), array(1.0F, 2.0F));

-- vector_inner_product: dimension mismatch
SELECT vector_inner_product(array(1.0F, 2.0F, 3.0F), array(1.0F, 2.0F));

-- vector_l2_distance: dimension mismatch
SELECT vector_l2_distance(array(1.0F, 2.0F, 3.0F), array(1.0F, 2.0F));

-- Type mismatch errors (only ARRAY<FLOAT> is accepted)

-- vector_cosine_similarity: ARRAY<DOUBLE> not accepted
SELECT vector_cosine_similarity(array(1.0D, 2.0D, 3.0D), array(4.0D, 5.0D, 6.0D));

-- vector_cosine_similarity: ARRAY<INT> not accepted
SELECT vector_cosine_similarity(array(1, 2, 3), array(4, 5, 6));

-- vector_inner_product: ARRAY<DOUBLE> not accepted
SELECT vector_inner_product(array(1.0D, 2.0D, 3.0D), array(4.0D, 5.0D, 6.0D));

-- vector_inner_product: ARRAY<INT> not accepted
SELECT vector_inner_product(array(1, 2, 3), array(4, 5, 6));

-- vector_l2_distance: ARRAY<DOUBLE> not accepted
SELECT vector_l2_distance(array(1.0D, 2.0D, 3.0D), array(4.0D, 5.0D, 6.0D));

-- vector_l2_distance: ARRAY<INT> not accepted
SELECT vector_l2_distance(array(1, 2, 3), array(4, 5, 6));

-- Mixed type errors (first arg correct, second wrong)
SELECT vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), array(1.0D, 2.0D, 3.0D));

-- Non-array argument errors (argument is not an array at all)

-- vector_cosine_similarity: first argument is not an array
SELECT vector_cosine_similarity('not an array', array(1.0F, 2.0F, 3.0F));

-- vector_cosine_similarity: second argument is not an array
SELECT vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), 'not an array');

-- vector_cosine_similarity: both arguments are not arrays
SELECT vector_cosine_similarity(123, 456);

-- vector_inner_product: first argument is not an array
SELECT vector_inner_product('not an array', array(1.0F, 2.0F, 3.0F));

-- vector_inner_product: second argument is not an array
SELECT vector_inner_product(array(1.0F, 2.0F, 3.0F), 'not an array');

-- vector_l2_distance: first argument is not an array
SELECT vector_l2_distance('not an array', array(1.0F, 2.0F, 3.0F));

-- vector_l2_distance: second argument is not an array
SELECT vector_l2_distance(array(1.0F, 2.0F, 3.0F), 'not an array');

-- Large vectors (test SIMD unroll path with 16 elements)
SELECT vector_inner_product(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    array(16.0F, 15.0F, 14.0F, 13.0F, 12.0F, 11.0F, 10.0F, 9.0F, 8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F)
);

SELECT vector_l2_distance(
    array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
    array(16.0F, 15.0F, 14.0F, 13.0F, 12.0F, 11.0F, 10.0F, 9.0F, 8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F)
);
