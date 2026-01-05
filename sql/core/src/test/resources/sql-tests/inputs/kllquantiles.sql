-- Positive test cases
-- Create tables with two columns for each data type

-- Integer table
DROP TABLE IF EXISTS t_int_1_5_through_7_11;
CREATE TABLE t_int_1_5_through_7_11 AS
VALUES 
  (1, 5), (2, 6), (3, 7), (4, 8), (5, 9), (6, 10), (7, 11) AS tab(col1, col2);

-- Long table  
DROP TABLE IF EXISTS t_long_1_5_through_7_11;
CREATE TABLE t_long_1_5_through_7_11 AS
VALUES
  (1L, 5L), (2L, 6L), (3L, 7L), (4L, 8L), (5L, 9L), (6L, 10L), (7L, 11L) AS tab(col1, col2);

-- SMALLINT (ShortType) table
DROP TABLE IF EXISTS t_short_1_5_through_7_11;
CREATE TABLE t_short_1_5_through_7_11 AS
VALUES
    (CAST(1 AS SMALLINT), CAST(5 AS SMALLINT)),
    (CAST(2 AS SMALLINT), CAST(6 AS SMALLINT)),
    (CAST(3 AS SMALLINT), CAST(7 AS SMALLINT)),
    (CAST(4 AS SMALLINT), CAST(8 AS SMALLINT)),
    (CAST(5 AS SMALLINT), CAST(9 AS SMALLINT)),
    (CAST(6 AS SMALLINT), CAST(10 AS SMALLINT)),
    (CAST(7 AS SMALLINT), CAST(11 AS SMALLINT))
    AS tab(col1, col2);

-- TINYINT (ByteType) table
DROP TABLE IF EXISTS t_byte_1_5_through_7_11;
CREATE TABLE t_byte_1_5_through_7_11 AS
VALUES
    (CAST(1 AS TINYINT), CAST(5 AS TINYINT)),
    (CAST(2 AS TINYINT), CAST(6 AS TINYINT)),
    (CAST(3 AS TINYINT), CAST(7 AS TINYINT)),
    (CAST(4 AS TINYINT), CAST(8 AS TINYINT)),
    (CAST(5 AS TINYINT), CAST(9 AS TINYINT)),
    (CAST(6 AS TINYINT), CAST(10 AS TINYINT)),
    (CAST(7 AS TINYINT), CAST(11 AS TINYINT))
    AS tab(col1, col2);

-- Float table
DROP TABLE IF EXISTS t_float_1_5_through_7_11;
CREATE TABLE t_float_1_5_through_7_11 AS
VALUES
    (CAST(1 AS FLOAT), CAST(5 AS FLOAT)),
    (CAST(2 AS FLOAT), CAST(6 AS FLOAT)),
    (CAST(3 AS FLOAT), CAST(7 AS FLOAT)),
    (CAST(4 AS FLOAT), CAST(8 AS FLOAT)),
    (CAST(5 AS FLOAT), CAST(9 AS FLOAT)),
    (CAST(6 AS FLOAT), CAST(10 AS FLOAT)),
    (CAST(7 AS FLOAT), CAST(11 AS FLOAT)) AS tab(col1, col2);

-- Double table
DROP TABLE IF EXISTS t_double_1_5_through_7_11;
CREATE TABLE t_double_1_5_through_7_11 AS
VALUES
    (CAST(1 AS DOUBLE), CAST(5 AS DOUBLE)),
    (CAST(2 AS DOUBLE), CAST(6 AS DOUBLE)),
    (CAST(3 AS DOUBLE), CAST(7 AS DOUBLE)),
    (CAST(4 AS DOUBLE), CAST(8 AS DOUBLE)),
    (CAST(5 AS DOUBLE), CAST(9 AS DOUBLE)),
    (CAST(6 AS DOUBLE), CAST(10 AS DOUBLE)),
    (CAST(7 AS DOUBLE), CAST(11 AS DOUBLE)) AS tab(col1, col2);

-- BIGINT sketches
SELECT lower(kll_sketch_to_string_bigint(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_bigint(agg, 0.5) - 4) < 1 AS median_close_to_4,
       abs(kll_sketch_get_rank_bigint(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_byte_1_5_through_7_11
);

SELECT lower(kll_sketch_to_string_bigint(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_bigint(agg, 0.5) - 4) < 1 AS median_close_to_4,
       abs(kll_sketch_get_rank_bigint(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_int_1_5_through_7_11
);

SELECT lower(kll_sketch_to_string_bigint(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_bigint(agg, 0.5) - 4) < 1 AS median_close_to_4,
       abs(kll_sketch_get_rank_bigint(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

SELECT lower(kll_sketch_to_string_bigint(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_bigint(agg, 0.5) - 4) < 1 AS median_close_to_4,
       abs(kll_sketch_get_rank_bigint(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_short_1_5_through_7_11
);

-- FLOAT sketches (only accepts float types to avoid precision loss)
SELECT lower(kll_sketch_to_string_float(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_float(agg, 0.5) - 4.0) < 0.5 AS median_close_to_4,
       abs(kll_sketch_get_rank_float(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- DOUBLE sketches (accepts float and double types to avoid precision loss from integer conversion)
SELECT lower(kll_sketch_to_string_double(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_double(agg, 0.5) - 4.0) < 0.5 AS median_close_to_4,
       abs(kll_sketch_get_rank_double(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_double(col1) AS agg
    FROM t_double_1_5_through_7_11
);

-- Test float column with double sketch (valid type promotion)
SELECT lower(kll_sketch_to_string_double(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_double(agg, 0.5) - 4.0) < 0.5 AS median_close_to_4,
       abs(kll_sketch_get_rank_double(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_double(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- Merging sketches and converting them to strings
SELECT
  split(
    kll_sketch_to_string_bigint(
      kll_sketch_merge_bigint(
        kll_sketch_agg_bigint(col1),
        kll_sketch_agg_bigint(col1)
      )
    ),
    '\n'
  )[1] AS result
  FROM t_byte_1_5_through_7_11;

SELECT
  split(
    kll_sketch_to_string_float(
      kll_sketch_merge_float(
        kll_sketch_agg_float(col1),
        kll_sketch_agg_float(col1)
      )
    ),
    '\n'
  )[1] AS result
FROM t_byte_1_5_through_7_11;

SELECT
  split(
    kll_sketch_to_string_double(
      kll_sketch_merge_double(
        kll_sketch_agg_double(col1),
        kll_sketch_agg_double(col1)
      )
    ),
    '\n'
  )[1] AS result
FROM t_byte_1_5_through_7_11;

-- Tests verifying that NULL input values are ignored by aggregate functions

-- Test BIGINT aggregate ignores NULL values
-- Verify that the sketch computed with NULLs matches the sketch without NULLs
-- Both should compute median of [1, 3, 5, 7] which is 4
-- Input data: 1, NULL, 3, 5, NULL, 7
SELECT abs(kll_sketch_get_quantile_bigint(agg_with_nulls, 0.5) - 
           kll_sketch_get_quantile_bigint(agg_without_nulls, 0.5)) < 1 AS medians_match,
       abs(kll_sketch_get_rank_bigint(agg_with_nulls, 4) - 
           kll_sketch_get_rank_bigint(agg_without_nulls, 4)) < 0.1 AS ranks_match
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg_with_nulls
    FROM (VALUES (1L), (CAST(NULL AS BIGINT)), (3L), (5L), (CAST(NULL AS BIGINT)), (7L)) AS tab(col1)
) WITH_NULLS,
(
    SELECT kll_sketch_agg_bigint(col1) AS agg_without_nulls
    FROM (VALUES (1L), (3L), (5L), (7L)) AS tab(col1)
) WITHOUT_NULLS;

-- Test FLOAT aggregate ignores NULL values
-- Verify that the sketch computed with NULLs matches the sketch without NULLs
-- Input data: 1.0, NULL, 3.0, 5.0, NULL, 7.0
SELECT abs(kll_sketch_get_quantile_float(agg_with_nulls, 0.5) - 
           kll_sketch_get_quantile_float(agg_without_nulls, 0.5)) < 0.5 AS medians_match,
       abs(kll_sketch_get_rank_float(agg_with_nulls, 4.0) - 
           kll_sketch_get_rank_float(agg_without_nulls, 4.0)) < 0.1 AS ranks_match
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg_with_nulls
    FROM (VALUES (1.0F), (CAST(NULL AS FLOAT)), (3.0F), (5.0F), (CAST(NULL AS FLOAT)), (7.0F)) AS tab(col1)
) WITH_NULLS,
(
    SELECT kll_sketch_agg_float(col1) AS agg_without_nulls
    FROM (VALUES (1.0F), (3.0F), (5.0F), (7.0F)) AS tab(col1)
) WITHOUT_NULLS;

-- Test DOUBLE aggregate ignores NULL values
-- Verify that the sketch computed with NULLs matches the sketch without NULLs
-- Input data: 1.0, NULL, 3.0, 5.0, NULL, 7.0
SELECT abs(kll_sketch_get_quantile_double(agg_with_nulls, 0.5) - 
           kll_sketch_get_quantile_double(agg_without_nulls, 0.5)) < 0.5 AS medians_match,
       abs(kll_sketch_get_rank_double(agg_with_nulls, 4.0) - 
           kll_sketch_get_rank_double(agg_without_nulls, 4.0)) < 0.1 AS ranks_match
FROM (
    SELECT kll_sketch_agg_double(col1) AS agg_with_nulls
    FROM (VALUES (1.0D), (CAST(NULL AS DOUBLE)), (3.0D), (5.0D), (CAST(NULL AS DOUBLE)), (7.0D)) AS tab(col1)
) WITH_NULLS,
(
    SELECT kll_sketch_agg_double(col1) AS agg_without_nulls
    FROM (VALUES (1.0D), (3.0D), (5.0D), (7.0D)) AS tab(col1)
) WITHOUT_NULLS;

-- Tests covering NULLs
-- NULL sketch to get_quantile
SELECT kll_sketch_get_quantile_bigint(CAST(NULL AS BINARY), 0.5) AS null_sketch;

-- NULL sketch to get_rank
SELECT kll_sketch_get_rank_float(CAST(NULL AS BINARY), 5.0) AS null_sketch;

-- Tests for the optional k parameter
-- Positive tests with valid k values
SELECT LENGTH(kll_sketch_to_string_bigint(kll_sketch_agg_bigint(col1, 8))) > 0 AS k_min_value
FROM t_long_1_5_through_7_11;

SELECT LENGTH(kll_sketch_to_string_bigint(kll_sketch_agg_bigint(col1, 200))) > 0 AS k_default_value
FROM t_long_1_5_through_7_11;

SELECT LENGTH(kll_sketch_to_string_bigint(kll_sketch_agg_bigint(col1, 400))) > 0 AS k_custom_value
FROM t_long_1_5_through_7_11;

SELECT LENGTH(kll_sketch_to_string_bigint(kll_sketch_agg_bigint(col1, 65535))) > 0 AS k_max_value
FROM t_long_1_5_through_7_11;

SELECT LENGTH(kll_sketch_to_string_float(kll_sketch_agg_float(col1, 100))) > 0 AS k_float_sketch
FROM t_float_1_5_through_7_11;

SELECT LENGTH(kll_sketch_to_string_double(kll_sketch_agg_double(col1, 300))) > 0 AS k_double_sketch
FROM t_double_1_5_through_7_11;

-- Tests for kll_sketch_get_n functions
-- BIGINT sketches
SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(col1)) AS n_bigint
FROM t_long_1_5_through_7_11;

SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(col1)) AS n_byte
FROM t_byte_1_5_through_7_11;

SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(col1)) AS n_short
FROM t_short_1_5_through_7_11;

SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(col1)) AS n_int
FROM t_int_1_5_through_7_11;

-- FLOAT sketches
SELECT kll_sketch_get_n_float(kll_sketch_agg_float(col1)) AS n_float
FROM t_float_1_5_through_7_11;

-- DOUBLE sketches
SELECT kll_sketch_get_n_double(kll_sketch_agg_double(col1)) AS n_double
FROM t_double_1_5_through_7_11;

-- Test with different k values
SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(col1, 100)) AS n_k_100
FROM t_long_1_5_through_7_11;

-- Negative tests
-- These queries should fail with type mismatch or validation errors

-- Type mismatch: BIGINT sketch does not accept DOUBLE columns
SELECT lower(kll_sketch_to_string_bigint(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_bigint(agg, 0.5) - 4) < 1 AS median_close_to_4,
       abs(kll_sketch_get_rank_bigint(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_double_1_5_through_7_11
);

-- Type mismatch: BIGINT sketch does not accept FLOAT columns
SELECT lower(kll_sketch_to_string_bigint(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_bigint(agg, 0.5) - 4) < 1 AS median_close_to_4,
       abs(kll_sketch_get_rank_bigint(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- Type mismatch: FLOAT sketch does not accept DOUBLE columns
SELECT lower(kll_sketch_to_string_float(agg)) LIKE '%kll%' AS str_contains_kll,
       abs(kll_sketch_get_quantile_float(agg, 0.5) - 4.0) < 0.5 AS median_close_to_4,
       abs(kll_sketch_get_rank_float(agg, 3) - 0.4) < 0.1 AS rank3_close_to_0_4
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg
    FROM t_double_1_5_through_7_11
);

-- Type mismatch: FLOAT sketch does not accept integer types (BIGINT) to avoid precision loss
SELECT kll_sketch_agg_float(col1) AS invalid_float_bigint
FROM t_long_1_5_through_7_11;

-- Type mismatch: FLOAT sketch does not accept integer types (INT) to avoid precision loss
SELECT kll_sketch_agg_float(col1) AS invalid_float_int
FROM t_int_1_5_through_7_11;

-- Type mismatch: FLOAT sketch does not accept integer types (SMALLINT) to avoid precision loss
SELECT kll_sketch_agg_float(col1) AS invalid_float_short
FROM t_short_1_5_through_7_11;

-- Type mismatch: FLOAT sketch does not accept integer types (TINYINT) to avoid precision loss
SELECT kll_sketch_agg_float(col1) AS invalid_float_byte
FROM t_byte_1_5_through_7_11;

-- Type mismatch: DOUBLE sketch does not accept integer types (BIGINT) to avoid precision loss
SELECT kll_sketch_agg_double(col1) AS invalid_double_bigint
FROM t_long_1_5_through_7_11;

-- Type mismatch: DOUBLE sketch does not accept integer types (INT) to avoid precision loss
SELECT kll_sketch_agg_double(col1) AS invalid_double_int
FROM t_int_1_5_through_7_11;

-- Type mismatch: DOUBLE sketch does not accept integer types (SMALLINT) to avoid precision loss
SELECT kll_sketch_agg_double(col1) AS invalid_double_short
FROM t_short_1_5_through_7_11;

-- Type mismatch: DOUBLE sketch does not accept integer types (TINYINT) to avoid precision loss
SELECT kll_sketch_agg_double(col1) AS invalid_double_byte
FROM t_byte_1_5_through_7_11;

-- Invalid quantile: quantile value must be between 0 and 1 (negative value)
SELECT kll_sketch_get_quantile_bigint(agg, -0.5) AS invalid_quantile
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

-- Invalid quantile: quantile value must be between 0 and 1 (value > 1)
SELECT kll_sketch_get_quantile_bigint(agg, 1.5) AS invalid_quantile
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

-- Invalid quantile: quantile array with out of range values
SELECT kll_sketch_get_quantile_float(agg, array(-0.1, 0.5, 1.5)) AS invalid_quantiles
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- Type mismatch: wrong sketch type for get_rank function
SELECT kll_sketch_get_rank_bigint(agg, 5) AS wrong_type
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- Type mismatch: incompatible sketches in merge (BIGINT and FLOAT)
SELECT kll_sketch_merge_bigint(agg1, agg2) AS incompatible_merge
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg1,
           kll_sketch_agg_float(CAST(col1 AS FLOAT)) AS agg2
    FROM t_long_1_5_through_7_11
);

-- Invalid input: non-sketch binary data to get_quantile
SELECT kll_sketch_get_quantile_bigint(CAST('not_a_sketch' AS BINARY), 0.5) AS invalid_binary;

-- Note: get_quantile functions cannot detect sketch type mismatches at the binary level.
-- This query succeeds even though we're using a FLOAT get_quantile on a BIGINT sketch,
-- but it returns garbage values because it interprets the BIGINT binary data as FLOAT data.
SELECT kll_sketch_get_quantile_float(agg, 0.5) IS NOT NULL AS returns_value
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

-- Note: to_string functions cannot detect sketch type mismatches because they just
-- interpret the binary data. This query succeeds even though we're using a DOUBLE
-- to_string function on a BIGINT sketch. The function reads the binary representation
-- and produces output, but the numeric values will be incorrectly interpreted.
SELECT lower(kll_sketch_to_string_double(agg)) LIKE '%kll%' AS contains_kll_header
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

-- Negative tests for k parameter
-- k parameter too small (minimum is 8)
SELECT kll_sketch_agg_bigint(col1, 7) AS k_too_small
FROM t_long_1_5_through_7_11;

-- k parameter too large (maximum is 65535)
SELECT kll_sketch_agg_bigint(col1, 65536) AS k_too_large
FROM t_long_1_5_through_7_11;

-- k parameter is NULL
SELECT kll_sketch_agg_float(col1, CAST(NULL AS INT)) AS k_is_null
FROM t_float_1_5_through_7_11;

-- k parameter is not foldable (non-constant)
SELECT kll_sketch_agg_double(col1, CAST(col1 AS INT)) AS k_non_constant
FROM t_double_1_5_through_7_11;

-- k parameter has wrong type (STRING instead of INT)
SELECT kll_sketch_agg_bigint(col1, '100') AS k_wrong_type
FROM t_long_1_5_through_7_11;

-- Negative tests for kll_sketch_get_n functions
-- Invalid binary data
SELECT kll_sketch_get_n_bigint(X'deadbeef') AS invalid_binary_bigint;

SELECT kll_sketch_get_n_float(X'cafebabe') AS invalid_binary_float;

SELECT kll_sketch_get_n_double(X'12345678') AS invalid_binary_double;

-- Wrong argument types
SELECT kll_sketch_get_n_bigint(42) AS wrong_argument_type;

SELECT kll_sketch_get_n_float(42.0) AS wrong_argument_type;

SELECT kll_sketch_get_n_double(42.0D) AS wrong_argument_type;

-- Negative tests for kll_sketch_get_quantile functions with invalid second argument types
-- Invalid type: STRING instead of DOUBLE for quantile parameter
SELECT kll_sketch_get_quantile_bigint(agg, 'invalid') AS quantile_string
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

-- Invalid type: BINARY instead of DOUBLE for quantile parameter
SELECT kll_sketch_get_quantile_float(agg, X'deadbeef') AS quantile_binary
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- Invalid type: BOOLEAN instead of DOUBLE for quantile parameter
SELECT kll_sketch_get_quantile_double(agg, true) AS quantile_boolean
FROM (
    SELECT kll_sketch_agg_double(col1) AS agg
    FROM t_double_1_5_through_7_11
);

-- Negative tests for kll_sketch_get_rank functions with invalid second argument types
-- Invalid type: STRING instead of BIGINT for rank value parameter
SELECT kll_sketch_get_rank_bigint(agg, 'invalid') AS rank_string
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg
    FROM t_long_1_5_through_7_11
);

-- Invalid type: BINARY instead of FLOAT for rank value parameter
SELECT kll_sketch_get_rank_float(agg, X'cafebabe') AS rank_binary
FROM (
    SELECT kll_sketch_agg_float(col1) AS agg
    FROM t_float_1_5_through_7_11
);

-- Invalid type: BOOLEAN instead of DOUBLE for rank value parameter
SELECT kll_sketch_get_rank_double(agg, false) AS rank_boolean
FROM (
    SELECT kll_sketch_agg_double(col1) AS agg
    FROM t_double_1_5_through_7_11
);

-- Negative tests for non-foldable (non-constant) rank/quantile arguments
-- These tests verify that get_quantile and get_rank functions require compile-time constant arguments

-- Non-foldable scalar rank argument to get_quantile (column reference)
SELECT kll_sketch_get_quantile_bigint(agg, CAST(col1 AS DOUBLE) / 10.0) AS non_foldable_scalar_rank
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg, col1
    FROM t_long_1_5_through_7_11
    GROUP BY col1
);

-- Non-foldable array rank argument to get_quantile (array containing column reference)
SELECT kll_sketch_get_quantile_bigint(agg, array(0.25, CAST(col1 AS DOUBLE) / 10.0, 0.75)) AS non_foldable_array_rank
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg, col1
    FROM t_long_1_5_through_7_11
    GROUP BY col1
);

-- Non-foldable scalar quantile argument to get_rank (column reference)
SELECT kll_sketch_get_rank_bigint(agg, col1) AS non_foldable_scalar_quantile
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg, col1
    FROM t_long_1_5_through_7_11
    GROUP BY col1
);

-- Non-foldable array quantile argument to get_rank (array containing column reference)
SELECT kll_sketch_get_rank_bigint(agg, array(1L, col1, 5L)) AS non_foldable_array_quantile
FROM (
    SELECT kll_sketch_agg_bigint(col1) AS agg, col1
    FROM t_long_1_5_through_7_11
    GROUP BY col1
);

-- Clean up
DROP TABLE IF EXISTS t_int_1_5_through_7_11;
DROP TABLE IF EXISTS t_long_1_5_through_7_11;
DROP TABLE IF EXISTS t_short_1_5_through_7_11;
DROP TABLE IF EXISTS t_byte_1_5_through_7_11;
DROP TABLE IF EXISTS t_float_1_5_through_7_11;
DROP TABLE IF EXISTS t_double_1_5_through_7_11;
