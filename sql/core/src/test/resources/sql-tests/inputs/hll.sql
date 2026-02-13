-- Positive test cases
-- Create a table with some testing data.
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS hll_binary_test;
DROP TABLE IF EXISTS hll_string_test;

CREATE TABLE t1 USING JSON AS VALUES (0), (1), (2), (2), (2), (3), (4) as tab(col);
CREATE TABLE hll_binary_test (bytes BINARY) USING PARQUET;
CREATE TABLE hll_string_test (s STRING) USING PARQUET;

INSERT INTO hll_binary_test VALUES (X''), (CAST('  ' AS BINARY)), (X'e280'), (X'c1'), (X'c120');

-- `\u030A` is the "combining ring above" Unicode character: https://www.compart.com/en/unicode/U+030A
-- `\uFFFD is the Unicode replacement character
-- `\xC1` is an invalid Unicode byte.
-- `\x80` is a Unicode continuation byte, that is it cannot be the first byte of a multi-byte UTF8 character.
-- All strings are different based on the UTF8_BINARY collation.
-- The first and second strings are equal for any collation with the RTRIM modifier, and equal to the empty string.
-- The last three strings are respectively equal to the next last three strings for any collation with the RTRIM modifier.
-- The strings "\xC1", "\x80" and "\uFFFD" are equal for all collations except UTF8_BINARY.
-- The (sub)strings `å` and `a\u030A` are equal for the UNICODE family of collations.
-- `å` is the lowercase version of `Å`.
INSERT INTO hll_string_test VALUES (''), ('  '), (CAST(X'C1' AS STRING)), (CAST(X'80' AS STRING)), ('\uFFFD'), ('Å'), ('å'), ('a\u030A'), ('Å '), ('å  '), ('a\u030A   ');

SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result FROM t1;

SELECT hll_sketch_estimate(hll_sketch_agg(bytes)) FROM hll_binary_test;

SELECT hll_sketch_estimate(hll_sketch_agg(s)) utf8_b FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UTF8_LCASE)) utf8_lc FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UNICODE)) unicode FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UNICODE_CI)) unicode_ci FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UTF8_BINARY_RTRIM)) utf8_b_rt FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UTF8_LCASE_RTRIM)) utf8_lc_rt FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UNICODE_RTRIM)) unicode_rt FROM hll_string_test;
SELECT hll_sketch_estimate(hll_sketch_agg(s COLLATE UNICODE_CI_RTRIM)) unicode_ci_rt FROM hll_string_test;

SELECT hll_sketch_estimate(hll_sketch_agg(col, 12))
FROM VALUES (50), (60), (60), (60), (75), (100) tab(col);

SELECT hll_sketch_estimate(hll_sketch_agg(col))
FROM VALUES ('abc'), ('def'), ('abc'), ('ghi'), ('abc') tab(col);

SELECT hll_sketch_estimate(
  hll_union(
    hll_sketch_agg(col1),
    hll_sketch_agg(col2)))
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

SELECT hll_sketch_estimate(hll_union_agg(sketch, true))
    FROM (SELECT hll_sketch_agg(col) as sketch
            FROM VALUES (1) AS tab(col)
          UNION ALL
          SELECT hll_sketch_agg(col, 20) as sketch
            FROM VALUES (1) AS tab(col));

-- Negative test cases
SELECT hll_sketch_agg(col)
FROM VALUES (ARRAY(1, 2)), (ARRAY(3, 4)) tab(col);

SELECT hll_sketch_agg(col, 2)
FROM VALUES (50), (60), (60) tab(col);

SELECT hll_sketch_agg(col, 40)
FROM VALUES (50), (60), (60) tab(col);

SELECT hll_sketch_agg(col, CAST(NULL AS INT)) AS k_is_null
FROM VALUES (15), (16), (17) tab(col);

SELECT hll_sketch_agg(col, CAST(col AS INT)) AS k_non_constant
FROM VALUES (15), (16), (17) tab(col);

SELECT hll_sketch_agg(col, '15')
FROM VALUES (50), (60), (60) tab(col);

SELECT hll_union(
    hll_sketch_agg(col1, 12),
    hll_sketch_agg(col2, 13))
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

SELECT hll_union_agg(sketch, false)
FROM (SELECT hll_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT hll_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

SELECT hll_union(1, 2)
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- The HLL functions receive invalid buffers as inputs.
SELECT hll_sketch_estimate(CAST ('abc' AS BINARY));

SELECT hll_union(CAST ('abc' AS BINARY), CAST ('def' AS BINARY));

SELECT hll_union_agg(buffer, false)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Clean up
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS hll_binary_test;
DROP TABLE IF EXISTS hll_string_test;
