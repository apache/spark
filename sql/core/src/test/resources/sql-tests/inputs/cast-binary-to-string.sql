-- Test UTF-8 validation when casting binary to string
-- Tests for SPARK-54586: Validate UTF-8 when casting BinaryType to StringType
-- The default behavior (validateUtf8=true) ensures StringType always contains valid UTF-8

-- Valid UTF-8 should work in all modes
CREATE TEMPORARY VIEW valid_utf8 AS VALUES
  (X'48656C6C6F', 'ASCII Hello'),
  (X'436166C3A9', 'Latin-1: Café'),
  (X'E282AC313030', 'Symbol: €100')
AS data(binary_col, description);

SELECT description, CAST(binary_col AS STRING) AS result FROM valid_utf8;


-- Invalid UTF-8 in LEGACY mode (default) returns NULL
SET spark.sql.ansi.enabled=false;

CREATE TEMPORARY VIEW invalid_utf8 AS VALUES
  (X'80', 'Invalid continuation byte'),
  (X'C1', 'Invalid start byte'),
  (X'FF', 'Invalid UTF-8 byte'),
  (X'C080', 'Overlong encoding'),
  (X'E28000', 'Incomplete 3-byte sequence')
AS data(binary_col, description);

SELECT description, CAST(binary_col AS STRING) AS result FROM invalid_utf8;


-- Invalid UTF-8 in ANSI mode throws error
SET spark.sql.ansi.enabled=true;

-- This should throw SparkRuntimeException
SELECT CAST(X'80' AS STRING);


-- try_cast always returns NULL for invalid UTF-8
SET spark.sql.ansi.enabled=false;

SELECT
  try_cast(X'48656C6C6F' AS STRING) AS valid_result,
  try_cast(X'80' AS STRING) AS invalid_result;


-- Legacy behavior with validation OFF
SET spark.sql.castBinaryToString.validateUtf8=false;

SELECT
  CAST(X'80' AS STRING) IS NOT NULL AS invalid_has_value,
  CAST(X'48656C6C6F' AS STRING) AS valid_value;


-- Ensure validation flag doesn't affect valid UTF-8
SET spark.sql.castBinaryToString.validateUtf8=true;

SELECT CAST(X'48656C6C6F' AS STRING) AS with_validation;

SET spark.sql.castBinaryToString.validateUtf8=false;

SELECT CAST(X'48656C6C6F' AS STRING) AS without_validation;


-- Cleanup
DROP VIEW IF EXISTS valid_utf8;
DROP VIEW IF EXISTS invalid_utf8;

