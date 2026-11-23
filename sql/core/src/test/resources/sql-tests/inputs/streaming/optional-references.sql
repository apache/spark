-- Optional references in a streaming query: a `?` after an identifier makes that identifier
-- optional, so that a reference that cannot be resolved evaluates to a static NULL.

--SET spark.sql.optionalReferences.enabled=true

DROP TABLE IF EXISTS streaming_sql_golden_optional_references_source;
DROP TABLE IF EXISTS streaming_sql_golden_optional_references_sink;

CREATE TABLE streaming_sql_golden_optional_references_source
  (id INT, value STRING) USING parquet;
INSERT INTO streaming_sql_golden_optional_references_source
VALUES (1, 'one'), (2, 'two'), (3, 'three');

CREATE TABLE streaming_sql_golden_optional_references_sink
  (id INT, value STRING, deleted_at STRING) USING parquet;

-- The source has the columns `id` and `value`, but no column `deleted_at` or `archived`.
CREATE FLOW streaming_sql_golden_optional_references_flow AS
INSERT INTO streaming_sql_golden_optional_references_sink BY NAME
SELECT id?, upper(value?) AS value, deleted_at?
FROM STREAM(streaming_sql_golden_optional_references_source)
WHERE id? >= 2 AND archived? IS NULL;

-- An AUTO CDC flow modifies its target like a MERGE statement, so it rejects `?`.
CREATE FLOW streaming_sql_golden_optional_references_cdc_flow AS
AUTO CDC INTO streaming_sql_golden_optional_references_sink
FROM STREAM(streaming_sql_golden_optional_references_source)
KEYS (id)
APPLY AS DELETE WHEN deleted? = true
SEQUENCE BY id;

REFRESH FLOW streaming_sql_golden_optional_references_flow;

SELECT * FROM streaming_sql_golden_optional_references_sink ORDER BY id;

INSERT INTO streaming_sql_golden_optional_references_source VALUES (4, 'four');

REFRESH FLOW streaming_sql_golden_optional_references_flow;

SELECT * FROM streaming_sql_golden_optional_references_sink ORDER BY id;

DROP TABLE streaming_sql_golden_optional_references_source;
DROP TABLE streaming_sql_golden_optional_references_sink;
