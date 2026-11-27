--SET spark.sql.json.combineUnicodeSurrogatesInUtf8.enabled=true

CREATE TABLE json_unicode_source (j STRING) USING parquet;
INSERT INTO json_unicode_source VALUES ('{"a":["🌊"]}');
CREATE TABLE json_unicode_sink (a STRING) USING parquet;

CREATE FLOW json_unicode_flow AS
INSERT INTO json_unicode_sink BY NAME
SELECT get_json_object(j, '$.a') AS a FROM STREAM(json_unicode_source);

REFRESH FLOW json_unicode_flow;
SELECT a FROM json_unicode_sink;

DROP TABLE json_unicode_source;
DROP TABLE json_unicode_sink;
