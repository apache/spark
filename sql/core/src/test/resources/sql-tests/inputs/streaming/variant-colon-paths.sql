--SET spark.sql.legacy.variantPathBackslashAsLiteral=false

CREATE TABLE variant_colon_source (v VARIANT) USING parquet;
INSERT INTO variant_colon_source SELECT parse_json(r'{"a\"b":1}');

CREATE TABLE variant_colon_sink (`a"b` VARIANT) USING parquet;

CREATE FLOW variant_colon_flow AS
INSERT INTO variant_colon_sink BY NAME
SELECT v:["a\"b"] FROM STREAM(variant_colon_source);

REFRESH FLOW variant_colon_flow;

SELECT `a"b`::int AS value FROM variant_colon_sink;

DROP TABLE variant_colon_source;
DROP TABLE variant_colon_sink;
