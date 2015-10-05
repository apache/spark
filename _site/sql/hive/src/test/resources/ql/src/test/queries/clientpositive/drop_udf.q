CREATE TEMPORARY FUNCTION test_translate AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestTranslate';

EXPLAIN
DROP TEMPORARY FUNCTION test_translate;

DROP TEMPORARY FUNCTION test_translate;
