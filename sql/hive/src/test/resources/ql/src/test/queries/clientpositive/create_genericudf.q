EXPLAIN
CREATE TEMPORARY FUNCTION test_translate AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestTranslate';

CREATE TEMPORARY FUNCTION test_translate AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestTranslate';

CREATE TABLE dest1(c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING, c6 STRING, c7 STRING);

FROM src 
INSERT OVERWRITE TABLE dest1 
SELECT 
    test_translate('abc', 'a', 'b'),
    test_translate('abc', 'ab', 'bc'),
    test_translate(NULL, 'a', 'b'),
    test_translate('a', NULL, 'b'),
    test_translate('a', 'a', NULL),
    test_translate('abc', 'ab', 'b'),
    test_translate('abc', 'a', 'ab');

SELECT dest1.* FROM dest1 LIMIT 1;

DROP TEMPORARY FUNCTION test_translate;
