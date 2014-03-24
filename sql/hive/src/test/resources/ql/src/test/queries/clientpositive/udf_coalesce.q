DESCRIBE FUNCTION coalesce;
DESCRIBE FUNCTION EXTENDED coalesce;

EXPLAIN
SELECT COALESCE(1),
       COALESCE(1, 2),
       COALESCE(NULL, 2),
       COALESCE(1, NULL),
       COALESCE(NULL, NULL, 3),
       COALESCE(4, NULL, NULL, NULL),
       COALESCE('1'),
       COALESCE('1', '2'),
       COALESCE(NULL, '2'),
       COALESCE('1', NULL),
       COALESCE(NULL, NULL, '3'),
       COALESCE('4', NULL, NULL, NULL),
       COALESCE(1.0),
       COALESCE(1.0, 2.0),
       COALESCE(NULL, 2.0),
       COALESCE(NULL, 2.0, 3.0),
       COALESCE(2.0, NULL, 3.0),
       COALESCE(IF(TRUE, NULL, 0), NULL)
FROM src LIMIT 1;

SELECT COALESCE(1),
       COALESCE(1, 2),
       COALESCE(NULL, 2),
       COALESCE(1, NULL),
       COALESCE(NULL, NULL, 3),
       COALESCE(4, NULL, NULL, NULL),
       COALESCE('1'),
       COALESCE('1', '2'),
       COALESCE(NULL, '2'),
       COALESCE('1', NULL),
       COALESCE(NULL, NULL, '3'),
       COALESCE('4', NULL, NULL, NULL),
       COALESCE(1.0),
       COALESCE(1.0, 2.0),
       COALESCE(NULL, 2.0),
       COALESCE(NULL, 2.0, 3.0),
       COALESCE(2.0, NULL, 3.0),
       COALESCE(IF(TRUE, NULL, 0), NULL)
FROM src LIMIT 1;

EXPLAIN
SELECT COALESCE(src_thrift.lint[1], 999),
       COALESCE(src_thrift.lintstring[0].mystring, '999'),
       COALESCE(src_thrift.mstringstring['key_2'], '999')
FROM src_thrift;

SELECT COALESCE(src_thrift.lint[1], 999),
       COALESCE(src_thrift.lintstring[0].mystring, '999'),
       COALESCE(src_thrift.mstringstring['key_2'], '999')
FROM src_thrift;
