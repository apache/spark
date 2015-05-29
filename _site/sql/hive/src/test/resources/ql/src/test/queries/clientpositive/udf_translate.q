set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION translate;
DESCRIBE FUNCTION EXTENDED translate;

-- Create some tables to serve some input data
CREATE TABLE table_input(input STRING);
CREATE TABLE table_translate(input_string STRING, from_string STRING, to_string STRING);

FROM src INSERT OVERWRITE TABLE table_input SELECT 'abcd' WHERE src.key = 86;
FROM src INSERT OVERWRITE TABLE table_translate SELECT 'abcd', 'ahd', '12' WHERE src.key = 86;

-- Run some queries on constant input parameters
SELECT  translate('abcd', 'ab', '12'),
        translate('abcd', 'abc', '12') FROM src tablesample (1 rows);

-- Run some queries where first parameter being a table column while the other two being constants
SELECT translate(table_input.input, 'ab', '12'),
       translate(table_input.input, 'abc', '12') FROM table_input tablesample (1 rows);

-- Run some queries where all parameters are coming from table columns
SELECT translate(input_string, from_string, to_string) FROM table_translate tablesample (1 rows);

-- Run some queries where some parameters are NULL
SELECT translate(NULL, 'ab', '12'),
       translate('abcd', NULL, '12'),
       translate('abcd', 'ab', NULL),
       translate(NULL, NULL, NULL) FROM src tablesample (1 rows);

-- Run some queries where the same character appears several times in the from string (2nd argument) of the UDF
SELECT translate('abcd', 'aba', '123'),
       translate('abcd', 'aba', '12') FROM src tablesample (1 rows);

-- Run some queries for the ignorant case when the 3rd parameter has more characters than the second one
SELECT translate('abcd', 'abc', '1234') FROM src tablesample (1 rows);

-- Test proper function over UTF-8 characters
SELECT translate('Àbcd', 'À', 'Ã') FROM src tablesample (1 rows);

