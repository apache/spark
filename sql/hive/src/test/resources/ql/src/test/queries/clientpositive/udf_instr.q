set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION instr;
DESCRIBE FUNCTION EXTENDED instr;

EXPLAIN
SELECT instr('abcd', 'abc'),
       instr('abcabc', 'ccc'),
       instr(123, '23'),
       instr(123, 23),
       instr(TRUE, 1),
       instr(FALSE, 1),
       instr('12345', CAST('2' AS TINYINT)),
       instr(CAST('12345' AS SMALLINT), '34'),
       instr(CAST('123456789012' AS BIGINT), '456'),
       instr(CAST(1.25 AS FLOAT), '.25'),
       instr(CAST(16.0 AS DOUBLE), '.0'),
       instr(null, 'abc'),
       instr('abcd', null)
FROM src tablesample (1 rows);

SELECT instr('abcd', 'abc'),
       instr('abcabc', 'ccc'),
       instr(123, '23'),
       instr(123, 23),
       instr(TRUE, 1),
       instr(FALSE, 1),
       instr('12345', CAST('2' AS TINYINT)),
       instr(CAST('12345' AS SMALLINT), '34'),
       instr(CAST('123456789012' AS BIGINT), '456'),
       instr(CAST(1.25 AS FLOAT), '.25'),
       instr(CAST(16.0 AS DOUBLE), '.0'),
       instr(null, 'abc'),
       instr('abcd', null)
FROM src tablesample (1 rows);
