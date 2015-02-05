set hive.fetch.task.conversion=more;

use default;
-- Test map_values() UDF

DESCRIBE FUNCTION map_values;
DESCRIBE FUNCTION EXTENDED map_values;

-- Evaluate function against STRING valued values
SELECT map_values(map(1, "a", 2, "b", 3, "c")) FROM src tablesample (1 rows);

-- Evaluate function against INT valued keys
SELECT map_values(map("a", 1, "b", 2, "c", 3)) FROM src tablesample (1 rows);
