set hive.fetch.task.conversion=more;

use default;
-- Test map_keys() UDF

DESCRIBE FUNCTION map_keys;
DESCRIBE FUNCTION EXTENDED map_keys;

-- Evaluate function against INT valued keys
SELECT map_keys(map(1, "a", 2, "b", 3, "c")) FROM src tablesample (1 rows);

-- Evaluate function against STRING valued keys
SELECT map_keys(map("a", 1, "b", 2, "c", 3)) FROM src tablesample (1 rows);
