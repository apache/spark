set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION regexp;
DESCRIBE FUNCTION EXTENDED regexp;

SELECT 'fofo' REGEXP '^fo', 'fo\no' REGEXP '^fo\no$', 'Bn' REGEXP '^Ba*n', 'afofo' REGEXP 'fo',
'afofo' REGEXP '^fo', 'Baan' REGEXP '^Ba?n', 'axe' REGEXP 'pi|apa', 'pip' REGEXP '^(pi)*$'
FROM src tablesample (1 rows);
