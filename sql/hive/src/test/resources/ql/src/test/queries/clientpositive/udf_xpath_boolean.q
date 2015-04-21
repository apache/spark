set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION xpath_boolean ;
DESCRIBE FUNCTION EXTENDED xpath_boolean ;

SELECT xpath_boolean ('<a><b>b</b></a>', 'a/b') FROM src tablesample (1 rows) ;
SELECT xpath_boolean ('<a><b>b</b></a>', 'a/c') FROM src tablesample (1 rows) ;
SELECT xpath_boolean ('<a><b>b</b></a>', 'a/b = "b"') FROM src tablesample (1 rows) ;
SELECT xpath_boolean ('<a><b>b</b></a>', 'a/b = "c"') FROM src tablesample (1 rows) ;
SELECT xpath_boolean ('<a><b>10</b></a>', 'a/b < 10') FROM src tablesample (1 rows) ;
SELECT xpath_boolean ('<a><b>10</b></a>', 'a/b = 10') FROM src tablesample (1 rows) ;
