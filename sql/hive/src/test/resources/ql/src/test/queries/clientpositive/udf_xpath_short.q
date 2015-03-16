set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION xpath_short ;
DESCRIBE FUNCTION EXTENDED xpath_short ;

SELECT xpath_short ('<a>this is not a number</a>', 'a') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a>this 2 is not a number</a>', 'a') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a><b>2000000000</b><c>40000000000</c></a>', 'a/b * a/c') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a>try a boolean</a>', 'a = 10') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'a/b') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/*)') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/b)') FROM src tablesample (1 rows) ;
SELECT xpath_short ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/b[@class="odd"])') FROM src tablesample (1 rows) ;