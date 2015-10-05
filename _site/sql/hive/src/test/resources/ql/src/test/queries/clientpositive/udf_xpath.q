set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION xpath ;
DESCRIBE FUNCTION EXTENDED xpath ;

SELECT xpath ('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/text()') FROM src tablesample (1 rows) ;
SELECT xpath ('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/*/text()') FROM src tablesample (1 rows) ;
SELECT xpath ('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()') FROM src tablesample (1 rows) ;
SELECT xpath ('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/c/text()') FROM src tablesample (1 rows) ;
SELECT xpath ('<a><b class="bb">b1</b><b>b2</b><b>b3</b><c class="bb">c1</c><c>c2</c></a>', 'a/*[@class="bb"]/text()') FROM src tablesample (1 rows) ;