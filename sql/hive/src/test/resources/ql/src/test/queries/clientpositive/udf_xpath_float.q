DESCRIBE FUNCTION xpath_float ;
DESCRIBE FUNCTION EXTENDED xpath_float ;

SELECT xpath_float ('<a>this is not a number</a>', 'a') FROM src LIMIT 1 ;
SELECT xpath_float ('<a>this 2 is not a number</a>', 'a') FROM src LIMIT 1 ;
SELECT xpath_float ('<a><b>2000000000</b><c>40000000000</c></a>', 'a/b * a/c') FROM src LIMIT 1 ;
SELECT xpath_float ('<a>try a boolean</a>', 'a = 10') FROM src LIMIT 1 ;
SELECT xpath_float ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'a/b') FROM src LIMIT 1 ;
SELECT xpath_float ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/*)') FROM src LIMIT 1 ;
SELECT xpath_float ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/b)') FROM src LIMIT 1 ;
SELECT xpath_float ('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/b[@class="odd"])') FROM src LIMIT 1 ;