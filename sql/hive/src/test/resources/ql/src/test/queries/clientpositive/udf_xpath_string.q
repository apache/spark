DESCRIBE FUNCTION xpath_string ;
DESCRIBE FUNCTION EXTENDED xpath_string ;

SELECT xpath_string ('<a><b>bb</b><c>cc</c></a>', 'a') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>bb</b><c>cc</c></a>', 'a/b') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>bb</b><c>cc</c></a>', 'a/c') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>bb</b><c>cc</c></a>', 'a/d') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>b1</b><b>b2</b></a>', '//b') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>b1</b><b>b2</b></a>', 'a/b[1]') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>b1</b><b>b2</b></a>', 'a/b[2]') FROM src LIMIT 1 ;
SELECT xpath_string ('<a><b>b1</b><b id="b_2">b2</b></a>', 'a/b[@id="b_2"]') FROM src LIMIT 1 ;
