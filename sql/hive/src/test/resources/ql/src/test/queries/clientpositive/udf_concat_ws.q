set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION concat_ws;
DESCRIBE FUNCTION EXTENDED concat_ws;

CREATE TABLE dest1(c1 STRING, c2 STRING, c3 STRING);

FROM src INSERT OVERWRITE TABLE dest1 SELECT 'abc', 'xyz', '8675309'  WHERE src.key = 86;

EXPLAIN
SELECT concat_ws(dest1.c1, dest1.c2, dest1.c3),
       concat_ws(',', dest1.c1, dest1.c2, dest1.c3),
       concat_ws(NULL, dest1.c1, dest1.c2, dest1.c3),
       concat_ws('**', dest1.c1, NULL, dest1.c3) FROM dest1;

SELECT concat_ws(dest1.c1, dest1.c2, dest1.c3),
       concat_ws(',', dest1.c1, dest1.c2, dest1.c3),
       concat_ws(NULL, dest1.c1, dest1.c2, dest1.c3),
       concat_ws('**', dest1.c1, NULL, dest1.c3) FROM dest1;

-- evalutes function for array of strings
EXPLAIN
SELECT concat_ws('.', array('www', 'face', 'book', 'com'), '1234'),
       concat_ws('-', 'www', array('face', 'book', 'com'), '1234'),
       concat_ws('F', 'www', array('face', 'book', 'com', '1234')),
       concat_ws('_', array('www', 'face'), array('book', 'com', '1234')),
       concat_ws('**', 'www', array('face'), array('book', 'com', '1234')),
       concat_ws('[]', array('www'), 'face', array('book', 'com', '1234')),
       concat_ws('AAA', array('www'), array('face', 'book', 'com'), '1234') FROM dest1 tablesample (1 rows);

SELECT concat_ws('.', array('www', 'face', 'book', 'com'), '1234'),
       concat_ws('-', 'www', array('face', 'book', 'com'), '1234'),
       concat_ws('F', 'www', array('face', 'book', 'com', '1234')),
       concat_ws('_', array('www', 'face'), array('book', 'com', '1234')),
       concat_ws('**', 'www', array('face'), array('book', 'com', '1234')),
       concat_ws('[]', array('www'), 'face', array('book', 'com', '1234')),
       concat_ws('AAA', array('www'), array('face', 'book', 'com'), '1234') FROM dest1 tablesample (1 rows);

SELECT concat_ws(NULL, array('www', 'face', 'book', 'com'), '1234'),
       concat_ws(NULL, 'www', array('face', 'book', 'com'), '1234'),
       concat_ws(NULL, 'www', array('face', 'book', 'com', '1234')),
       concat_ws(NULL, array('www', 'face'), array('book', 'com', '1234')),
       concat_ws(NULL, 'www', array('face'), array('book', 'com', '1234')),
       concat_ws(NULL, array('www'), 'face', array('book', 'com', '1234')),
       concat_ws(NULL, array('www'), array('face', 'book', 'com'), '1234') FROM dest1 tablesample (1 rows);
