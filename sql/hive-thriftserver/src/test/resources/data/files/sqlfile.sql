SHOW DATABASES; #
SHOW TABLES ; --
CREATE TABLE t(id int, name string, tag string)
ROW FORMAT DELIMITED FIELDS TERMINATED by '\t'
;
SELECT id, 1+1 FROM t; --test;
SELECT id, 1+1 FROM t; #test;
SELECT id, 1 + 1 FROM t;# This comment continues to the end of line;
SELECT id, 1 + 1 FROM t;-- This comment continues to the end of line;
SELECT id, 1/* this is an in-line comment */  + 1 FROM t;
SELECT id, 1 +
/*
this is a
multiple-line comment
*/
1 FROM t;

SHOW DATABASES; /* test */ SHOW
TABLES; --end;
--end1
#end2