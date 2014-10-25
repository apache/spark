add file ../../data/scripts/newline.py;
set hive.transform.escape.input=true;

create table tmp_tmp(key string, value string) stored as rcfile;
insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python newline.py' AS key, value FROM src limit 6;

select * from tmp_tmp ORDER BY key ASC, value ASC;

drop table tmp_tmp;

add file ../../data/scripts/escapednewline.py;
add file ../../data/scripts/escapedtab.py;
add file ../../data/scripts/doubleescapedtab.py;
add file ../../data/scripts/escapedcarriagereturn.py;

create table tmp_tmp(key string, value string) stored as rcfile;
insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python escapednewline.py' AS key, value FROM src limit 5;

select * from tmp_tmp ORDER BY key ASC, value ASC;

SELECT TRANSFORM(key, value) USING
'cat' AS (key, value) FROM tmp_tmp;

insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python escapedcarriagereturn.py' AS key, value FROM src limit 5;

select * from tmp_tmp ORDER BY key ASC, value ASC;

SELECT TRANSFORM(key, value) USING
'cat' AS (key, value) FROM tmp_tmp;

insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python escapedtab.py' AS key, value FROM src limit 5;

select * from tmp_tmp ORDER BY key ASC, value ASC;

SELECT TRANSFORM(key, value) USING
'cat' AS (key, value) FROM tmp_tmp;

insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python doubleescapedtab.py' AS key, value FROM src limit 5;

select * from tmp_tmp ORDER BY key ASC, value ASC;

SELECT TRANSFORM(key, value) USING
'cat' AS (key, value) FROM tmp_tmp;

SELECT key FROM (SELECT TRANSFORM ('a\tb', 'c') USING 'cat' AS (key, value) FROM src limit 1)a ORDER BY key ASC;

SELECT value FROM (SELECT TRANSFORM ('a\tb', 'c') USING 'cat' AS (key, value) FROM src limit 1)a ORDER BY value ASC;
