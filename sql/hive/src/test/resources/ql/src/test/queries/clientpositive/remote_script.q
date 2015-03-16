dfs -put ../../data/scripts/newline.py /newline.py;
add file hdfs:///newline.py;
set hive.transform.escape.input=true;

create table tmp_tmp(key string, value string) stored as rcfile;
insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python newline.py' AS key, value FROM src limit 6;

select * from tmp_tmp ORDER BY key ASC, value ASC;

dfs -rmr /newline.py;
drop table tmp_tmp;
