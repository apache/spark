

create table tmp(col0 string, col1 string,col2 string,col3 string,col4 string,col5 string,col6 string,col7 string,col8 string,col9 string,col10 string,col11 string);

insert overwrite table tmp select key, cast(key + 1 as int), key +2, key+3, key+4, cast(key+5 as int), key+6, key+7, key+8, key+9, key+10, cast(key+11 as int) from src where key = 100;

select * from tmp;

explain
FROM src a JOIN tmp b ON (a.key = b.col11)
SELECT /*+ MAPJOIN(a) */ a.value, b.col5, count(1) as count
where b.col11 = 111
group by a.value, b.col5;

FROM src a JOIN tmp b ON (a.key = b.col11)
SELECT /*+ MAPJOIN(a) */ a.value, b.col5, count(1) as count
where b.col11 = 111
group by a.value, b.col5;


