CREATE TABLE mytable(key binary, value int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '9'
STORED AS TEXTFILE;
-- this query loads native binary data, stores in a table and then queries it. Note that string.txt contains binary data. Also uses transform clause and then length udf.

LOAD DATA LOCAL INPATH '../data/files/string.txt' INTO TABLE mytable;

create table dest1 (key binary, value int);

insert overwrite table dest1 select transform(*) using 'cat' as key binary, value int from mytable; 

select key, value, length (key) from dest1;
