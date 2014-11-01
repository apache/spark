



create table hive_foo (foo_id int, foo_name string, foo_a string, foo_b string, 
foo_c string, foo_d string) row format delimited fields terminated by ','
stored as textfile;

create table hive_bar (bar_id int, bar_0 int, foo_id int, bar_1 int, bar_name
string, bar_a string, bar_b string, bar_c string, bar_d string) row format 
delimited fields terminated by ',' stored as textfile;

create table hive_count (bar_id int, n int) row format delimited fields 
terminated by ',' stored as textfile;

load data local inpath '../../data/files/hive_626_foo.txt' overwrite into table hive_foo;
load data local inpath '../../data/files/hive_626_bar.txt' overwrite into table hive_bar;
load data local inpath '../../data/files/hive_626_count.txt' overwrite into table hive_count;

explain
select hive_foo.foo_name, hive_bar.bar_name, n from hive_foo join hive_bar on hive_foo.foo_id =
hive_bar.foo_id join hive_count on hive_count.bar_id = hive_bar.bar_id;

select hive_foo.foo_name, hive_bar.bar_name, n from hive_foo join hive_bar on hive_foo.foo_id =
hive_bar.foo_id join hive_count on hive_count.bar_id = hive_bar.bar_id;





