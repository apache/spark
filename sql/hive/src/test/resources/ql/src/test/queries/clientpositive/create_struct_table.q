
create table abc(strct struct<a:int, b:string, c:string>)
row format delimited
  fields terminated by '\t'
  collection items terminated by '\001';

load data local inpath '../../data/files/kv1.txt'
overwrite into table abc;

SELECT strct, strct.a, strct.b FROM abc LIMIT 10;


