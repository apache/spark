explain create table abc(mydata uniontype<int,double,array<string>,struct<a:int,b:string>>,
strct struct<a:int, b:string, c:string>);

create table abc(mydata uniontype<int,double,array<string>,struct<a:int,b:string>>,
strct struct<a:int, b:string, c:string>);

load data local inpath '../../data/files/union_input.txt'
overwrite into table abc;

SELECT * FROM abc;
