create database if not exists table_in_database_creation;
create table table_in_database_creation.test1  as select * from src limit 1;
create table `table_in_database_creation.test2` as select * from src limit 1;
create table table_in_database_creation.test3 (a string);
create table `table_in_database_creation.test4` (a string);
drop database table_in_database_creation cascade;