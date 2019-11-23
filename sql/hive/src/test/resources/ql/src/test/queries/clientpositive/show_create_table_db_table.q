-- Test SHOW CREATE TABLE on a table name of format "db.table".

CREATE DATABASE tmp_feng comment 'for show create table test';
SHOW DATABASES;
CREATE TABLE tmp_feng.tmp_showcrt (key string, value int);
USE default;
SHOW CREATE TABLE tmp_feng.tmp_showcrt;
DROP TABLE tmp_feng.tmp_showcrt;
DROP DATABASE tmp_feng;

