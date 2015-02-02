DESCRIBE FUNCTION current_database;

explain
select current_database();
select current_database();

create database xxx;
use xxx;

explain
select current_database();
select current_database();

set hive.fetch.task.conversion=more;

use default;

explain
select current_database();
select current_database();

use xxx;

explain
select current_database();
select current_database();
