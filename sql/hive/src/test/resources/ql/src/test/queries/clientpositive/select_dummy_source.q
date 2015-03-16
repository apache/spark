explain
select 'a', 100;
select 'a', 100;

--evaluation
explain
select 1 + 1;
select 1 + 1;

-- explode (not possible for lateral view)
explain
select explode(array('a', 'b'));
select explode(array('a', 'b'));

set hive.fetch.task.conversion=more;

explain
select 'a', 100;
select 'a', 100;

explain
select 1 + 1;
select 1 + 1;

explain
select explode(array('a', 'b'));
select explode(array('a', 'b'));

-- subquery
explain
select 2 + 3,x from (select 1 + 2 x) X;
select 2 + 3,x from (select 1 + 2 x) X;

