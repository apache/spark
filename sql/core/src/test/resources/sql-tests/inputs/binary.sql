--SET spark.sql.binaryOutputStyle=UTF-8

SELECT X'';
SELECT X'4561736F6E2059616F20323031382D31312D31373A31333A33333A3333';
SELECT CAST('Spark' as BINARY);
SELECT array( X'', X'4561736F6E2059616F20323031382D31312D31373A31333A33333A3333', CAST('Spark' as BINARY));
SELECT to_csv(named_struct('n', 1, 'info', X'4561736F6E2059616F20323031382D31312D31373A31333A33333A3333'));
select to_xml(named_struct('name', binary('Eason'), 'birth', 2018, 'org', binary('Kindergarten Cop')));
SELECT from_xml(
         to_xml(named_struct('name', binary('Eason'), 'birth', 2018, 'org', binary('Kindergarten Cop'))),
         'name STRING, birth INT, org STRING');
SELECT from_xml(
         to_xml(named_struct('name', binary('Eason'), 'birth', 2018, 'org', binary('Kindergarten Cop'))),
         'name binary, birth INT, org binary');
