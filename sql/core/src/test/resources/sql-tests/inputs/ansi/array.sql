--IMPORT array.sql

-- index out of range for array elements
-- return null results if array index in [] operator is out of bound
set spark.sql.ansi.strictIndexOperator=false;
select array(1, 2, 3)[5];
select array(1, 2, 3)[-1];

-- the configuration spark.sql.ansi.strictIndexOperator doesn't affect the function element_at
select element_at(array(1, 2, 3), 5);
select element_at(array(1, 2, 3), -5);
select element_at(array(1, 2, 3), 0);

-- -- the configuration spark.sql.ansi.strictIndexOperator doesn't affect the function elt
select elt(4, '123', '456');
select elt(0, '123', '456');
select elt(-1, '123', '456');
