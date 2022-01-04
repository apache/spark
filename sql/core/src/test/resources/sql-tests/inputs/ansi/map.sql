--IMPORT map.sql

-- key does not exist
-- return null results if the map key in [] operator doesn't exist
set spark.sql.ansi.strictIndexOperator=false;
select map(1, 'a', 2, 'b')[5];
-- the configuration spark.sql.ansi.strictIndexOperator doesn't affect the function element_at
select element_at(map(1, 'a', 2, 'b'), 5);
