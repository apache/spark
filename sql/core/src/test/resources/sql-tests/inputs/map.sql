-- test cases for map functions

-- key does not exist
-- the following queries should return null results.
set spark.sql.ansi.failOnElementNotExists=false;
select element_at(map(1, 'a', 2, 'b'), 5);
select map(1, 'a', 2, 'b')[5];

-- the following queries should throw exceptions under ANSI mode.
set spark.sql.ansi.failOnElementNotExists=true;
select element_at(map(1, 'a', 2, 'b'), 5);
select map(1, 'a', 2, 'b')[5];

-- map_contains_key
select map_contains_key(map(1, 'a', 2, 'b'), 5);
select map_contains_key(map(1, 'a', 2, 'b'), 1);
-- map_contains_key: input type is different from the key type
select map_contains_key(map(1, 'a', 2, 'b'), 5.0);
select map_contains_key(map(1, 'a', 2, 'b'), 1.0);
select map_contains_key(map(1.0, 'a', 2, 'b'), 5);
select map_contains_key(map(1.0, 'a', 2, 'b'), 1);
select map_contains_key(map('1', 'a', '2', 'b'), 1);
select map_contains_key(map(1, 'a', 2, 'b'), '1');
