-- test cases for map functions

-- key does not exist
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
