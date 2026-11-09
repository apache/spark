-- test cases for map functions

-- key does not exist
select element_at(map(1, 'a', 2, 'b'), 5);
select map(1, 'a', 2, 'b')[5];
-- the key is out of the range of the map key type
select map(1Y, 'a', 44Y, 'b')[300], map(1Y, 'a', 44Y, 'b')[44];
select map(1S, 'a', 44S, 'b')[65580], map(1S, 'a', 44S, 'b')[44];
select map(1, 'a', 44, 'b')[4294967340L], map(1, 'a', 44, 'b')[44L];
select k, map(1Y, 'a', 44Y, 'b')[k] from values (44), (300), (-212), (null) as t(k);


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
