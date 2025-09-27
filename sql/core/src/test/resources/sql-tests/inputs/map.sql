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


-- Check if catalyst combine nested `map_concat`s
select map_concat();
select map_concat(map_concat());
select map_concat(map('a', 1));
select map_concat(map('a', 1), map_concat());
select map_concat(col1, map_concat(col2, col3), col4) col
                 from (select map('a', id) col1, map('b', id) col2, map('c', id) col3, map('d', id) col4 from range(10));
explain extended select map_concat(col1, map_concat(col2, col3), col4) col
                 from (select map('a', id) col1, map('b', id) col2, map('c', id) col3, map('d', id) col4 from range(10));

set spark.sql.mapKeyDedupPolicy = LAST_WIN;
select map_concat(col1, map_concat(col2, col3), col4) col
    from (select map('a', id) col1, map('a', id + 1) col2, map('a', id + 2) col3, map('a', id + 3) col4 from range(1));
set spark.sql.mapKeyDedupPolicy = EXCEPTION;

