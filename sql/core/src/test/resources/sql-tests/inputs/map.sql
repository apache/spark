-- test cases for map functions

-- key does not exist
select element_at(map(1, 'a', 2, 'b'), 5);
select map(1, 'a', 2, 'b')[5];
