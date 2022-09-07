-- array input
SELECT try_element_at(array(1, 2, 3), 0);
SELECT try_element_at(array(1, 2, 3), 1);
SELECT try_element_at(array(1, 2, 3), 3);
SELECT try_element_at(array(1, 2, 3), 4);
SELECT try_element_at(array(1, 2, 3), -1);
SELECT try_element_at(array(1, 2, 3), -4);

-- map input
SELECT try_element_at(map('a','b'), 'a');
SELECT try_element_at(map('a','b'), 'abc');
