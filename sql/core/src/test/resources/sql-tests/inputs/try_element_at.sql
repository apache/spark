-- array input
SELECT try_element_at(array(1, 2, 3), 0);
SELECT try_element_at(array(1, 2, 3), 1);
SELECT try_element_at(array(1, 2, 3), 3);
SELECT try_element_at(array(1, 2, 3), 4);
SELECT try_element_at(array(1, 2, 3), -1);
SELECT try_element_at(array(1, 2, 3), -4);
SELECT try_element_at(array(1, 2, 3), -2147483648);
-- non-literal index: avoids constant folding so the codegen path is exercised end-to-end
SELECT try_element_at(array(1, 2, 3), idx) from values (-2147483648) as t(idx);

-- map input
SELECT try_element_at(map('a','b'), 'a');
SELECT try_element_at(map('a','b'), 'abc');
