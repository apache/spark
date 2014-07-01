-- invalid argument type
SELECT sort_array(array(array(10, 20), array(5, 15), array(3, 13))) FROM src LIMIT 1;
