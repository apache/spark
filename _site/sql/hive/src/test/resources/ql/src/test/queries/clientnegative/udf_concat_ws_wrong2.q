-- invalid argument type
SELECT concat_ws('[]', array(100, 200, 50)) FROM src LIMIT 1;
