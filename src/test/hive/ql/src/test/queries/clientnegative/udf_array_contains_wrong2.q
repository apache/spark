-- invalid second argument
SELECT array_contains(array(1, 2, 3), '2') FROM src LIMIT 1;
