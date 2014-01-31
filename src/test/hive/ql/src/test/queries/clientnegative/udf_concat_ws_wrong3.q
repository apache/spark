-- invalid argument type
SELECT concat_ws(1234, array('www', 'facebook', 'com')) FROM src LIMIT 1;
