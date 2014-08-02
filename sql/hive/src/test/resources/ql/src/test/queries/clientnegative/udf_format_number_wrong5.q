-- invalid argument type
SELECT format_number(array(12332.123456, 321.23), 5) FROM src LIMIT 1;
