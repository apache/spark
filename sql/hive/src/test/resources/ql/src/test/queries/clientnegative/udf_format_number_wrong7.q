-- invalid argument type(format_number returns the result as a string)
SELECT format_number(format_number(12332.123456, 4), 2) FROM src LIMIT 1;
