-- invalid argument(second argument should be >= 0)
SELECT format_number(12332.123456, -4) FROM src LIMIT 1;
