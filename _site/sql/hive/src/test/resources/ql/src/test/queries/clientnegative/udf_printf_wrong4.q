-- invalid argument type
SELECT printf("Hello World %s", array("invalid", "argument")) FROM src LIMIT 1;
