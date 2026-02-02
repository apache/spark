set hive.fetch.task.conversion=more;

describe function between;
describe function extended between;

explain SELECT * FROM src where key + 100 between (150 + -50) AND (150 + 50) LIMIT 20;
SELECT * FROM src where key + 100 between (150 + -50) AND (150 + 50) LIMIT 20;

explain SELECT * FROM src where key + 100 not between (150 + -50) AND (150 + 50) LIMIT 20;
SELECT * FROM src where key + 100 not between (150 + -50) AND (150 + 50) LIMIT 20;

explain SELECT * FROM src where 'b' between 'a' AND 'c' LIMIT 1;
SELECT * FROM src where 'b' between 'a' AND 'c' LIMIT 1;

explain SELECT * FROM src where 2 between 2 AND '3' LIMIT 1;
SELECT * FROM src where 2 between 2 AND '3' LIMIT 1;
