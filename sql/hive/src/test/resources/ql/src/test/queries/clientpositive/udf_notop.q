set hive.fetch.task.conversion=more;

SELECT 1 NOT IN (1, 2, 3),
       4 NOT IN (1, 2, 3),
       1 = 2 NOT IN (true, false),
       "abc" NOT LIKE "a%",
       "abc" NOT LIKE "b%",
       "abc" NOT RLIKE "^ab",
       "abc" NOT RLIKE "^bc",
       "abc" NOT REGEXP "^ab",
       "abc" NOT REGEXP "^bc",
       1 IN (1, 2) AND "abc" NOT LIKE "bc%" FROM src tablesample (1 rows);
