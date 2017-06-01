-- Aliased subqueries in FROM clause
SELECT * FROM (SELECT * FROM testData) AS t WHERE key = 1;

FROM (SELECT * FROM testData WHERE key = 1) AS t SELECT *;

-- Optional `AS` keyword
SELECT * FROM (SELECT * FROM testData) t WHERE key = 1;

FROM (SELECT * FROM testData WHERE key = 1) t SELECT *;

-- Disallow unaliased subqueries in FROM clause
SELECT * FROM (SELECT * FROM testData) WHERE key = 1;

FROM (SELECT * FROM testData WHERE key = 1) SELECT *;
