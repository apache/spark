-- Test cases for NEAREST BY top-K ranking join.

CREATE VIEW users(user_id, score) AS VALUES (1, 10.0), (2, 20.0), (3, 30.0);
CREATE VIEW products(product, pscore) AS VALUES ('A', 11.0), ('B', 22.0), ('C', 5.0);

-- Basic APPROX NEAREST BY SIMILARITY with k = 1
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- APPROX NEAREST BY DISTANCE with k = 2
SELECT u.user_id, p.product, p.pscore
FROM users u JOIN products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore);

-- EXACT NEAREST BY SIMILARITY with default k = 1
SELECT u.user_id, p.product
FROM users u INNER JOIN products p
  EXACT NEAREST BY SIMILARITY -abs(u.score - p.pscore);

-- LEFT OUTER JOIN with NEAREST BY, empty right side
SELECT u.user_id, p.product
FROM users u LEFT OUTER JOIN (SELECT * FROM products WHERE false) p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- Explicit INNER keyword
SELECT u.user_id, p.product
FROM users u INNER JOIN products p
  APPROX NEAREST 1 BY DISTANCE abs(u.score - p.pscore);

-- Error: unsupported join type (RIGHT OUTER)
SELECT u.user_id, p.product
FROM users u RIGHT OUTER JOIN products p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- Error: num_results out of range (0)
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 0 BY SIMILARITY -abs(u.score - p.pscore);

-- Error: num_results out of range (100001)
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 100001 BY SIMILARITY -abs(u.score - p.pscore);

-- Error: non-orderable ranking expression
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 1 BY SIMILARITY map(u.score, p.pscore);

-- Error: EXACT mode with nondeterministic ranking expression
SELECT u.user_id, p.product
FROM users u JOIN products p
  EXACT NEAREST 1 BY SIMILARITY rand() + p.pscore;

DROP VIEW users;
DROP VIEW products;
