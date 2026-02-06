-- Test max_by and min_by aggregate functions with k parameter

-- Test data
CREATE OR REPLACE TEMPORARY VIEW basic_data AS SELECT * FROM VALUES
  ('Alice', 85),
  ('Bob', 92),
  ('Carol', 78),
  ('Dave', 95),
  ('Eve', 88)
AS basic_data(name, score);

-- Basic max_by with k
SELECT max_by(name, score, 3) FROM basic_data;

-- Basic min_by with k
SELECT min_by(name, score, 3) FROM basic_data;

-- k = 1 (should return array with single element)
SELECT max_by(name, score, 1) FROM basic_data;
SELECT min_by(name, score, 1) FROM basic_data;

-- k larger than row count (should return all elements)
SELECT max_by(name, score, 10) FROM basic_data;
SELECT min_by(name, score, 10) FROM basic_data;

-- GROUP BY test data
CREATE OR REPLACE TEMPORARY VIEW dept_data AS SELECT * FROM VALUES
  ('Eng', 'Alice', 120000),
  ('Eng', 'Bob', 95000),
  ('Eng', 'Carol', 110000),
  ('Sales', 'Dave', 80000),
  ('Sales', 'Eve', 75000),
  ('Sales', 'Frank', 85000)
AS dept_data(dept, emp, salary);

-- max_by with GROUP BY
SELECT dept, max_by(emp, salary, 2) FROM dept_data GROUP BY dept ORDER BY dept;

-- min_by with GROUP BY
SELECT dept, min_by(emp, salary, 2) FROM dept_data GROUP BY dept ORDER BY dept;

-- NULL handling: NULL ordering values are skipped
CREATE OR REPLACE TEMPORARY VIEW null_data AS SELECT * FROM VALUES
  ('a', 10),
  ('b', NULL),
  ('c', 30),
  ('d', 20)
AS null_data(x, y);

SELECT max_by(x, y, 2) FROM null_data;
SELECT min_by(x, y, 2) FROM null_data;

-- NULL values (not ordering) are preserved
CREATE OR REPLACE TEMPORARY VIEW null_value_data AS SELECT * FROM VALUES
  (NULL, 10),
  ('b', 20),
  ('c', 30)
AS null_value_data(x, y);

SELECT max_by(x, y, 2) FROM null_value_data;
SELECT min_by(x, y, 2) FROM null_value_data;

-- Different data types for ordering
CREATE OR REPLACE TEMPORARY VIEW typed_data AS SELECT * FROM VALUES
  ('a', 1.5),
  ('b', 2.5),
  ('c', 0.5)
AS typed_data(name, score);

SELECT max_by(name, score, 2) FROM typed_data;
SELECT min_by(name, score, 2) FROM typed_data;

-- Date ordering
CREATE OR REPLACE TEMPORARY VIEW date_data AS SELECT * FROM VALUES
  ('event1', DATE '2024-01-15'),
  ('event2', DATE '2024-03-20'),
  ('event3', DATE '2024-02-10')
AS date_data(event, event_date);

SELECT max_by(event, event_date, 2) FROM date_data;
SELECT min_by(event, event_date, 2) FROM date_data;

-- Window function test
SELECT dept, emp, salary,
       max_by(emp, salary, 2) OVER (PARTITION BY dept) as top2,
       min_by(emp, salary, 2) OVER (PARTITION BY dept) as bottom2
FROM dept_data
ORDER BY dept, emp;

-- Error case: k must be positive
SELECT max_by(name, score, 0) FROM basic_data;

-- Error case: k must be positive (negative)
SELECT max_by(name, score, -1) FROM basic_data;

-- Error case: k exceeds maximum limit (100000)
SELECT max_by(name, score, 100001) FROM basic_data;

-- Cleanup
DROP VIEW basic_data;
DROP VIEW dept_data;
DROP VIEW null_data;
DROP VIEW null_value_data;
DROP VIEW typed_data;
DROP VIEW date_data;
