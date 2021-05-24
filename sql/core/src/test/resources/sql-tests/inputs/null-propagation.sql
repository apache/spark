
-- count(null) should be 0
SELECT COUNT(NULL) FROM VALUES 1, 2, 3;
SELECT COUNT(1 + NULL) FROM VALUES 1, 2, 3;

-- count(null) on window should be 0
SELECT COUNT(NULL) OVER () FROM VALUES 1, 2, 3;
SELECT COUNT(1 + NULL) OVER () FROM VALUES 1, 2, 3;

