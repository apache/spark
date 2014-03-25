-- Ensure that UNION ALL columns are in the correct order on both sides
-- Ensure that the appropriate error message is propagated
CREATE TABLE IF NOT EXISTS union3  (bar int, baz int);
SELECT * FROM ( SELECT f.bar, f.baz FROM union3 f UNION ALL SELECT b.baz, b.bar FROM union3 b ) c;
DROP TABLE union3;
