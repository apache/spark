-- Test SHOW CREATE TABLE on a view name.

CREATE VIEW tmp_copy_src AS SELECT * FROM src;
SHOW CREATE TABLE tmp_copy_src;
DROP VIEW tmp_copy_src;

