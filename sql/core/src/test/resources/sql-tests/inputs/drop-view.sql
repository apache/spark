-- DROP VIEW on a temporary view, then IF EXISTS and DROP VIEW on the missing view.
CREATE TEMPORARY VIEW drop_view_v1 AS SELECT 1 AS c;
DROP VIEW drop_view_v1;
DROP VIEW IF EXISTS drop_view_v1;
DROP VIEW drop_view_v1;

-- DROP VIEW on a missing view, with and without IF EXISTS.
DROP VIEW drop_view_missing;
DROP VIEW IF EXISTS drop_view_missing;

-- DROP VIEW on a global temporary view, then IF EXISTS after it is gone.
CREATE GLOBAL TEMPORARY VIEW drop_view_gv1 AS SELECT 1 AS c;
DROP VIEW global_temp.drop_view_gv1;
DROP VIEW IF EXISTS global_temp.drop_view_gv1;

-- DROP VIEW on a persistent view, unqualified and with a database qualifier.
CREATE VIEW drop_view_pv1 AS SELECT 1 AS c;
DROP VIEW drop_view_pv1;
CREATE VIEW drop_view_pv2 AS SELECT 2 AS c;
DROP VIEW default.drop_view_pv2;

-- A temporary view shadows a persistent view of the same name. The unqualified DROP VIEW
-- drops the temporary view, and the qualified DROP VIEW drops the persistent view.
CREATE VIEW drop_view_sh AS SELECT 2 AS c;
CREATE TEMPORARY VIEW drop_view_sh AS SELECT 1 AS c;
SELECT * FROM drop_view_sh;
DROP VIEW drop_view_sh;
SELECT * FROM drop_view_sh;
DROP VIEW default.drop_view_sh;

-- DROP VIEW and DROP VIEW IF EXISTS on a table.
CREATE TABLE drop_view_t1 (c INT) USING parquet;
DROP VIEW drop_view_t1;
DROP VIEW IF EXISTS drop_view_t1;
DROP TABLE drop_view_t1;

-- DROP VIEW with an IDENTIFIER clause, with a literal and with a concatenated string.
CREATE VIEW drop_view_id1 AS SELECT 1 AS c;
DROP VIEW IDENTIFIER('drop_view_id1');
CREATE VIEW drop_view_concat AS SELECT 1 AS c;
DROP VIEW IDENTIFIER('drop_view_' || 'concat');

-- DROP VIEW with an IDENTIFIER clause over a session variable.
CREATE VIEW drop_view_var_v AS SELECT 1 AS c;
DECLARE VARIABLE drop_view_var STRING DEFAULT 'drop_view_var_v';
DROP VIEW IDENTIFIER(drop_view_var);
DROP TEMPORARY VARIABLE drop_view_var;

-- DROP VIEW with IDENTIFIER arguments that are not a constant non-null string.
DROP VIEW IDENTIFIER(1);
DROP VIEW IDENTIFIER(cast(null AS STRING));
DROP VIEW IDENTIFIER(rand());
DROP VIEW IF EXISTS IDENTIFIER(1);

-- DROP VIEW with a global_temp name that has more than two parts, with and without IF EXISTS.
DROP VIEW global_temp.drop_view_a.drop_view_b;
DROP VIEW IF EXISTS global_temp.drop_view_a.drop_view_b;

-- EXPLAIN of DROP VIEW IF EXISTS on a missing view.
EXPLAIN DROP VIEW IF EXISTS drop_view_missing;

-- DROP VIEW on session-qualified temporary views.
CREATE TEMPORARY VIEW drop_view_s1 AS SELECT 1 AS c;
DROP VIEW session.drop_view_s1;
DROP VIEW IF EXISTS drop_view_s1;
CREATE TEMPORARY VIEW drop_view_s2 AS SELECT 1 AS c;
DROP VIEW system.session.drop_view_s2;
DROP VIEW IF EXISTS drop_view_s2;
