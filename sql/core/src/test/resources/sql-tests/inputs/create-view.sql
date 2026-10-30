-- Baseline for persistent CREATE [OR REPLACE] VIEW. Names are single-part (current schema).
-- Every object created here is dropped at the end of the file.
DROP VIEW IF EXISTS cv_plain;
DROP VIEW IF EXISTS cv_over_view;
DROP VIEW IF EXISTS cv_join;
DROP VIEW IF EXISTS cv_agg;
DROP VIEW IF EXISTS cv_window;
DROP VIEW IF EXISTS cv_union;
DROP VIEW IF EXISTS cv_scalar;
DROP VIEW IF EXISTS cv_exists;
DROP VIEW IF EXISTS cv_cols;
DROP VIEW IF EXISTS cv_rec;
DROP VIEW IF EXISTS cv_a;
DROP VIEW IF EXISTS cv_b;
DROP VIEW IF EXISTS cv_uses_temp_view;
DROP VIEW IF EXISTS cv_uses_temp_fn;
DROP VIEW IF EXISTS cv_uses_var;
DROP VIEW IF EXISTS cv_ident_var;
DROP VIEW IF EXISTS cv_ident_lit;
DROP VIEW IF EXISTS cv_ident_target;
DROP VIEW IF EXISTS cv_var_target;
DROP VIEW IF EXISTS cv_qualified;
DROP VIEW IF EXISTS cv_shadow;
DROP VIEW IF EXISTS cv_cte;
DROP VIEW IF EXISTS cv_cte_twice;
DROP VIEW IF EXISTS cv_cte_nested;
DROP VIEW IF EXISTS cv_meta;
DROP VIEW IF EXISTS cv_collate;
DROP VIEW IF EXISTS cv_exec;
DROP VIEW IF EXISTS cv_using;
DROP VIEW IF EXISTS cv_natural;
DROP VIEW IF EXISTS cv_file_meta;
DROP VIEW IF EXISTS cv_coll_values;
DROP VIEW IF EXISTS cv_coll_cast;
DROP VIEW IF EXISTS cv_coll_outer;
DROP VIEW IF EXISTS cv_coll_inner;
DROP VIEW IF EXISTS cv_two_fns;
DROP VIEW IF EXISTS cv_fn_subquery;
DROP VIEW IF EXISTS cv_fn_lambda;
DROP VIEW IF EXISTS cv_fn_cte;
DROP VIEW IF EXISTS cv_expr_target;
DROP VIEW IF EXISTS cv_tgt_sfx;
DROP VIEW IF EXISTS cv_tgt_sfx_legacy;
DROP VIEW IF EXISTS cv_rel_sub;
DROP VIEW IF EXISTS cv_rel_cte;
DROP VIEW IF EXISTS cv_rel_alias;
DROP VIEW IF EXISTS cv_col_const;
DROP VIEW IF EXISTS cv_fn_const;
DROP VIEW IF EXISTS cv_tvf_const;
DROP VIEW IF EXISTS cv_ident_var_legacy;
DROP VIEW IF EXISTS cv_col_var;
DROP VIEW IF EXISTS cv_ident_var_sub;
DROP TABLE IF EXISTS cv_t1;
DROP TABLE IF EXISTS cv_t2;
DROP TABLE IF EXISTS cv_k1;
DROP TABLE IF EXISTS cv_k2;
DROP TABLE IF EXISTS cv_file;

CREATE TABLE cv_t1 (id INT, name STRING, grp INT);
CREATE TABLE cv_t2 (id INT, score INT);
INSERT INTO cv_t1 VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2);
INSERT INTO cv_t2 VALUES (1, 10), (2, 20), (4, 40);

-- Plain view, view over view.
CREATE VIEW cv_plain AS SELECT id, name FROM cv_t1 WHERE grp = 1;
SELECT * FROM cv_plain ORDER BY id;

CREATE VIEW cv_over_view AS SELECT id * 10 AS scaled FROM cv_plain;
SELECT * FROM cv_over_view ORDER BY scaled;

-- OR REPLACE changes the schema of the view; the new schema is what SELECT returns.
CREATE OR REPLACE VIEW cv_plain AS SELECT id, name, grp FROM cv_t1;
SELECT * FROM cv_plain ORDER BY id;

-- Body shapes: join, aggregate with alias, window, union, scalar subquery, EXISTS.
CREATE VIEW cv_join AS SELECT a.id, a.name, b.score FROM cv_t1 a JOIN cv_t2 b ON a.id = b.id;
SELECT * FROM cv_join ORDER BY id;

CREATE VIEW cv_agg AS SELECT grp, count(*) AS cnt, sum(id) AS total FROM cv_t1 GROUP BY grp;
SELECT * FROM cv_agg ORDER BY grp;

CREATE VIEW cv_window AS SELECT id, row_number() OVER (ORDER BY id) AS rn FROM cv_t1;
SELECT * FROM cv_window ORDER BY id;

CREATE VIEW cv_union AS SELECT id FROM cv_t1 UNION ALL SELECT id FROM cv_t2;
SELECT * FROM cv_union ORDER BY id;

CREATE VIEW cv_scalar AS SELECT id, (SELECT max(score) FROM cv_t2) AS max_score FROM cv_t1;
SELECT * FROM cv_scalar ORDER BY id;

CREATE VIEW cv_exists AS SELECT id FROM cv_t1 t WHERE EXISTS (SELECT 1 FROM cv_t2 s WHERE s.id = t.id);
SELECT * FROM cv_exists ORDER BY id;

-- Dependent view after OR REPLACE removes a column that cv_over_view does not use.
CREATE OR REPLACE VIEW cv_plain AS SELECT id FROM cv_t1 WHERE grp = 1;
SELECT * FROM cv_over_view ORDER BY scaled;

-- Root shapes that depend on how the body's top-level operator is handled: USING and NATURAL
-- joins, and ORDER BY with LIMIT over a file-based table filtered on _metadata.file_name.
CREATE TABLE cv_k1 (k INT, a STRING);
CREATE TABLE cv_k2 (k INT, b INT);
INSERT INTO cv_k1 VALUES (1, 'x'), (2, 'y');
INSERT INTO cv_k2 VALUES (1, 10), (3, 30);

CREATE VIEW cv_using AS SELECT * FROM cv_k1 JOIN cv_k2 USING (k);
SELECT * FROM cv_using;
DESCRIBE cv_using;

CREATE VIEW cv_natural AS SELECT * FROM cv_k1 NATURAL JOIN cv_k2;
SELECT * FROM cv_natural;
DESCRIBE cv_natural;

CREATE TABLE cv_file (id INT, v STRING) USING parquet;
INSERT INTO cv_file VALUES (3, 'c'), (1, 'a'), (2, 'b');

CREATE VIEW cv_file_meta AS
  SELECT id, v FROM cv_file WHERE _metadata.file_name LIKE 'part-%' ORDER BY id LIMIT 2;
SELECT * FROM cv_file_meta;
DESCRIBE cv_file_meta;

-- Column list: success with column comments; too few, too many, duplicate user names.
CREATE VIEW cv_cols (x COMMENT 'first column', y) AS SELECT id, name FROM cv_t1;
SELECT * FROM cv_cols ORDER BY x;

CREATE VIEW cv_cols_few (x, y, z) AS SELECT id, name FROM cv_t1;

CREATE VIEW cv_cols_many (x) AS SELECT id, name FROM cv_t1;

CREATE VIEW cv_cols_dup (a, a) AS SELECT id, name FROM cv_t1;

-- Duplicate output names in the body.
CREATE VIEW cv_body_dup AS SELECT 1 AS a, 2 AS a;

-- Existing objects: view over view, view over table, IF NOT EXISTS, OR REPLACE over a table.
CREATE VIEW cv_plain AS SELECT 1 AS other;

CREATE VIEW cv_t1 AS SELECT 1 AS other;

CREATE VIEW IF NOT EXISTS cv_plain AS SELECT 99 AS other;
SELECT * FROM cv_plain ORDER BY id;

CREATE VIEW IF NOT EXISTS cv_t1 AS SELECT 1 AS other;
SELECT COUNT(*) FROM cv_t1;

CREATE OR REPLACE VIEW cv_t1 AS SELECT 1 AS other;
SELECT COUNT(*) FROM cv_t1;

-- Recursive view: self reference through OR REPLACE, and a two-view cycle.
CREATE VIEW cv_rec AS SELECT 1 AS n;
CREATE OR REPLACE VIEW cv_rec AS SELECT n + 1 AS n FROM cv_rec;

CREATE VIEW cv_a AS SELECT 1 AS x;
CREATE VIEW cv_b AS SELECT x FROM cv_a;
CREATE OR REPLACE VIEW cv_a AS SELECT x FROM cv_b;

-- Temp objects referenced from a persistent view body.
CREATE TEMPORARY VIEW cv_tmp_view AS SELECT 1 AS a;
CREATE VIEW cv_uses_temp_view AS SELECT * FROM cv_tmp_view;
DROP VIEW cv_tmp_view;

CREATE TEMPORARY FUNCTION cv_tmp_fn() RETURNS INT RETURN 1;
CREATE VIEW cv_uses_temp_fn AS SELECT cv_tmp_fn() AS f;
DROP TEMPORARY FUNCTION cv_tmp_fn;

DECLARE VARIABLE cv_var INT DEFAULT 1;
CREATE VIEW cv_uses_var AS SELECT cv_var AS v;

DECLARE VARIABLE cv_src STRING DEFAULT 'cv_t1';
CREATE VIEW cv_ident_var AS SELECT id FROM IDENTIFIER(cv_src);

-- Two temp functions in one body; a temp function inside a scalar subquery, a lambda, and a CTE.
CREATE TEMPORARY FUNCTION cv_tf1() RETURNS INT RETURN 1;
CREATE TEMPORARY FUNCTION cv_tf2(x INT) RETURNS INT RETURN x + 1;

CREATE VIEW cv_two_fns AS SELECT cv_tf1() AS a, cv_tf2(1) AS b;
CREATE VIEW cv_fn_subquery AS SELECT (SELECT cv_tf2(1)) AS s;
CREATE VIEW cv_fn_lambda AS SELECT transform(array(1), x -> x + cv_tf1()) AS t;
CREATE VIEW cv_fn_cte AS WITH c AS (SELECT cv_tf1() AS v) SELECT v FROM c;

DROP TEMPORARY FUNCTION cv_tf1;
DROP TEMPORARY FUNCTION cv_tf2;

-- Foldable non-variable identifier in the body.
CREATE VIEW cv_ident_lit AS SELECT id FROM IDENTIFIER('cv_' || 't1');
SELECT * FROM cv_ident_lit ORDER BY id;

-- Targets: IDENTIFIER('lit'), IDENTIFIER(variable), default.-qualified name.
CREATE VIEW IDENTIFIER('cv_ident_target') AS SELECT 1 AS a;
SELECT * FROM cv_ident_target;

DECLARE VARIABLE cv_target STRING DEFAULT 'cv_var_target';
CREATE VIEW IDENTIFIER(cv_target) AS SELECT 2 AS a;
SELECT * FROM cv_var_target;

CREATE VIEW default.cv_qualified AS SELECT 3 AS a;
SELECT * FROM default.cv_qualified;

-- Constant IDENTIFIER in the body and the target: relation, CTE, alias, column, function, TVF.
CREATE VIEW IDENTIFIER('cv_' || 'expr_target') AS SELECT 1 AS a;
SELECT * FROM cv_expr_target;

DECLARE VARIABLE cv_tgt_sfx STRING DEFAULT 'sfx';
CREATE VIEW IDENTIFIER('cv_tgt_' || cv_tgt_sfx) AS SELECT 2 AS a;
SELECT * FROM cv_tgt_sfx;

CREATE VIEW cv_rel_sub AS SELECT x.id FROM (SELECT id FROM IDENTIFIER('cv_t' || '1')) AS x;
SELECT * FROM cv_rel_sub ORDER BY id;

CREATE VIEW cv_rel_cte AS WITH c AS (SELECT id FROM IDENTIFIER('cv_' || 't1')) SELECT id FROM c;
SELECT * FROM cv_rel_cte ORDER BY id;

CREATE VIEW cv_rel_alias AS SELECT a.id FROM IDENTIFIER('cv_t1') AS a;
SELECT * FROM cv_rel_alias ORDER BY id;

CREATE VIEW cv_col_const AS SELECT IDENTIFIER('i' || 'd') AS c FROM cv_t1;
SELECT * FROM cv_col_const ORDER BY c;

CREATE VIEW cv_fn_const AS SELECT IDENTIFIER('ab' || 's')(-1) AS a;
SELECT * FROM cv_fn_const;

CREATE VIEW cv_tvf_const AS SELECT * FROM IDENTIFIER('ran' || 'ge')(0, 2);
SELECT * FROM cv_tvf_const ORDER BY id;

-- The legacy configuration allows variable identifiers in the body. Variable targets remain
-- valid, while reading the persisted body still requires a constant identifier expression.
SET spark.sql.legacy.allowSessionVariableInPersistedView=true;
CREATE VIEW cv_ident_var_legacy AS SELECT id FROM IDENTIFIER(cv_src);
SELECT * FROM cv_ident_var_legacy ORDER BY id;
CREATE VIEW IDENTIFIER('cv_tgt_' || cv_tgt_sfx || '_legacy') AS SELECT 3 AS a;
RESET spark.sql.legacy.allowSessionVariableInPersistedView;

-- Without the legacy configuration, variable identifiers in columns, functions, relations,
-- scalar subqueries, and CTEs are rejected as temporary object references.
DECLARE VARIABLE cv_col_name STRING DEFAULT 'id';
CREATE VIEW cv_col_var AS SELECT IDENTIFIER(cv_col_name) FROM cv_t1;

DECLARE VARIABLE cv_fn_name STRING DEFAULT 'abs';
CREATE VIEW cv_fn_var AS SELECT IDENTIFIER(cv_fn_name)(-1) AS a;

CREATE VIEW cv_ident_var_sub AS SELECT (SELECT max(id) FROM IDENTIFIER(cv_src)) AS m;

CREATE VIEW cv_ident_var_cte AS WITH c AS (SELECT id FROM IDENTIFIER(cv_src)) SELECT id FROM c;

DECLARE VARIABLE cv_idx_str STRING DEFAULT '1';
CREATE VIEW cv_ident_var_concat AS SELECT id FROM IDENTIFIER('cv_t' || cv_idx_str);

DROP TEMPORARY VARIABLE cv_tgt_sfx;
DROP TEMPORARY VARIABLE cv_col_name;
DROP TEMPORARY VARIABLE cv_fn_name;
DROP TEMPORARY VARIABLE cv_idx_str;

-- A persistent view whose name equals an existing temp view. SELECT and
-- DROP hit the temp view first; after the temp view is dropped, the persistent view is visible.
CREATE TEMPORARY VIEW cv_shadow AS SELECT 'temp' AS src;
CREATE VIEW cv_shadow AS SELECT 'persistent' AS src;
SELECT * FROM cv_shadow;
DROP VIEW cv_shadow;
SELECT * FROM cv_shadow;
DROP VIEW cv_shadow;

-- CTE in the view body: single CTE, CTE referenced twice, nested WITH.
CREATE VIEW cv_cte AS WITH c AS (SELECT id FROM cv_t1) SELECT id FROM c;
SELECT * FROM cv_cte ORDER BY id;

CREATE VIEW cv_cte_twice AS
  WITH c AS (SELECT id FROM cv_t1 WHERE grp = 1)
  SELECT a.id, b.id AS id2 FROM c a JOIN c b ON a.id = b.id;
SELECT * FROM cv_cte_twice ORDER BY id;

CREATE VIEW cv_cte_nested AS
  WITH outer_c AS (WITH inner_c AS (SELECT 5 AS x) SELECT x FROM inner_c)
  SELECT x FROM outer_c;
SELECT * FROM cv_cte_nested;

-- Metadata: column list with comments, view comment, table properties, default collation,
-- and schema binding in one statement.
CREATE VIEW cv_meta (id COMMENT 'row id', label COMMENT 'label text')
  COMMENT 'metadata baseline'
  TBLPROPERTIES ('owner_team' = 'sql', 'purpose' = 'baseline')
  DEFAULT COLLATION UTF8_LCASE
  WITH SCHEMA BINDING
  AS SELECT id, name AS label FROM cv_t1;
DESCRIBE cv_meta;
SHOW TBLPROPERTIES cv_meta ('owner_team');
SELECT * FROM cv_meta ORDER BY id;

-- String literal in the body of a DEFAULT COLLATION view is compared case-insensitively.
CREATE VIEW cv_collate DEFAULT COLLATION UTF8_LCASE AS SELECT 'ABC' AS s;
SELECT COUNT(*) FROM cv_collate WHERE s = 'abc';

-- DEFAULT COLLATION UTF8_LCASE is retained on VALUES and CAST output columns.
CREATE VIEW cv_coll_values DEFAULT COLLATION UTF8_LCASE AS
  SELECT s FROM VALUES ('ABC'), ('def') AS t(s);
SELECT COUNT(*) FROM cv_coll_values WHERE s = 'abc';

CREATE VIEW cv_coll_cast DEFAULT COLLATION UTF8_LCASE AS
  SELECT CAST(s AS STRING) AS cs FROM VALUES ('ABC') AS t(s);
SELECT COUNT(*) FROM cv_coll_cast WHERE cs = 'abc';

-- A view with its own DEFAULT COLLATION, referenced from an outer view with a different one.
CREATE VIEW cv_coll_inner DEFAULT COLLATION UNICODE AS SELECT 'ABC' AS s;
CREATE VIEW cv_coll_outer DEFAULT COLLATION UTF8_LCASE AS SELECT s FROM cv_coll_inner;
SELECT COUNT(*) FROM cv_coll_outer WHERE s = 'abc';
SELECT DISTINCT COLLATION(s) FROM cv_coll_outer;

-- Persistent view output expressions require explicit aliases unless the legacy setting is on.
SET spark.sql.legacy.allowAutoGeneratedAliasForView=false;
CREATE VIEW cv_auto AS SELECT count(*) FROM cv_t1;
CREATE VIEW cv_auto_union AS SELECT count(*) FROM cv_t1 UNION ALL SELECT 1;
CREATE VIEW cv_auto_corr AS
  SELECT id, (SELECT count(*) FROM cv_t1 s WHERE s.id = t.id) FROM cv_t1 t;
CREATE VIEW cv_auto_corr_aliased AS
  SELECT id, (SELECT count(*) FROM cv_t1 s WHERE s.id = t.id) AS cnt FROM cv_t1 t;
SELECT * FROM cv_auto_corr_aliased ORDER BY id;

SET spark.sql.legacy.allowAutoGeneratedAliasForView=true;
CREATE VIEW cv_auto AS SELECT count(*) FROM cv_t1;
SELECT * FROM cv_auto;
CREATE VIEW cv_auto_union AS SELECT count(*) FROM cv_t1 UNION ALL SELECT 1;
SELECT * FROM cv_auto_union ORDER BY 1;
CREATE VIEW cv_auto_corr AS
  SELECT id, (SELECT count(*) FROM cv_t1 s WHERE s.id = t.id) FROM cv_t1 t;
SELECT * FROM cv_auto_corr ORDER BY id;
RESET spark.sql.legacy.allowAutoGeneratedAliasForView;

-- EXECUTE IMMEDIATE and plain EXPLAIN. EXPLAIN does not create the view.
EXECUTE IMMEDIATE 'CREATE VIEW cv_exec AS SELECT 1 AS a';
SELECT * FROM cv_exec;

EXPLAIN CREATE VIEW cv_explain AS SELECT 1 AS a;
SELECT * FROM cv_explain;

-- Parser-level one-liner: IF NOT EXISTS combined with OR REPLACE.
CREATE OR REPLACE VIEW IF NOT EXISTS cv_plain AS SELECT 1 AS a;

DROP VIEW IF EXISTS cv_plain;
DROP VIEW IF EXISTS cv_over_view;
DROP VIEW IF EXISTS cv_join;
DROP VIEW IF EXISTS cv_agg;
DROP VIEW IF EXISTS cv_window;
DROP VIEW IF EXISTS cv_union;
DROP VIEW IF EXISTS cv_scalar;
DROP VIEW IF EXISTS cv_exists;
DROP VIEW IF EXISTS cv_cols;
DROP VIEW IF EXISTS cv_rec;
DROP VIEW IF EXISTS cv_a;
DROP VIEW IF EXISTS cv_b;
DROP VIEW IF EXISTS cv_uses_temp_view;
DROP VIEW IF EXISTS cv_uses_temp_fn;
DROP VIEW IF EXISTS cv_uses_var;
DROP VIEW IF EXISTS cv_ident_var;
DROP VIEW IF EXISTS cv_two_fns;
DROP VIEW IF EXISTS cv_fn_subquery;
DROP VIEW IF EXISTS cv_fn_lambda;
DROP VIEW IF EXISTS cv_fn_cte;
DROP VIEW IF EXISTS cv_ident_lit;
DROP VIEW IF EXISTS cv_ident_target;
DROP VIEW IF EXISTS cv_var_target;
DROP VIEW IF EXISTS cv_qualified;
DROP VIEW IF EXISTS cv_cte;
DROP VIEW IF EXISTS cv_cte_twice;
DROP VIEW IF EXISTS cv_cte_nested;
DROP VIEW IF EXISTS cv_meta;
DROP VIEW IF EXISTS cv_collate;
DROP VIEW IF EXISTS cv_coll_values;
DROP VIEW IF EXISTS cv_coll_cast;
DROP VIEW IF EXISTS cv_coll_outer;
DROP VIEW IF EXISTS cv_coll_inner;
DROP VIEW IF EXISTS cv_using;
DROP VIEW IF EXISTS cv_natural;
DROP VIEW IF EXISTS cv_file_meta;
DROP VIEW IF EXISTS cv_exec;
DROP VIEW IF EXISTS cv_expr_target;
DROP VIEW IF EXISTS cv_tgt_sfx;
DROP VIEW IF EXISTS cv_tgt_sfx_legacy;
DROP VIEW IF EXISTS cv_rel_sub;
DROP VIEW IF EXISTS cv_rel_cte;
DROP VIEW IF EXISTS cv_rel_alias;
DROP VIEW IF EXISTS cv_col_const;
DROP VIEW IF EXISTS cv_fn_const;
DROP VIEW IF EXISTS cv_tvf_const;
DROP VIEW IF EXISTS cv_ident_var_legacy;
DROP VIEW IF EXISTS cv_col_var;
DROP VIEW IF EXISTS cv_ident_var_sub;
DROP VIEW IF EXISTS cv_auto;
DROP VIEW IF EXISTS cv_auto_union;
DROP VIEW IF EXISTS cv_auto_corr;
DROP VIEW IF EXISTS cv_auto_corr_aliased;
DROP TEMPORARY VARIABLE cv_var;
DROP TEMPORARY VARIABLE cv_src;
DROP TEMPORARY VARIABLE cv_target;
DROP TABLE IF EXISTS cv_t1;
DROP TABLE IF EXISTS cv_t2;
DROP TABLE IF EXISTS cv_k1;
DROP TABLE IF EXISTS cv_k2;
DROP TABLE IF EXISTS cv_file;
