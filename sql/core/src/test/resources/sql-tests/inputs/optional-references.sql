-- Tests for optional column, field, variable, and named parameter references: a `?` after an
-- identifier makes that identifier optional, so that a reference that cannot be resolved
-- evaluates to a static NULL of type VOID instead of raising a not-found error.

--SET spark.sql.optionalReferences.enabled=true

CREATE TEMPORARY VIEW people AS SELECT * FROM VALUES
  (1, named_struct('city', 'SF', 'zip', '94105')),
  (2, named_struct('city', 'NYC', 'zip', '10001'))
  AS t(id, addr);

CREATE TEMPORARY VIEW orgs AS SELECT * FROM VALUES
  (1, named_struct('addr', named_struct('city', 'SF', 'zip', '94105')))
  AS t(id, info);

CREATE TEMPORARY VIEW orders AS SELECT * FROM VALUES
  (1, array(named_struct('sku', 'a', 'qty', 5))),
  (2, array(named_struct('sku', 'b', 'qty', 9)))
  AS t(id, items);

-- A missing column resolves to NULL and an existing column resolves normally. Either way, the
-- output column keeps the name of the reference.
SELECT id?, name? FROM people;
SELECT typeof(id?), typeof(name?) FROM people LIMIT 1;
SELECT * FROM (SELECT id?, name? FROM people);

-- Whitespace and comments between the identifier and `?` are insignificant.
SELECT id ?, name /* comment */ ? FROM people;

-- Optional references in expressions, predicates, grouping, ordering, and function arguments.
SELECT id, bonus? + 1 AS b FROM people;
SELECT coalesce(archived?, false) AS archived, count(*) AS cnt
FROM people
WHERE archived? IS NULL
GROUP BY archived?;
SELECT id FROM people WHERE coalesce(archived?, false) = false ORDER BY id;
SELECT id FROM people ORDER BY missing?, id DESC;
SELECT id, sum(id) OVER (PARTITION BY region? ORDER BY id) AS running FROM people;
SELECT id FROM people GROUP BY id HAVING max(missing?) IS NULL ORDER BY id;
SELECT id FROM people GROUP BY id HAVING max(addr.state?) IS NULL ORDER BY id;
SELECT id FROM people GROUP BY id ORDER BY max(addr.state?), id;
SELECT id, CASE WHEN missing? IS NULL THEN 'none' ELSE 'some' END AS c FROM people;
SELECT a.id FROM people a JOIN people b ON a.id = b.id AND a.missing? IS NULL ORDER BY a.id;
SELECT a.id, b.missing?, b.addr.state? FROM people a JOIN people b USING (id) ORDER BY a.id;
SELECT id, missing?, count(*) FROM people GROUP BY ALL ORDER BY id;
SELECT count(*) + missing? AS c FROM people GROUP BY ALL;
SELECT missing?, count(*) FROM people;
SELECT count(missing?), max(id?) FROM people;
SELECT DISTINCT missing?, addr?.city FROM people ORDER BY city;
SELECT id, row_number() OVER (ORDER BY id) AS rn FROM people QUALIFY rn = 1 AND missing? IS NULL;

-- GROUP BY, HAVING, and ORDER BY can reference aliases of the SELECT list as usual.
SELECT coalesce(region?, 'none') AS region, count(*) AS cnt FROM people GROUP BY region?;
SELECT coalesce(region?, 'none') AS region, count(*) AS cnt FROM people
GROUP BY 1 HAVING region? = 'none';
SELECT coalesce(rank?, id) AS rank FROM people ORDER BY rank? DESC;

-- Where a name can resolve in more than one way, an optional reference resolves like the same
-- reference without `?`: ORDER BY falls back from the SELECT list to the columns of the FROM
-- clause, and HAVING falls back from those columns to the SELECT list.
SELECT id, named_struct('city', addr.city) AS addr FROM people ORDER BY addr.zip?;
SELECT id, named_struct('city', addr.city) AS addr FROM people ORDER BY addr.state?, id DESC;
SELECT id, named_struct('city', addr.city) AS addr FROM people ORDER BY addr?.state;
SELECT named_struct('state', 'CA') AS addr FROM people GROUP BY addr HAVING addr.state? = 'CA';
SELECT named_struct('state', 'CA') AS addr FROM people GROUP BY addr
HAVING addr.country? IS NULL;
-- `addr.city` exists below the SELECT list but is not grouped, so HAVING falls back to the alias
-- `addr` of the SELECT list, which has no `city`.
SELECT named_struct('state', 'CA') AS addr FROM people GROUP BY id HAVING addr.city? IS NULL;
-- A column that is out of scope is not found.
SELECT DISTINCT id FROM people ORDER BY addr?, id DESC;

-- Common table expressions and set operations.
WITH cte AS (SELECT id, missing? AS m FROM people) SELECT id, m, other? FROM cte ORDER BY id;
SELECT id?, missing? FROM people UNION ALL SELECT missing?, id? FROM people
ORDER BY id NULLS FIRST, missing NULLS FIRST;

-- An optional reference that resolves keeps its type and nullability.
SELECT id? + 1 AS next_id, addr?.city AS city FROM people;

-- Qualified and nested references: `?` guards only the identifier that it follows.
SELECT p.name? FROM people AS p;
SELECT people.name?, people.addr.state? FROM people;
SELECT addr?.city FROM people;
SELECT addr?.state FROM people;
SELECT addr.state? FROM people;
SELECT ghost?.city FROM people;
SELECT ghost.city? FROM people;
SELECT ghost?.city? FROM people;
SELECT t.name? FROM people;
SELECT people?, p? FROM people AS p;

-- Three-part names: `?` binds to the immediately preceding identifier only.
SELECT info.addr.zip? FROM orgs;
SELECT info.addr.geo? FROM orgs;
SELECT info.loc?.city FROM orgs;
SELECT info.loc.city? FROM orgs;
SELECT info?.addr.zip FROM orgs;
SELECT a?.b?.c? FROM orgs;
SELECT missing?.a.b, info.addr.geo?.x FROM orgs;
SELECT struct(id?, missing?) AS s FROM orgs;

-- Errors other than not-found are still raised.
SELECT x? FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b;
SELECT addr.state? FROM people a JOIN people b ON a.id = b.id;
SELECT id.missing? FROM people;
SELECT missing? FROM people GROUP BY id;

-- An optional reference to an existing column is still subject to the usual rules.
SELECT addr.zip? FROM people GROUP BY id;

-- Lateral column aliases resolve as usual.
SELECT id AS a, a? + 1 AS b, c? FROM people;
SELECT named_struct('x', 1) AS s, s.x?, s.y? FROM people;
SELECT missing? AS a, a? AS b FROM people;
SELECT missing? AS a, a + 1 AS b FROM people;
SELECT id AS a, a? + 1 AS b FROM people WHERE missing? IS NULL;
-- A missing optional reference is NULL under its own name, but unlike the name of `addr.city`,
-- that name is not a lateral column alias and not a GROUP BY alias, so a reference to it
-- without `?` is not found. ORDER BY and HAVING can reference it as a column of the SELECT list.
SELECT missing?, missing? IS NULL AS m, addr.city, upper(city) AS c FROM people;
SELECT missing?, missing + 1 AS m FROM people;
SELECT missing + 1 AS m, missing? FROM people;
SELECT addr.state?, upper(state) AS s FROM people;
SELECT missing?, count(*) AS c FROM people GROUP BY missing;
SELECT addr.state?, count(*) AS c FROM people GROUP BY state;
SELECT addr.city, count(*) AS c FROM people GROUP BY city ORDER BY city;
SELECT missing?, count(*) AS c FROM people GROUP BY missing?;
SELECT missing? AS m, count(*) AS c FROM people GROUP BY m;
SELECT missing?, id FROM people ORDER BY missing, id;
SELECT missing?, count(*) AS c FROM people GROUP BY ALL HAVING missing IS NULL;
-- A field that the column does not have is not looked up in a lateral column alias of the same
-- name, like without `?`.
SELECT named_struct('state', 'CA') AS addr, addr.state? FROM people;

-- Session variables resolve as usual.
DECLARE OR REPLACE VARIABLE known INT DEFAULT 7;
SELECT known?, unknown?;
SELECT known? FROM people WHERE missing? IS NULL LIMIT 1;
DECLARE OR REPLACE VARIABLE known_struct STRUCT<a: INT> DEFAULT named_struct('a', 1);
SELECT known_struct.a?, known_struct.b?;
SELECT session.known?, session.unknown?, system.session.unknown?;
SELECT session?, system.session?;
SELECT known_struct.b;
-- A SET VAR statement outside of a SQL script.
SET VAR known = unknown?;
SELECT known;
SET VAR known = (SELECT max(id) + coalesce(max(bonus?), 10) FROM people);
SELECT known;
EXECUTE IMMEDIATE 'SET VAR known = :p?' USING 5 AS p;
SELECT known;
EXECUTE IMMEDIATE 'SET VAR known = coalesce(unknown?, :p?, ?)' USING 7;
EXECUTE IMMEDIATE 'SET VAR known = coalesce(unknown?, ?)' USING 7;
SELECT known;
DROP TEMPORARY VARIABLE known;
DROP TEMPORARY VARIABLE known_struct;

-- SQL scripting variables resolve as usual.
--QUERY-DELIMITER-START
BEGIN
  DECLARE local_known INT DEFAULT 5;
  SELECT local_known?, local_unknown?;
END;
--QUERY-DELIMITER-END
-- The label of a compound statement and the variable of a FOR statement qualify variables.
--QUERY-DELIMITER-START
BEGIN
  lbl: BEGIN
    DECLARE x INT DEFAULT 1;
    FOR r AS SELECT id, addr FROM people WHERE id = 1 DO
      SELECT lbl.x?, lbl.y?, r.id?, r.addr.city?, r.missing?, r.addr.state?;
    END FOR;
  END;
END;
--QUERY-DELIMITER-END
--QUERY-DELIMITER-START
BEGIN
  lbl: BEGIN
    DECLARE x INT DEFAULT 1;
    SELECT other.x?;
  END;
END;
--QUERY-DELIMITER-END

-- Correlated subqueries resolve against the same scopes as without `?`.
SELECT id, (SELECT max(o.missing?) FROM people i WHERE i.id = o.id) AS m FROM people o ORDER BY id;
SELECT id, (SELECT max(i.id) FROM people i WHERE i.id = o.id?) AS m FROM people o ORDER BY id;
SELECT id, (SELECT max(i.id) FROM people i WHERE i.id = o.id AND o.addr.state? IS NULL) AS m
FROM people o ORDER BY id;
SELECT id, (SELECT max(o.addr.state?) FROM people i WHERE i.id = o.id) AS m FROM people o
ORDER BY id;
SELECT id FROM people o WHERE EXISTS (
  SELECT 1 FROM people i WHERE i.id = o.id AND o.ghost? IS NULL) ORDER BY id;
SELECT id FROM people GROUP BY id
HAVING (SELECT max(i.id) FROM people i WHERE i.id = people.id AND people.ghost? IS NULL) > 0
ORDER BY id;
SELECT p.id, l.next_id, l.m FROM people p, LATERAL (SELECT p.id + 1 AS next_id, p.missing? AS m) l
ORDER BY p.id;
SELECT p.id, l.x FROM people p JOIN LATERAL (SELECT p.id AS x) l ON l.missing? IS NULL
ORDER BY p.id;
SELECT p.id, l.x, l.g
FROM people p
JOIN LATERAL (SELECT q.id AS x, q.ghost? AS g FROM people q WHERE q.id = p.id) l
  ON l.x = p.missing? OR l.g IS NULL
ORDER BY p.id;
SELECT id, (SELECT named_struct('id', p.id, 'missing', p.missing?) AS s) AS s FROM people p
ORDER BY id;
SELECT (SELECT (SELECT missing? AS a) AS b) AS c;
SELECT id, (SELECT max(i.id) AS id FROM people i WHERE i.id = o.id GROUP BY i.addr
  HAVING id > 0 AND missing? IS NULL) AS m
FROM people o ORDER BY id;
-- A field that the column of the subquery does not have is not looked up in the outer query.
SELECT id, (SELECT addr.zip? FROM (SELECT named_struct('city', 'x') AS addr)) AS zip FROM people
ORDER BY id;
-- A subquery can reference the alias of a generator of the query around it.
SELECT explode(array(1, 2)) AS e, (SELECT max(id) FROM people WHERE id = e?) AS m;

-- Lambda functions.
SELECT transform(array(named_struct('a', 1)), x -> x.a?) AS a,
  transform(array(named_struct('a', 1)), x -> x.b?) AS b;
SELECT transform(array(1, 2), x -> x + missing?) AS t FROM people LIMIT 1;
SELECT filter(array(1, 2), x -> x? > 1) AS f;
SELECT transform(array(NULL), x -> x?.a) AS t;

-- Optional fields of expressions other than column references.
SELECT array(named_struct('a', 1))[0].a?, array(named_struct('a', 1))[0].b?;
SELECT missing?[0], missing?['key'] FROM people LIMIT 1;
SELECT id, items[0].qty?, items[0].price? FROM orders ORDER BY items[0].qty? DESC;
SELECT id FROM orders ORDER BY items[0].price?, id DESC;
SELECT id FROM orders ORDER BY items?[0].price, id DESC;
-- A path extraction from a missing optional VARIANT column is NULL, like a field of a missing
-- struct column. A written NULL is not a VARIANT.
SELECT CAST(v?:a AS STRING) AS a, CAST(missing?:a AS STRING) AS b, missing?:a::INT AS i
FROM VALUES (parse_json('{"a": 1}')) AS t(v);
SELECT NULL:a;

-- IDENTIFIER clause.
SELECT IDENTIFIER('id')?, IDENTIFIER('name')? FROM people;
SELECT IDENTIFIER('people.name')? FROM people;
SELECT IDENTIFIER('i' || 'd')?, IDENTIFIER('na' || 'me')? FROM people;
SELECT IDENTIFIER('ghost')?.city, addr.IDENTIFIER('state')? FROM people;
SELECT addr.IDENTIFIER('sta' || 'te')? FROM people;
DECLARE OR REPLACE VARIABLE column_name STRING DEFAULT 'name';
SELECT IDENTIFIER(column_name)? FROM people;
DROP TEMPORARY VARIABLE column_name;

-- SQL user-defined functions.
CREATE TEMPORARY FUNCTION optional_fn(a INT) RETURNS INT RETURN a? + coalesce(b?, 10);
SELECT optional_fn(1);
CREATE TEMPORARY FUNCTION optional_qualified_fn(a INT) RETURNS INT
  RETURN optional_qualified_fn.a? + coalesce(optional_qualified_fn.b?, 10);
SELECT optional_qualified_fn(1);
-- The body of a SQL function does not see the columns of the query that calls it, with or
-- without `?`.
SELECT (SELECT optional_fn(1)) AS r;
SELECT (SELECT optional_fn(1)) AS r FROM VALUES (5) AS t(b);
CREATE TEMPORARY FUNCTION optional_table_fn(a INT) RETURNS TABLE (x INT, y INT)
  RETURN SELECT a, coalesce(b?, 10);
SELECT p.id, f.x, f.y FROM people p, LATERAL optional_table_fn(p.id) f ORDER BY p.id;
DECLARE OR REPLACE VARIABLE fn_variable INT DEFAULT 100;
CREATE TEMPORARY FUNCTION variable_fn() RETURNS INT RETURN fn_variable;
SELECT (SELECT variable_fn()) AS r FROM VALUES (5) AS t(fn_variable);
-- The legacy conf has no effect while optional references are enabled.
SET spark.sql.legacy.sqlFunctionBodyResolvesOuterReferences=true;
SELECT (SELECT variable_fn()) AS r FROM VALUES (5) AS t(fn_variable);
RESET spark.sql.legacy.sqlFunctionBodyResolvesOuterReferences;
DROP TEMPORARY FUNCTION optional_fn;
DROP TEMPORARY FUNCTION optional_qualified_fn;
DROP TEMPORARY FUNCTION optional_table_fn;
DROP TEMPORARY FUNCTION variable_fn;
DROP TEMPORARY VARIABLE fn_variable;

-- SQL pipe syntax.
FROM people |> SELECT id, missing?;
FROM people |> WHERE missing? IS NULL |> EXTEND missing? AS m |> AGGREGATE count(m) AS cnt;

-- Spark has no try-cast operator, so `?` followed by `::` is an optional reference that is cast.
SELECT id?::STRING FROM people;
SELECT name?::STRING FROM people;
SELECT name? ::STRING, (name?)::STRING, CAST(name? AS STRING) FROM people LIMIT 1;

-- Optional named parameter markers: `:name?` is NULL if no value is bound to `name`.
EXECUTE IMMEDIATE 'SELECT id?, ? FROM people' USING 10;
EXECUTE IMMEDIATE 'SELECT :x?, :y?' USING 1 AS x;
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE :city? IS NULL OR addr.city = :city? ORDER BY id';
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE :city? IS NULL OR addr.city = :city? ORDER BY id'
  USING 'SF' AS city;
EXECUTE IMMEDIATE 'SELECT :x ?, :x /* comment */ ?' USING 5 AS x;
EXECUTE IMMEDIATE 'SELECT :x?::INT' USING '5' AS x;
EXECUTE IMMEDIATE 'SELECT :x?AS v, :y?IS NULL AS w' USING 5 AS x;
EXECUTE IMMEDIATE 'SELECT :y?, :y';
EXECUTE IMMEDIATE 'SELECT :x?, ?' USING 1;
SELECT :x?;
SELECT :x;

-- Positional parameter markers are unchanged.
EXECUTE IMMEDIATE 'SELECT TRIM(BOTH ? FROM ?)' USING 'x', 'xabcx';
EXECUTE IMMEDIATE 'SELECT INTERVAL ? DAY' USING '1';
EXECUTE IMMEDIATE 'SELECT DISTINCT ? FROM people' USING 5;
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE id IN (SELECT ?) ORDER BY id' USING 2;
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE id IN (VALUES ?, ?) ORDER BY id' USING 1, 2;
EXECUTE IMMEDIATE 'SELECT ? ? FROM people' USING 'a', 'b';
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE id = ? ORDER BY id' USING 2;
EXECUTE IMMEDIATE 'SELECT ?::INT' USING '5';
EXECUTE IMMEDIATE 'SELECT ALL ? FROM people LIMIT ?' USING 5, 1;
EXECUTE IMMEDIATE 'SELECT NOT ?, CASE ? WHEN 1 THEN ? ELSE ? END' USING true, 1, 'a', 'b';
EXECUTE IMMEDIATE
  'SELECT id FROM people WHERE id BETWEEN ? AND ? AND addr.city LIKE ? ORDER BY id'
  USING 1, 2, '%F';
-- A statement that is valid without optional references keeps its meaning: the `?` is a
-- parameter marker, although `distinct?` is also a reference to a column named `distinct`.
EXECUTE IMMEDIATE 'SELECT first(DISTINCT ?) AS f FROM people' USING 5;
-- Parameter markers in a statement with optional references.
EXECUTE IMMEDIATE
  'SELECT missing?, id FROM people WHERE id IN (SELECT ?) AND id BETWEEN ? AND ? LIMIT ?'
  USING 2, 1, 2, 1;
-- In a statement that is not valid without optional references, a `?` after a keyword marks a
-- reference to a column with the name of the keyword where the syntax allows a reference, so
-- `distinct?` is NULL here. A value that no marker takes is an error, so that the following
-- markers do not take the values of the preceding ones silently.
EXECUTE IMMEDIATE 'SELECT missing?, first(DISTINCT ?) AS f FROM people' USING 5;
EXECUTE IMMEDIATE 'SELECT first(DISTINCT ?) AS f, ? AS g FROM people' USING 5, 6;
EXECUTE IMMEDIATE 'SELECT missing?, first(DISTINCT ?) AS f, ? AS g FROM people' USING 5, 6;
EXECUTE IMMEDIATE 'SELECT missing? FROM people' USING 5;
-- A marker before an optional reference.
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE id = ? AND deleted? IS NULL' USING 1;

-- Views are the primary use case: a view stays valid while its source table evolves.
CREATE OR REPLACE TEMPORARY VIEW people_v AS SELECT id, addr, deleted_at? FROM people;
SELECT * FROM people_v ORDER BY id;
DESCRIBE people_v;

CREATE TABLE evolving (id INT) USING parquet;
INSERT INTO evolving VALUES (1);
CREATE VIEW evolving_v WITH SCHEMA EVOLUTION AS SELECT id, deleted_at? FROM evolving;
CREATE VIEW evolving_cast_v AS SELECT id, CAST(deleted_at? AS STRING) AS deleted_at FROM evolving;
CREATE VIEW evolving_void_v AS SELECT id, deleted_at? FROM evolving;
SELECT * FROM evolving_v;
ALTER TABLE evolving ADD COLUMNS (deleted_at STRING);
INSERT INTO evolving VALUES (2, 'yesterday');
SELECT * FROM evolving_v ORDER BY id;
-- A view WITH SCHEMA BINDING or COMPENSATION, the default, keeps the types of its columns, and a
-- missing optional reference has the type VOID, which a column that appears later cannot be cast
-- to, so such a view should cast the reference. WITH SCHEMA [TYPE] EVOLUTION takes the new type.
-- An expression over an optional reference is named after NULL while the reference is missing,
-- so it needs an alias for the view to keep its column names.
SELECT * FROM evolving_cast_v ORDER BY id;
SELECT * FROM evolving_void_v ORDER BY id;
DROP VIEW evolving_v;
DROP VIEW evolving_cast_v;
DROP VIEW evolving_void_v;
DROP TABLE evolving;

-- A view keeps working after a column that it references with `?` is gone, unlike a view that
-- references the column without `?`.
CREATE TABLE events (id INT, deleted_at STRING) USING parquet;
INSERT INTO events VALUES (1, NULL), (2, 'yesterday');
CREATE VIEW events_v AS SELECT id, deleted_at? FROM events;
CREATE VIEW events_strict_v AS SELECT id, deleted_at FROM events;
SELECT * FROM events_v ORDER BY id;
DROP TABLE events;
CREATE TABLE events (id INT) USING parquet;
INSERT INTO events VALUES (1), (2);
SELECT * FROM events_v ORDER BY id;
SELECT * FROM events_strict_v ORDER BY id;
DROP VIEW events_v;
DROP VIEW events_strict_v;
DROP TABLE events;

-- The query of an INSERT, including INSERT OVERWRITE, VALUES lists, and the query of a CREATE
-- TABLE AS SELECT are allowed: like any query, they only read data, and the rows that an
-- overwrite replaces do not depend on the query.
CREATE TABLE target (id INT, addr STRUCT<city: STRING, zip: STRING>) USING parquet;
INSERT INTO target SELECT id?, addr
FROM (SELECT named_struct('city', 'LA', 'zip', '90001') AS addr);
INSERT INTO target VALUES (missing?, NULL);
WITH s AS (SELECT id?, missing? FROM people WHERE id = 1) INSERT INTO target SELECT * FROM s;
SELECT * FROM target ORDER BY id NULLS FIRST, addr NULLS LAST;
SELECT * FROM VALUES (missing?, 1) AS t(a, b);
CREATE TABLE ctas USING parquet AS SELECT id, CAST(missing? AS STRING) AS m FROM people;
SELECT * FROM ctas ORDER BY id;
INSERT OVERWRITE target SELECT id?, addr FROM people WHERE missing? IS NULL;
SELECT * FROM target ORDER BY id;
DROP TABLE ctas;

-- UPDATE, DELETE, MERGE, and the condition of INSERT ... REPLACE reject `?`: a missing column must
-- not change which rows they modify.
UPDATE target SET id = other? WHERE id = 1;
UPDATE target SET id = 1 WHERE missing? IS NULL;
EXECUTE IMMEDIATE 'UPDATE target SET id = :v? WHERE id = 1' USING 1 AS v;
EXECUTE IMMEDIATE 'DELETE FROM target WHERE id = :v?';
DELETE FROM target WHERE missing? IS NULL;
DELETE FROM target WHERE id IN (SELECT id? FROM people);
MERGE INTO target t USING people s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.id = s.missing?;
MERGE INTO target t USING (SELECT id? AS id FROM people) s ON t.id = s.id
  WHEN MATCHED THEN DELETE;
WITH s AS (SELECT id? AS id FROM people) MERGE INTO target t USING s ON t.id = s.id
  WHEN MATCHED THEN DELETE;
WITH s AS (SELECT id? AS id FROM people) DELETE FROM target WHERE id IN (SELECT id FROM s);
INSERT INTO target REPLACE WHERE missing? IS NULL SELECT * FROM target;
-- A partition specification rejects `:name?`, which would stand for the NULL partition.
EXECUTE IMMEDIATE 'TRUNCATE TABLE target PARTITION (id = :p?)';
EXECUTE IMMEDIATE 'INSERT INTO target PARTITION (id = :p?) SELECT NULL' USING 1 AS p;
-- Write targets are column names rather than expressions, so `?` is a syntax error.
UPDATE target SET missing? = 1;
INSERT INTO target (id, missing?) VALUES (1, NULL);
DROP TABLE target;

-- Expressions that define table objects reject `?`.
CREATE TABLE ddl (a INT, b INT GENERATED ALWAYS AS (a? + 1)) USING parquet;
CREATE TABLE ddl (a INT, b INT DEFAULT missing?) USING parquet;
CREATE TABLE ddl (a INT, CONSTRAINT c CHECK (a? > 0)) USING parquet;
CREATE TABLE ddl (a INT) USING parquet OPTIONS (compression = codec?);
CREATE TABLE ddl (a INT) USING parquet;
ALTER TABLE ddl ALTER COLUMN a SET DEFAULT missing?;
ALTER TABLE ddl ADD CONSTRAINT c CHECK (a? > 0);
DROP TABLE ddl;

-- `?` is a syntax error on anything other than a column, field, or named parameter reference.
SELECT people.*? FROM people;
SELECT abs?(1);
SELECT abs(1)? FROM people;
SELECT id FROM people?;
SELECT id?? FROM people;

-- While optional references are disabled, `?` after an identifier is a syntax error, including
-- in a view that was created while they were enabled, and `?` after a named parameter marker is a
-- positional parameter marker.
SET spark.sql.optionalReferences.enabled=false;
SELECT id? FROM people;
SELECT addr.city? FROM people;
SELECT IDENTIFIER('id')? FROM people;
SELECT :x?;
EXECUTE IMMEDIATE 'SELECT :x?' USING 1 AS x;
EXECUTE IMMEDIATE 'SELECT id FROM people WHERE id IN (SELECT ?) ORDER BY id' USING 2;
SELECT * FROM people_v ORDER BY id;

-- A session that does not set the config can still query a view that was created while optional
-- references were enabled, because the view captures the config.
RESET spark.sql.optionalReferences.enabled;
SELECT id? FROM people;
SELECT * FROM people_v ORDER BY id;
