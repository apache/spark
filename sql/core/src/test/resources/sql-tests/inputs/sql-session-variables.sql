SET spark.sql.ansi.enabled = true;

DECLARE title STRING;

SET VARIABLE title = '-- Basic sanity --';
DECLARE var1 INT = 5;
SELECT var1;
SET VARIABLE var1 = 6;
SELECT var1;
DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'Create Variable - Success Cases';
DECLARE VARIABLE var1 INT;
SELECT 'Expect: INT, NULL', typeof(var1), var1;

DECLARE OR REPLACE VARIABLE var1 DOUBLE;
SELECT 'Expect: DOUBLE, NULL', typeof(var1), var1;

DROP TEMPORARY VARIABLE var1;
DECLARE OR REPLACE VARIABLE var1 TIMESTAMP;
SELECT 'Expect: TIMESTAMP, NULL', typeof(var1), var1;

SET VARIABLE title = 'Create Variable - Failure Cases';
-- No support for IF NOT EXISTS
DECLARE VARIABLE IF NOT EXISTS var1 INT;
DROP TEMPORARY VARIABLE IF EXISTS var1;

SET VARIABLE title = 'Drop Variable';
DECLARE VAR var1 INT;
SELECT var1;
DROP TEMPORARY VAR var1;

-- Variable is gone
SELECT var1;
DROP TEMPORARY VARIABLE var1;

-- Success
DROP TEMPORARY VARIABLE IF EXISTS var1;

-- Fail: TEMPORARY is mandatory on DROP
DECLARE VARIABLE var1 INT;
DROP VARIABLE var1;
DROP VARIABLE system.session.var1;
DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'Test qualifiers - success';
DECLARE VARIABLE var1 INT DEFAULT 1;
SELECT 1 as Expected, var1 as Unqualified, session.var1 AS SchemaQualified, system.session.var1 AS fullyQualified;
SET VARIABLE var1 = 2;
SELECT 2 as Expected, var1 as Unqualified, session.var1 AS SchemaQualified, system.session.var1 AS fullyQualified;

DECLARE OR REPLACE VARIABLE session.var1 INT DEFAULT 1;
SELECT 1 as Expected, var1 as Unqualified, session.var1 AS SchemaQualified, system.session.var1 AS fullyQualified;
SET VARIABLE session.var1 = 2;
SELECT 2 as Expected, var1 as Unqualified, session.var1 AS SchemaQualified, system.session.var1 AS fullyQualified;

DECLARE OR REPLACE VARIABLE system.session.var1 INT DEFAULT 1;
SELECT 1 as Expected, var1 as Unqualified, session.var1 AS SchemaQualified, system.session.var1 AS fullyQualified;
SET VARIABLE system.session.var1 = 2;
SELECT 2 as Expected, var1 as Unqualified, session.var1 AS SchemaQualified, system.session.var1 AS fullyQualified;

DECLARE OR REPLACE VARIABLE sySteM.sEssIon.vAr1 INT DEFAULT 1;
SELECT 1 as Expected, var1 as Unqualified, sessIon.Var1 AS SchemaQualified, System.sessiOn.var1 AS fullyQualified;
SET VARIABLE sYstem.sesSiOn.vaR1 = 2;
SELECT 2 as Expected, VAR1 as Unqualified, SESSION.VAR1 AS SchemaQualified, SYSTEM.SESSION.VAR1 AS fullyQualified;

DECLARE OR REPLACE VARIABLE var1 INT;
DROP TEMPORARY VARIABLE var1;
DROP TEMPORARY VARIABLE var1;

DECLARE OR REPLACE VARIABLE var1 INT;
DROP TEMPORARY VARIABLE session.var1;
DROP TEMPORARY VARIABLE var1;

DECLARE OR REPLACE VARIABLE var1 INT;
DROP TEMPORARY VARIABLE system.session.var1;
DROP TEMPORARY VARIABLE var1;

DECLARE OR REPLACE VARIABLE var1 INT;
DROP TEMPORARY VARIABLE sysTem.sesSion.vAr1;
DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'Test variable in aggregate';
SELECT (SELECT MAX(id) FROM RANGE(10) WHERE id < title) FROM VALUES 1, 2 AS t(title);

SET VARIABLE title = 'Test qualifiers - fail';
DECLARE OR REPLACE VARIABLE builtin.var1 INT;
DECLARE OR REPLACE VARIABLE system.sesion.var1 INT;
DECLARE OR REPLACE VARIABLE sys.session.var1 INT;

DECLARE OR REPLACE VARIABLE var1 INT;
SELECT var;
SELECT ses.var1;
SELECT b.sesson.var1;
SELECT builtn.session.var1;

SET VARIABLE ses.var1 = 1;
SET VARIABLE builtn.session.var1 = 1;

SET VARIABLE title = 'Test DEFAULT on create - success';
DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1;
SELECT 1 AS Expected, var1 AS result;

DECLARE OR REPLACE VARIABLE var1 DOUBLE DEFAULT 1 + RAND(5);
SELECT true AS Expected, var1 >= 1 AS result;

DECLARE OR REPLACE VARIABLE var1 = 'Hello';
SELECT 'STRING, Hello' AS Expected, typeof(var1) AS type, var1 AS result;

DECLARE OR REPLACE VARIABLE var1 DEFAULT NULL;
SELECT 'VOID, NULL' AS Expected, typeof(var1) AS type, var1 AS result;

DECLARE OR REPLACE VARIABLE INT DEFAULT 5.0;
SELECT 'INT, 5' AS Expected, typeof(var1) AS type, var1 AS result;

DECLARE OR REPLACE VARIABLE var1 MAP<string, double> DEFAULT MAP('Hello', 5.1, 'World', -7.1E10);
SELECT 'MAP<string, double>, [Hello -> 5.1, World -> -7E10]' AS Expected, typeof(var1) AS type, var1 AS result;

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT NULL;
SELECT 'NULL' AS Expected, var1 AS result;

DECLARE OR REPLACE VARIABLE var1 STRING DEFAULT CURRENT_DATABASE();
SELECT 'true' AS Expected, length(var1) > 0 AS result;

DROP TEMPORARY VARIABLE var1;

-- No type and no default is not allowed
DECLARE var1;

-- TBD: Store assignment cast test

SET VARIABLE title = 'Test DEFAULT on create - failures';

-- No subqueries allowed in DEFAULT expression
DECLARE OR REPLACE VARIABLE var1 INT DEFAULT (SELECT c1 FROM VALUES(1) AS T(c1));

-- Incompatible type
DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 'hello';

-- Runtime error
DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1 / 0;

-- Runtime overflow on assignment
DECLARE OR REPLACE VARIABLE var1 SMALLINT DEFAULT 100000;

SET VARIABLE title = 'SET VARIABLE - single target';

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 5;

SET VARIABLE var1 = 7;
SELECT var1;

SET VAR var1 = 8;
SELECT var1;

SET VARIABLE var1 = (SELECT c1 FROM VALUES(1) AS T(c1));
SELECT var1;

SET VARIABLE var1 = (SELECT c1 FROM VALUES(1) AS T(c1) WHERE 1=0);
SELECT var1 AS `null`;

SET VARIABLE var1 = (SELECT c1 FROM VALUES(1.0) AS T(c1));
SELECT var1;

SET VARIABLE var1 = (SELECT c1 FROM VALUES(1.0E10) AS T(c1));
SELECT var1;

SET VARIABLE var1 = (SELECT c1 FROM VALUES(1), (2) AS T(c1));

SET VARIABLE var1 = (SELECT c1, c1 FROM VALUES(1), (2) AS T(c1));

SET VARIABLE var1 = (SELECT c1 FROM VALUES('hello') AS T(c1));

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 5;
SET VARIABLE var1 = var1 + 1;
SELECT var1;

-- TBD store assignment cast test

DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'SET VARIABLE - comma separated target';

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 5;
DECLARE OR REPLACE VARIABLE var2 STRING DEFAULT 'hello';
DECLARE OR REPLACE VARIABLE var3 DOUBLE DEFAULT 2;

SET VARIABLE var1 = 6, var2 = 'world', var3 = pi();
SELECT var1 AS `6`, var2 AS `world` , var3 as `3.14...`;

SET VAR var1 = 7, var2 = 'universe', var3 = -1;
SELECT var1 AS `7`, var2 AS `universe` , var3 as `-1`;

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 5;
DECLARE OR REPLACE VARIABLE var2 STRING DEFAULT 'hello';
DECLARE OR REPLACE VARIABLE var3 DOUBLE DEFAULT 2;

SET VARIABLE var1 = var3, var2 = ascii(var1), var3 = var1;
SELECT var1 AS `2`, var2 AS `104`, var3 AS `5`;

SET VARIABLE var1 = var3, var2 = INTERVAL'5' HOUR, var3 = var1;

-- Duplicates check
SET VARIABLE var1 = 1, var2 = 0, vAr1 = 1;

DROP TEMPORARY VARIABLE var1;
DROP TEMPORARY VARIABLE var2;
DROP TEMPORARY VARIABLE var3;

SET VARIABLE title = 'SET VARIABLE - row assignment';

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 5;
DECLARE OR REPLACE VARIABLE var2 STRING DEFAULT 'hello';
DECLARE OR REPLACE VARIABLE var3 DOUBLE DEFAULT 2;

-- Must have at least one target
SET VARIABLE (var1) = (SELECT c1 FROM VALUES(1) AS T(c1));
SELECT var1;

SET VAR (var1) = (SELECT c1 FROM VALUES(2) AS T(c1));
SELECT var1;

SET VARIABLE (var1, var2) = (SELECT c1, c2 FROM VALUES(10, 11) AS T(c1, c2));
SELECT var1 AS `10`, var2 AS `11`;

SET VARIABLE (var1, var2, var3) = (SELECT c1, c2, c3 FROM VALUES(100, 110, 120) AS T(c1, c2, c3));
SELECT var1 AS `100`, var2 AS `110`, var3 AS `120`;

SET VARIABLE (var1, var2, var3) = (SELECT c1, c2, c3 FROM VALUES(100, 110, 120) AS T(c1, c2, c3) WHERE 1 = 0);
SELECT var1 AS `NULL`, var2 AS `NULL`, var3 AS `NULL`;

-- Fail no target
SET VARIABLE () = (SELECT 1);

-- Fail, more than one row
SET VARIABLE (var1, var2, var3) = (SELECT c1, c2, c3 FROM VALUES(100, 110, 120), (-100, -110, -120) AS T(c1, c2, c3));

-- Fail, not enough columns
SET VARIABLE (var1, var2, var3) = (SELECT c1, c2 FROM VALUES(100, 110, 120) AS T(c1, c2, c3));

-- Fail, too many columns
SET VARIABLE (var1, var2, var3) = (SELECT c1, c2, c3, c1 FROM VALUES(100, 110, 120) AS T(c1, c2, c3));

-- Fail, duplicated target
SET VARIABLE (var1, var2, var1) = (SELECT c1, c2, c3, c1 FROM VALUES(100, 110, 120) AS T(c1, c2, c3));

DROP TEMPORARY VARIABLE var1;
DROP TEMPORARY VARIABLE var2;
DROP TEMPORARY VARIABLE var3;

SET VARIABLE title = 'DEFAULT expression usage';

DECLARE OR REPLACE VARIABLE var1 STRING DEFAULT 'default1';
DECLARE OR REPLACE VARIABLE var2 STRING DEFAULT 'default2';
DECLARE OR REPLACE VARIABLE var3 STRING DEFAULT 'default3';

SET VARIABLE var1 = 'hello';

SET VARIABLE var1 = DEFAULT;
SELECT var1 AS `default`;

SET VARIABLE var1 = 'hello1';
SET VARIABLE var1 = 'hello2';
SET VARIABLE var1 = 'hello3';
SET VARIABLE var1 = DEFAULT, var2 = DEFAULT, var3 = DEFAULT;
SELECT var1 AS `default1`, var2 AS `default2`, var3 AS `default3`;

SET VARIABLE var1 = 'hello';
SET VARIABLE (var1) = (SELECT DEFAULT FROM VALUES(1) AS T(c1));
SELECT var1 AS `default`;

SET VARIABLE var1 = 'hello';
SET VARIABLE (var1) = (SELECT DEFAULT FROM VALUES('world') AS T(default));
SELECT var1 AS `world`;

SET VARIABLE var1 = 'hello';
SET VARIABLE (var1) = (SELECT DEFAULT FROM VALUES(1) AS T(c1) LIMIT 1);
SELECT var1 AS `default`;

SET VARIABLE var1 = 'hello';
SET VARIABLE (var1) = (SELECT DEFAULT FROM VALUES(1),(2),(3) AS T(c1) LIMIT 1 OFFSET 1);
SELECT var1 AS `default`;

SET VARIABLE var1 = 'hello';
SET VARIABLE (var1) = (SELECT DEFAULT FROM VALUES(1),(2),(3) AS T(c1) OFFSET 1);
SELECT var1 AS `default`;

SET VARIABLE var1 = 'hello';
SET VARIABLE (var1) = (WITH v1(c1) AS (VALUES(1) AS T(c1)) SELECT DEFAULT FROM VALUES(1),(2),(3) AS T(c1));
SELECT var1 AS `default`;

-- Failure
SET VARIABLE var1 = 'Hello' || DEFAULT;

SET VARIABLE (var1) = (VALUES(DEFAULT));

SET VARIABLE (var1) = (WITH v1(c1) AS (VALUES(1) AS T(c1)) SELECT DEFAULT + 1 FROM VALUES(1),(2),(3) AS T(c1));

SET VARIABLE var1 = session.default;

DROP TEMPORARY VARIABLE var1;
DROP TEMPORARY VARIABLE var2;
DROP TEMPORARY VARIABLE var3;

SET VARIABLE title = 'SET command';

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1;

-- Sanity: These are all configs
SET x.var1 = 5;
SET x = 5;
SET system.x.var = 5;
SET x.session.var1 = 5;

-- These raise errors: UNSUPPORTED_FEATURE.SET_VARIABLE_IN_SET
SET var1 = 5;
SET session.var1 = 5;
SET system.session.var1 = 5;
SET vAr1 = 5;
SET seSSion.var1 = 5;
SET sYStem.session.var1 = 5;

DROP TEMPORARY VARIABLE var1;

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1;

SELECT var1 AS `2` FROM VALUES(2) AS T(var1);

SELECT c1 AS `2` FROM VALUES(2) AS T(var1), LATERAL(SELECT var1) AS TT(c1);

SELECT session.var1 AS `1` FROM VALUES(2) AS T(var1);

SELECT c1 AS `1` FROM VALUES(2) AS T(var1), LATERAL(SELECT session.var1) AS TT(c1);

DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'variable references -- visibility';
DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1;

VALUES (var1);

SELECT var1;

SELECT sum(var1) FROM VALUES(1);
SELECT var1 + SUM(0) FROM VALUES(1);
SELECT substr('12345', var1, 1);
SELECT 1 FROM VALUES(1, 2) AS T(c1, c2) GROUP BY c1 + var1;
SELECT c1, sum(c2) FROM VALUES(1, 2) AS T(c1, c2) GROUP BY c1 HAVING sum(c1) != var1;
SELECT 1 FROM VALUES(1) AS T(c1) WHERE c1 IN (var1);
SELECT sum(c1) FILTER (c1 != var1) FROM VALUES(1, 2), (2, 3) AS T(c1, c2);
SELECT array(1, 2, 4)[var1];

-- TBD usage in body of lambda function

SELECT 1 FROM VALUES(1) AS T(c1) WHERE c1 = var1;

WITH v1 AS (SELECT var1 AS c1) SELECT c1 AS `1` FROM v1;

CREATE OR REPLACE TEMPORARY VIEW v AS SELECT var1 AS c1;
SELECT * FROM v;
DROP VIEW v;

DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'variable references -- prohibited';

DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1;

-- Known broken for parameters as well
--DROP TABLE IF EXISTS T;
--CREATE TABLE T(c1 INT DEFAULT (var1));
--DROP TABLE IF EXISTS T;

CREATE OR REPLACE VIEW v AS SELECT var1 AS c1;
DROP VIEW IF EXISTS V;

DROP TEMPORARY VARIABLE var1;

SET VARIABLE title = 'variable references -- test constant folding';

DECLARE OR REPLACE VARIABLE var1 STRING DEFAULT 'a INT';
SELECT from_json('{"a": 1}', var1);
DROP TEMPORARY VARIABLE var1;
