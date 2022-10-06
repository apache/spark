-- test cases for spark.sql.ansi.double_quoted_identifiers

-- Base line
SET spark.sql.ansi.double_quoted_identifiers = false;

-- All these should error out in the parser
SELECT 1 FROM "not_exist";

USE SCHEMA "not_exist";

ALTER TABLE "not_exist" ADD COLUMN not_exist int;

ALTER TABLE not_exist ADD COLUMN "not_exist" int;

SELECT 1 AS "not_exist" FROM not_exist;

SELECT 1 FROM not_exist AS X("hello");

SELECT "not_exist"();

SELECT "not_exist".not_exist();

-- All these should error out in analysis
SELECT 1 FROM `hello`;

USE SCHEMA `not_exist`;

ALTER TABLE `not_exist` ADD COLUMN not_exist int;

ALTER TABLE not_exist ADD COLUMN `not_exist` int;

SELECT 1 AS `not_exist` FROM `not_exist`;

SELECT 1 FROM not_exist AS X(`hello`);

SELECT `not_exist`();

SELECT `not_exist`.not_exist();

-- Strings in various situations all work
SELECT "hello";

CREATE TEMPORARY VIEW v(c1 COMMENT "hello") AS SELECT 1;
DROP VIEW v;

SELECT INTERVAL "1" YEAR;

-- Now turn on the config.
SET spark.sql.ansi.double_quoted_identifiers = true;

-- All these should error out in analysis now
SELECT 1 FROM "not_exist";

USE SCHEMA "not_exist";

ALTER TABLE "not_exist" ADD COLUMN not_exist int;

ALTER TABLE not_exist ADD COLUMN "not_exist" int;

SELECT 1 AS "not_exist" FROM not_exist;

SELECT 1 FROM not_exist AS X("hello");

SELECT "not_exist"();

SELECT "not_exist".not_exist();

SELECT "hello";

-- Back ticks still work
SELECT 1 FROM `hello`;

USE SCHEMA `not_exist`;

ALTER TABLE `not_exist` ADD COLUMN not_exist int;

ALTER TABLE not_exist ADD COLUMN `not_exist` int;

SELECT 1 AS `not_exist` FROM `not_exist`;

SELECT 1 FROM not_exist AS X(`hello`);

SELECT `not_exist`();

SELECT `not_exist`.not_exist();

-- These fail in the parser now
CREATE TEMPORARY VIEW v(c1 COMMENT "hello") AS SELECT 1;
DROP VIEW v;

SELECT INTERVAL "1" YEAR;

-- Single ticks still work
SELECT 'hello';

CREATE TEMPORARY VIEW v(c1 COMMENT 'hello') AS SELECT 1;
DROP VIEW v;

SELECT INTERVAL '1' YEAR;

-- A whole scenario
CREATE SCHEMA "myschema";
CREATE TEMPORARY VIEW "myview"("c1") AS
  WITH "v"("a") AS (SELECT 1) SELECT "a" FROM "v";
SELECT "a1" AS "a2" FROM "myview" AS "atab"("a1");
DROP TABLE "myview";
DROP SCHEMA "myschema";
