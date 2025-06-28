-- All these should error out in the parser in non-ansi mode, error out in the analyzer in ansi mode
SELECT 1 FROM "not_exist";

USE SCHEMA "not_exist";

ALTER TABLE "not_exist" ADD COLUMN not_exist int;

ALTER TABLE not_exist ADD COLUMN "not_exist" int;

SELECT 1 AS "not_exist" FROM not_exist;

SELECT 1 FROM not_exist AS X("hello");

SELECT "not_exist"();

SELECT "not_exist".not_exist();

select 1 from "not_""exists";

USE SCHEMA "not_""exist";

ALTER TABLE "not_""exist" ADD COLUMN not_exist int;

ALTER TABLE not_exist ADD COLUMN "not_""exist" int;

SELECT 1 AS "not_""exist" FROM not_exist;

SELECT 1 FROM not_exist AS X("he""llo");

SELECT "not_""exist"();

SELECT "not_""exist".not_exist();

CREATE SCHEMA "my""schema";
DROP SCHEMA "my""schema";

-- All these should be ok in ansi mode, error out in the parser in non-ansi mode
CREATE TEMPORARY VIEW "v""iew"(c1) AS SELECT 1;
DROP VIEW "v""iew";

CREATE TEMPORARY VIEW v("c""1") AS SELECT 1;
DROP VIEW v;

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

SELECT "he""llo";

-- Ok for non-ansi mode, error for ansi-mode
CREATE TEMPORARY VIEW v(c1 COMMENT "hello") AS SELECT 1;
DROP VIEW v;

SELECT INTERVAL "1" YEAR;

-- Single ticks still work
SELECT 'hello';

SELECT 'he''llo';

CREATE TEMPORARY VIEW v(c1 COMMENT 'hello') AS SELECT 1;
DROP VIEW v;

CREATE TEMPORARY VIEW v(c1 COMMENT 'he''llo') AS SELECT 1;
DROP VIEW v;

SELECT INTERVAL '1' YEAR;

-- A whole scenario
CREATE SCHEMA "myschema";
CREATE TEMPORARY VIEW "myview"("c1") AS
  WITH "v"("a") AS (SELECT 1) SELECT "a" FROM "v";
SELECT "a1" AS "a2" FROM "myview" AS "atab"("a1");
DROP TABLE "myview";
DROP SCHEMA "myschema";
