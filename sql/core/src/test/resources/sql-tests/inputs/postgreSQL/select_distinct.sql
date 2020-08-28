--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- SELECT_DISTINCT
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/select_distinct.sql
--

CREATE OR REPLACE TEMPORARY VIEW tmp AS
SELECT two, stringu1, ten, string4
FROM onek;

--
-- awk '{print $3;}' onek.data | sort -n | uniq
--
SELECT DISTINCT two FROM tmp ORDER BY 1;

--
-- awk '{print $5;}' onek.data | sort -n | uniq
--
SELECT DISTINCT ten FROM tmp ORDER BY 1;

--
-- awk '{print $16;}' onek.data | sort -d | uniq
--
SELECT DISTINCT string4 FROM tmp ORDER BY 1;

-- [SPARK-28010] Support ORDER BY ... USING syntax
--
-- awk '{print $3,$16,$5;}' onek.data | sort -d | uniq |
-- sort +0n -1 +1d -2 +2n -3
--
-- SELECT DISTINCT two, string4, ten
--    FROM tmp
--    ORDER BY two using <, string4 using <, ten using <;
SELECT DISTINCT two, string4, ten
   FROM tmp
   ORDER BY two ASC, string4 ASC, ten ASC;

-- Skip the person table because there is a point data type that we don't support.
--
-- awk '{print $2;}' person.data |
-- awk '{if(NF!=1){print $2;}else{print;}}' - emp.data |
-- awk '{if(NF!=1){print $2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $5;}else{print;}}' - stud_emp.data |
-- sort -n -r | uniq
--
-- SELECT DISTINCT p.age FROM person* p ORDER BY age using >;

--
-- Check mentioning same column more than once
--

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT count(*) FROM
--   (SELECT DISTINCT two, four, two FROM tenk1) ss;

SELECT count(*) FROM
  (SELECT DISTINCT two, four, two FROM tenk1) ss;

--
-- Also, some tests of IS DISTINCT FROM, which doesn't quite deserve its
-- very own regression file.
--

CREATE OR REPLACE TEMPORARY VIEW disttable AS SELECT * FROM
  (VALUES (1), (2), (3), (NULL))
  AS v(f1);

-- basic cases
SELECT f1, f1 IS DISTINCT FROM 2 as `not 2` FROM disttable;
SELECT f1, f1 IS DISTINCT FROM NULL as `not null` FROM disttable;
SELECT f1, f1 IS DISTINCT FROM f1 as `false` FROM disttable;
SELECT f1, f1 IS DISTINCT FROM f1+1 as `not null` FROM disttable;

-- check that optimizer constant-folds it properly
SELECT 1 IS DISTINCT FROM 2 as `yes`;
SELECT 2 IS DISTINCT FROM 2 as `no`;
SELECT 2 IS DISTINCT FROM null as `yes`;
SELECT null IS DISTINCT FROM null as `no`;

-- negated form
SELECT 1 IS NOT DISTINCT FROM 2 as `no`;
SELECT 2 IS NOT DISTINCT FROM 2 as `yes`;
SELECT 2 IS NOT DISTINCT FROM null as `no`;
SELECT null IS NOT DISTINCT FROM null as `yes`;
