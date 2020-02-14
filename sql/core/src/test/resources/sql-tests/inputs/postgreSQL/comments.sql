--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- COMMENTS
-- https://github.com/postgres/postgres/blob/REL_12_BETA3/src/test/regress/sql/comments.sql
--

SELECT 'trailing' AS first; -- trailing single line
SELECT /* embedded single line */ 'embedded' AS `second`;
SELECT /* both embedded and trailing single line */ 'both' AS third; -- trailing single line

SELECT 'before multi-line' AS fourth;
--QUERY-DELIMITER-START
/* This is an example of SQL which should not execute:
 * select 'multi-line';
 */
SELECT 'after multi-line' AS fifth;
--QUERY-DELIMITER-END

--
-- Nested comments
--
--QUERY-DELIMITER-START
/*
SELECT 'trailing' as x1; -- inside block comment
*/

/* This block comment surrounds a query which itself has a block comment...
SELECT /* embedded single line */ 'embedded' AS x2;
*/

SELECT -- continued after the following block comments...
/* Deeply nested comment.
   This includes a single apostrophe to make sure we aren't decoding this part as a string.
SELECT 'deep nest' AS n1;
/* Second level of nesting...
SELECT 'deeper nest' as n2;
/* Third level of nesting...
SELECT 'deepest nest' as n3;
*/
Hoo boy. Still two deep...
*/
Now just one deep...
*/
'deeply nested example' AS sixth;
--QUERY-DELIMITER-END
-- [SPARK-30824] Support submit sql content only contains comments.
-- /* and this is the end of the file */
