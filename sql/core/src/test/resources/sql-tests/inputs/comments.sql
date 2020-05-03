-- Test comments.

-- the first case of bracketed comment
--QUERY-DELIMITER-START
/* This is the first example of bracketed comment.
SELECT 'ommented out content' AS first;
*/
SELECT 'selected content' AS first;
--QUERY-DELIMITER-END

-- the second case of bracketed comment
--QUERY-DELIMITER-START
/* This is the second example of bracketed comment.
SELECT '/', 'ommented out content' AS second;
*/
SELECT '/', 'selected content' AS second;
--QUERY-DELIMITER-END

-- the third case of bracketed comment
--QUERY-DELIMITER-START
/* This is the third example of bracketed comment.
 *SELECT '*', 'ommented out content' AS third;
 */
SELECT '*', 'selected content' AS third;
--QUERY-DELIMITER-END

-- the first case of empty bracketed comment
--QUERY-DELIMITER-START
/**/
SELECT 'selected content' AS fourth;
--QUERY-DELIMITER-END

-- the first case of nested bracketed comment
--QUERY-DELIMITER-START
/* This is the first example of nested bracketed comment.
/* I am a nested bracketed comment.*/
*/
SELECT 'selected content' AS fifth;
--QUERY-DELIMITER-END

-- the second case of nested bracketed comment
--QUERY-DELIMITER-START
/* This is the second example of nested bracketed comment.
/* I am a nested bracketed comment.
 */
 */
SELECT 'selected content' AS sixth;
--QUERY-DELIMITER-END

-- the third case of nested bracketed comment
--QUERY-DELIMITER-START
/*
 * This is the third example of nested bracketed comment.
  /*
   * I am a nested bracketed comment.
   */
 */
SELECT 'selected content' AS seventh;
--QUERY-DELIMITER-END

-- the fourth case of nested bracketed comment
--QUERY-DELIMITER-START
/* 
 * This is the fourth example of nested bracketed comment.
SELECT /* I am a nested bracketed comment.*/ * FROM testData;
 */
SELECT 'selected content' AS eighth;
--QUERY-DELIMITER-END

-- the fifth case of nested bracketed comment
--QUERY-DELIMITER-START
SELECT /*
 * This is the fifth example of nested bracketed comment.
/* I am a second level of nested bracketed comment.
/* I am a third level of nested bracketed comment.
Other information of third level.
SELECT 'ommented out content' AS ninth;
*/
Other information of second level.
*/
Other information of first level.
*/
'selected content' AS ninth;
--QUERY-DELIMITER-END

-- the first case of empty nested bracketed comment
--QUERY-DELIMITER-START
/*/**/*/
SELECT 'selected content' AS tenth;
--QUERY-DELIMITER-END
