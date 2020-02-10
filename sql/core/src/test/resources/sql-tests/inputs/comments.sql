-- Test comments.

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- bracketed comment case one
/* This is the first example of bracketed comment.
SELECT 'ommented out content' AS first;
*/
SELECT 'selected content' AS first;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- bracketed comment case two
/* This is the second example of bracketed comment.
SELECT '/', 'ommented out content' AS second;
*/
SELECT '/', 'selected content' AS second;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- bracketed comment case three
/* This is the third example of bracketed comment.
 *SELECT '*', 'ommented out content' AS third;
 */
SELECT '*', 'selected content' AS third;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case one
/* This is the first example of nested bracketed comment.
/* I am a nested bracketed comment.*/
*/
SELECT 'selected content' AS four;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case two
/* This is the second example of nested bracketed comment.
/* I am a nested bracketed comment.
 */
 */
SELECT 'selected content' AS five;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case three
/*
 * This is the third example of nested bracketed comment.
  /*
   * I am a nested bracketed comment.
   */
 */
SELECT 'selected content' AS six;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case four
/* 
 * This is the four example of nested bracketed comment.
SELECT /* I am a nested bracketed comment.*/ * FROM testData;
 */
SELECT 'selected content' AS seven;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case five
SELECT /*
 * This is the five example of nested bracketed comment.
/* I am a second level of nested bracketed comment.
/* I am a third level of nested bracketed comment.
Other information of third level.
SELECT 'ommented out content' AS eight;
*/
Other information of second level.
*/
Other information of first level.
*/
'selected content' AS eight;
