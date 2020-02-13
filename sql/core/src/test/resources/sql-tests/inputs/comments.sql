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
SELECT 'selected content' AS fourth;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case two
/* This is the second example of nested bracketed comment.
/* I am a nested bracketed comment.
 */
 */
SELECT 'selected content' AS fifth;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case three
/*
 * This is the third example of nested bracketed comment.
  /*
   * I am a nested bracketed comment.
   */
 */
SELECT 'selected content' AS sixth;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case fourth
/* 
 * This is the fourth example of nested bracketed comment.
SELECT /* I am a nested bracketed comment.*/ * FROM testData;
 */
SELECT 'selected content' AS seventh;

-- [SPARK-30758] Spark SQL can't display bracketed comments well in generated golden files
-- nested bracketed comment case fifth
SELECT /*
 * This is the fifth example of nested bracketed comment.
/* I am a second level of nested bracketed comment.
/* I am a third level of nested bracketed comment.
Other information of third level.
SELECT 'ommented out content' AS eighth;
*/
Other information of second level.
*/
Other information of first level.
*/
'selected content' AS eight;
