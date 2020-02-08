-- Test comments.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (1, 1), (null, 2), (1, null), (null, null)
AS testData(a, b);

-- bracketed comment case one
-- /* This is the first example of bracketed comment.
-- SELECT * FROM testData;
-- */
-- SELECT * FROM testData;

-- bracketed comment case two
-- /* This is the second example of bracketed comment.
-- SELECT '/', * FROM testData;
-- */
-- SELECT '/', * FROM testData;

-- bracketed comment case three
-- /* This is the third example of bracketed comment.
--  *SELECT '*', * FROM testData;
--  */
-- SELECT '*', * FROM testData;

-- nested bracketed comment case one
-- /* This is the first example of nested bracketed comment.
-- /* I am a nested bracketed comment.*/
-- */
-- SELECT * FROM testData;

-- nested bracketed comment case two
-- /* This is the second example of nested bracketed comment.
-- /* I am a nested bracketed comment.
-- */
-- */
-- SELECT * FROM testData;

-- nested bracketed comment case three
-- /*
--  * This is the third example of nested bracketed comment.
--   /*
--    * I am a nested bracketed comment.
--    */
--  */
-- SELECT * FROM testData;

-- nested bracketed comment case four
-- /* 
--  * This is the four example of nested bracketed comment.
-- SELECT /* I am a nested bracketed comment.*/ * FROM testData;
--  */
-- SELECT * FROM testData;

-- nested bracketed comment case five
-- SELECT * /*
--  * This is the five example of nested bracketed comment.
-- /* I am a second level of nested bracketed comment.
-- /* I am a third level of nested bracketed comment.
-- Other information of third level.
-- SELECT * FROM testData;
-- */
-- Other information of second level.
-- */
-- Other information of first level.
-- */
-- FROM testData;
