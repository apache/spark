-- Check alias count for LATERAL VIEW syntax:
-- explode returns a table with only 1 col - should be an error if query specifies >1 col aliases
SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol1, myCol2 LIMIT 3;