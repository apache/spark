-- Check alias count for SELECT UDTF() syntax:
-- explode returns a table with only 1 col - should be an error if query specifies >1 col aliases
SELECT explode(array(1,2,3)) AS (myCol1, myCol2) LIMIT 3;