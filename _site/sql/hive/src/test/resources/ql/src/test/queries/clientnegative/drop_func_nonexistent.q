set hive.exec.drop.ignorenonexistent=false;
-- Can't use DROP FUNCTION if the function doesn't exist and IF EXISTS isn't specified
drop function nonexistent_function;
