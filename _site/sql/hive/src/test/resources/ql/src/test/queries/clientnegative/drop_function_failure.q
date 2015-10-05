set hive.exec.drop.ignorenonexistent=false;
-- Can't use DROP TEMPORARY FUNCTION if the function doesn't exist and IF EXISTS isn't specified
DROP TEMPORARY FUNCTION UnknownFunction;
