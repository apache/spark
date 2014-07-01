set hive.exec.drop.ignorenonexistent=false;
-- Can't use DROP INDEX if the index doesn't exist and IF EXISTS isn't specified
DROP INDEX UnknownIndex ON src;
