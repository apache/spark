set hive.exec.drop.ignorenonexistent=false;
-- Can't use DROP TABLE if the table doesn't exist and IF EXISTS isn't specified
DROP TABLE UnknownTable;
