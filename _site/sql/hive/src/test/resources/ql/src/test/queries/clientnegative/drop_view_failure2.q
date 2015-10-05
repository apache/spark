SET hive.exec.drop.ignorenonexistent=false;
-- Can't use DROP VIEW if the view doesn't exist and IF EXISTS isn't specified
DROP VIEW UnknownView;
