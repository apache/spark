--SET spark.sql.parser.optionalIntervalPrefix=false
--IMPORT interval.sql

-- Cannot make INTERVAL keywords optional with the ANSI mode enabled and
-- `spark.sql.parser.optionalIntervalPrefix=false`.
--IMPORT ansi/optional-interval.sql
