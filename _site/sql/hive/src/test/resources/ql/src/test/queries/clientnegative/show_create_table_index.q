CREATE TABLE tmp_showcrt (key int, value string);
CREATE INDEX tmp_index on table tmp_showcrt(key) as 'compact' WITH DEFERRED REBUILD;
SHOW CREATE TABLE default__tmp_showcrt_tmp_index__;
DROP INDEX tmp_index on tmp_showcrt;
DROP TABLE tmp_showcrt;

