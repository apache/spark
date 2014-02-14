create table src2 like src;
CREATE INDEX src_index_merge_test ON TABLE src2(key) as 'COMPACT' WITH DEFERRED REBUILD;
alter table src2 concatenate;
