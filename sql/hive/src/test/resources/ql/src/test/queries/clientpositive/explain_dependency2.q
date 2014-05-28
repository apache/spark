-- This test is used for testing EXPLAIN DEPENDENCY command

-- select from a table which does not involve a map-reduce job
EXPLAIN DEPENDENCY SELECT * FROM src;

-- select from a table which involves a map-reduce job
EXPLAIN DEPENDENCY SELECT count(*) FROM src;

-- select from a partitioned table which does not involve a map-reduce job
-- and some partitions are being selected
EXPLAIN DEPENDENCY SELECT * FROM srcpart where ds is not null;

-- select from a partitioned table which does not involve a map-reduce job
-- and none of the partitions are being selected
EXPLAIN DEPENDENCY SELECT * FROM srcpart where ds = '1';

-- select from a partitioned table which involves a map-reduce job
-- and some partitions are being selected
EXPLAIN DEPENDENCY SELECT count(*) FROM srcpart where ds is not null;

-- select from a partitioned table which involves a map-reduce job
-- and none of the partitions are being selected
EXPLAIN DEPENDENCY SELECT count(*) FROM srcpart where ds = '1';

create table tstsrcpart like srcpart;

-- select from a partitioned table with no partitions which does not involve a map-reduce job
EXPLAIN DEPENDENCY SELECT * FROM tstsrcpart where ds is not null;

-- select from a partitioned table with no partitions which involves a map-reduce job
EXPLAIN DEPENDENCY SELECT count(*) FROM tstsrcpart where ds is not null;
