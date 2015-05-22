DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=a/p2=a;
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=b/p2=a;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=b/p2=a/datafile;

MSCK TABLE repairtable;

MSCK REPAIR TABLE repairtable;

MSCK TABLE repairtable;

DROP TABLE repairtable;
