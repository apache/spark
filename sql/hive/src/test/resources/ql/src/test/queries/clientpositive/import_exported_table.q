dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_import_exported_table/;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_import_exported_table/exported_table/;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_import_exported_table/exported_table/data/;

dfs -copyFromLocal ../../data/files/exported_table/_metadata hdfs:///tmp/test_import_exported_table/exported_table;
dfs -copyFromLocal ../../data/files/exported_table/data/data hdfs:///tmp/test_import_exported_table/exported_table/data;

IMPORT FROM '/tmp/test_import_exported_table/exported_table';
DESCRIBE j1_41;
SELECT * from j1_41;

dfs -rmr hdfs:///tmp/test_import_exported_table;

