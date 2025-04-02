create table tmp_meta_export_listener_drop_test (foo string);
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/data/exports/HIVE-3427;
set hive.metastore.pre.event.listeners=org.apache.hadoop.hive.ql.parse.MetaDataExportListener;
set hive.metadata.export.location=${system:test.tmp.dir}/data/exports/HIVE-3427;
set hive.move.exported.metadata.to.trash=false;
drop table tmp_meta_export_listener_drop_test;
dfs -rmr ${system:test.tmp.dir}/data/exports/HIVE-3427;
set hive.metastore.pre.event.listeners=;
