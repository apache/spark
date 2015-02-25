insert overwrite local directory '${system:test.tmp.dir}/tez_local_src_table_1'
select * from src order by key limit 10 ;
dfs -cat file:${system:test.tmp.dir}/tez_local_src_table_1/000000_0 ;

dfs -rmr file:${system:test.tmp.dir}/tez_local_src_table_1/ ;
