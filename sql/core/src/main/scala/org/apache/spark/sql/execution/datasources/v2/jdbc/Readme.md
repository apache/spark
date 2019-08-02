# Plan/Status of ongoing work on DataSource V2 JDBC connector

## Plan
| Work Item                                     | Who's on it | Status  | Notes  |
|-----------------------------------------------| ----------- | ------  | -----  |
| Batch write ( append)                         | shivsood    | Done    | 1,6    |
| Batch write ( overwrite w truncate)           | shivsood    | Done    |        |
| Batch write ( overwrite w/o truncate)         | shivsood    | Issues  | 2,3    |
| Read  implementation w/o column pruning       | shivsood    | Done    |        |
| Read  w/o column pruning and filters          | shivsood    | Issues  | 4      |
| Columnar read                                 | TBD         |         |        |
| Streaming write                               | TBD         |         |        |
| Streaming read                                | TBD         |         |        |
| Transactional write                           | shivsood    | Issues  | 5      |


Status ->
WIP ( Work in Progress),
DONE ( implementation is done and tested),
Issues (blocking issues)

## Others
- Working branch is https://github.com/shivsood/spark-dsv2
- Interested in contribution? Add work item and your name against it and party on.

## Notes
1. mode(append) - what's the append semantics in V2.
  V1 append would throw an exception if the df col types are not same as table schema. Is that in v2
  achieved by Table::schema? schema returns source schema (as in db) and framework checks if the source schema is
  compatible with the dataframe type. Tested diff is number of cols and framework would raise an exception ,
  but diff in data type did not raise any exception.
2. mode(overwrite) - what's the overwrite semantics in V2?
  V1 overwrite will create a table if that does not exist. In V2 it seems that overwrite does not support
  create semantics. overwrite with a non-existing table failed following tables:schema() which returned null
  schema indicating no existing table. If not create scematics, then what's the diffrence between append
  and overwrite without column filters.
3. mode(overwrite) - why overwrite results in call to WriteBuilder::truncate(). Was expecting call to
   overwrite() instead. Same issues even if option("truncate","false") is explicitly specified during df.write.
   OverwriteByExpressionExec::execute() in WriteToDatasourceV2Exec.scala does overwrite only when
   filters are present. No clear why.
4. Read with column pruning fails with makeFromDriverError
   //df2.select("i").show(10) after df.read.format("jdbc2") c.f test("JDBCV2 read test")
   in MsSqlServerIntegrationSuite
   Error seen as below
   19/07/31 15:20:55 INFO DBPartitionReader: ***dsv2-flows*** close called. number of rows retrieved is 0
   19/07/31 15:20:55 ERROR Executor: Exception in task 0.0 in stage 4.0 (TID 4)
   com.microsoft.sqlserver.jdbc.SQLServerException: The index 2 is out of range.
   	at com.microsoft.sqlserver.jdbc.SQLServerException.makeFromDriverError(SQLServerException.java:228)
   	at com.microsoft.sqlserver.jdbc.SQLServerResultSet.verifyValidColumnIndex(SQLServerResultSet.java:570)
   	at com.microsoft.sqlserver.jdbc.SQLServerResultSet.getterGetColumn(SQLServerResultSet.java:2012)
   	at com.microsoft.sqlserver.jdbc.SQLServerResultSet.getValue(SQLServerResultSet.java:2041)

5. Transactional write - Does not seem feasible with the current FW. The FW suggest that executor send a commit message
   to Driver, and actual commit should only be done by the driver after receiving all commit confirmations. Dont see
   this feasible in JDBC as commit has to happen in JDBCConnection which is maintained by the TASKs and JDBCConnection
   is not serializable that it can be sent to the Driver.
   A slightly better solution may be a 2-way commit. Executors send a 'ReadyToCommit' to Driver. Driver having received
   all 'ReadyToCommit' messages from executors should then send "Commit" to all executors again and then executors
   can commit.

6. write flow - currently auto commits. This needs a fix ( set autocommit in JDBC connection to false and
   then commit it batches)

7. Bulk write - no provision for writing rows in bulk. Rows are provided one at a time.


- Lots of trivial logging. Draft implementation with API understanding as main goal

Update date : 7/31


