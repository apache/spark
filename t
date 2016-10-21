[1mdiff --git a/sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala b/sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala[m
[1mindex d9a2114..7529798 100644[m
[1m--- a/sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala[m
[1m+++ b/sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala[m
[36m@@ -390,7 +390,7 @@[m [mfinal class DataFrameWriter[T] private[sql](ds: Dataset[T]) {[m
         )[m
         val createCmd = CreateTable(tableDesc, mode, Some(df.logicalPlan))[m
         val cmd = if (tableDesc.partitionColumnNames.nonEmpty &&[m
[31m-            df.sparkSession.sqlContext.conf.filesourcePartitionManagement) {[m
[32m+[m[32m            df.sparkSession.sqlContext.conf.manageFilesourcePartitions) {[m
           val recoverPartitionCmd = AlterTableRecoverPartitionsCommand(tableDesc.identifier)[m
           Union(createCmd, recoverPartitionCmd)[m
         } else {[m
[1mdiff --git a/sql/core/src/main/scala/org/apache/spark/sql/execution/command/createDataSourceTables.scala b/sql/core/src/main/scala/org/apache/spark/sql/execution/command/createDataSourceTables.scala[m
[1mindex 288ca37..b04a553 100644[m
[1m--- a/sql/core/src/main/scala/org/apache/spark/sql/execution/command/createDataSourceTables.scala[m
[1m+++ b/sql/core/src/main/scala/org/apache/spark/sql/execution/command/createDataSourceTables.scala[m
[36m@@ -92,7 +92,7 @@[m [mcase class CreateDataSourceTableCommand(table: CatalogTable, ignoreIfExists: Boo[m
     }[m
 [m
     val newProps = if (partitionColumnNames.nonEmpty &&[m
[31m-        sparkSession.sqlContext.conf.filesourcePartitionManagement) {[m
[32m+[m[32m        sparkSession.sqlContext.conf.manageFilesourcePartitions) {[m
       table.properties ++[m
         Map(CatalogTable.PARTITION_PROVIDER_KEY -> CatalogTable.PARTITION_PROVIDER_HIVE)[m
     } else {[m
[36m@@ -244,7 +244,7 @@[m [mcase class CreateDataSourceTableAsSelectCommand([m
 [m
     result match {[m
       case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&[m
[31m-          sparkSession.sqlContext.conf.filesourcePartitionManagement =>[m
[32m+[m[32m          sparkSession.sqlContext.conf.manageFilesourcePartitions =>[m
         sparkSession.sessionState.executePlan([m
           AlterTableRecoverPartitionsCommand(table.identifier)).toRdd[m
       case _ =>[m
[1mdiff --git a/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala[m
[1mindex 176a8a3..18f7496 100644[m
[1m--- a/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala[m
[1m+++ b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala[m
[36m@@ -405,7 +405,7 @@[m [mcase class DataSource([m
             })[m
         }[m
 [m
[31m-        val fileCatalog = if (sparkSession.sqlContext.conf.filesourcePartitionManagement &&[m
[32m+[m[32m        val fileCatalog = if (sparkSession.sqlContext.conf.manageFilesourcePartitions &&[m
             catalogTable.isDefined && catalogTable.get.partitionProviderIsHive) {[m
           new TableFileCatalog([m
             sparkSession,[m
[1mdiff --git a/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala[m
[1mindex 0e97b78..2f8941f 100644[m
[1m--- a/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala[m
[1m+++ b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala[m
[36m@@ -190,7 +190,7 @@[m [mcase class DataSourceAnalysis(conf: CatalystConf) extends Rule[LogicalPlan] {[m
         mode)[m
 [m
       if (l.catalogTable.isDefined && l.catalogTable.get.partitionColumnNames.nonEmpty &&[m
[31m-          t.sparkSession.sqlContext.conf.filesourcePartitionManagement) {[m
[32m+[m[32m          t.sparkSession.sqlContext.conf.manageFilesourcePartitions) {[m
         val recoverPartitionCmd = AlterTableRecoverPartitionsCommand(l.catalogTable.get.identifier)[m
         Union(insertCmd, recoverPartitionCmd)[m
       } else {[m
[1mdiff --git a/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileStatusCache.scala b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileStatusCache.scala[m
[1mindex da48e61..7c2e6fd 100644[m
[1m--- a/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileStatusCache.scala[m
[1m+++ b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileStatusCache.scala[m
[36m@@ -64,7 +64,7 @@[m [mobject FileStatusCache {[m
    */[m
   def newCache(session: SparkSession): FileStatusCache = {[m
     synchronized {[m
[31m-      if (session.sqlContext.conf.filesourcePartitionManagement &&[m
[32m+[m[32m      if (session.sqlContext.conf.manageFilesourcePartitions &&[m
           session.sqlContext.conf.filesourcePartitionFileCacheSize > 0) {[m
         if (sharedCache == null) {[m
           sharedCache = new SharedInMemoryCache([m
[1mdiff --git a/sql/core/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala b/sql/core/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala[m
[1mindex 1d5bbcd..1b913bf 100644[m
[1m--- a/sql/core/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala[m
[1m+++ b/sql/core/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala[m
[36m@@ -271,8 +271,8 @@[m [mobject SQLConf {[m
       .booleanConf[m
       .createWithDefault(true)[m
 [m
[31m-  val HIVE_FILESOURCE_PARTITION_MANAGEMENT =[m
[31m-    SQLConfigBuilder("spark.sql.hive.filesourcePartitionManagement")[m
[32m+[m[32m  val HIVE_MANAGE_FILESOURCE_PARTITIONS =[m
[32m+[m[32m    SQLConfigBuilder("spark.sql.hive.manageFilesourcePartitions")[m
       .doc("When true, enable metastore partition management for file source tables as well. " +[m
            "This includes both datasource and converted Hive tables. This also controls whether " +[m
            "datasource tables will automatically store partition metadata in the Hive metastore.")[m
[36m@@ -679,7 +679,7 @@[m [mprivate[sql] class SQLConf extends Serializable with CatalystConf with Logging {[m
 [m
   def metastorePartitionPruning: Boolean = getConf(HIVE_METASTORE_PARTITION_PRUNING)[m
 [m
[31m-  def filesourcePartitionManagement: Boolean = getConf(HIVE_FILESOURCE_PARTITION_MANAGEMENT)[m
[32m+[m[32m  def manageFilesourcePartitions: Boolean = getConf(HIVE_MANAGE_FILESOURCE_PARTITIONS)[m
 [m
   def filesourcePartitionFileCacheSize: Long = getConf(HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE)[m
 [m
[1mdiff --git a/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveMetastoreCatalog.scala b/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveMetastoreCatalog.scala[m
[1mindex c704df4..817df47 100644[m
[1m--- a/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveMetastoreCatalog.scala[m
[1m+++ b/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveMetastoreCatalog.scala[m
[36m@@ -193,7 +193,7 @@[m [mprivate[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Log[m
       QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)[m
     val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.[m
 [m
[31m-    val lazyPruningEnabled = sparkSession.sqlContext.conf.filesourcePartitionManagement[m
[32m+[m[32m    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions[m
     val result = if (metastoreRelation.hiveQlTable.isPartitioned) {[m
       val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)[m
 [m
[1mdiff --git a/sql/hive/src/test/scala/org/apache/spark/sql/hive/HiveMetadataCacheSuite.scala b/sql/hive/src/test/scala/org/apache/spark/sql/hive/HiveMetadataCacheSuite.scala[m
[1mindex 069f01e..6e887d9 100644[m
[1m--- a/sql/hive/src/test/scala/org/apache/spark/sql/hive/HiveMetadataCacheSuite.scala[m
[1m+++ b/sql/hive/src/test/scala/org/apache/spark/sql/hive/HiveMetadataCacheSuite.scala[m
[36m@@ -63,7 +63,7 @@[m [mclass HiveMetadataCacheSuite extends QueryTest with SQLTestUtils with TestHiveSi[m
 [m
   def testCaching(pruningEnabled: Boolean): Unit = {[m
     test(s"partitioned table is cached when partition pruning is $pruningEnabled") {[m
[31m-      withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> pruningEnabled.toString) {[m
[32m+[m[32m      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> pruningEnabled.toString) {[m
         withTable("test") {[m
           withTempDir { dir =>[m
             spark.range(5).selectExpr("id", "id as f1", "id as f2").write[m
[1mdiff --git a/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionProviderCompatibilitySuite.scala b/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionProviderCompatibilitySuite.scala[m
[1mindex e8907f0..2256341 100644[m
[1m--- a/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionProviderCompatibilitySuite.scala[m
[1m+++ b/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionProviderCompatibilitySuite.scala[m
[36m@@ -56,7 +56,7 @@[m [mclass PartitionProviderCompatibilitySuite[m
   }[m
 [m
   private def verifyIsNewTable(tableName: String): Unit = {[m
[31m-    withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true") {[m
[32m+[m[32m    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {[m
       spark.sql(s"show partitions $tableName").count()  // check does not throw[m
     }[m
   }[m
[36m@@ -64,11 +64,11 @@[m [mclass PartitionProviderCompatibilitySuite[m
   test("convert partition provider to hive with repair table") {[m
     withTable("test") {[m
       withTempDir { dir =>[m
[31m-        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "false") {[m
[32m+[m[32m        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {[m
           setupPartitionedDatasourceTable("test", dir)[m
           assert(spark.sql("select * from test").count() == 5)[m
         }[m
[31m-        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true") {[m
[32m+[m[32m        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {[m
           verifyIsLegacyTable("test")[m
           spark.sql("msck repair table test")[m
           verifyIsNewTable("test")[m
[36m@@ -86,7 +86,7 @@[m [mclass PartitionProviderCompatibilitySuite[m
   test("when partition management is enabled, new tables have partition provider hive") {[m
     withTable("test") {[m
       withTempDir { dir =>[m
[31m-        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true") {[m
[32m+[m[32m        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {[m
           setupPartitionedDatasourceTable("test", dir)[m
           verifyIsNewTable("test")[m
           assert(spark.sql("select * from test").count() == 0)  // needs repair[m
[36m@@ -100,7 +100,7 @@[m [mclass PartitionProviderCompatibilitySuite[m
   test("when partition management is disabled, new tables have no partition provider") {[m
     withTable("test") {[m
       withTempDir { dir =>[m
[31m-        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "false") {[m
[32m+[m[32m        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {[m
           setupPartitionedDatasourceTable("test", dir)[m
           verifyIsLegacyTable("test")[m
           assert(spark.sql("select * from test").count() == 5)[m
[1mdiff --git a/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionedTablePerfStatsSuite.scala b/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionedTablePerfStatsSuite.scala[m
[1mindex 5fbf105..e679bad 100644[m
[1m--- a/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionedTablePerfStatsSuite.scala[m
[1m+++ b/sql/hive/src/test/scala/org/apache/spark/sql/hive/PartitionedTablePerfStatsSuite.scala[m
[36m@@ -110,7 +110,7 @@[m [mclass PartitionedTablePerfStatsSuite[m
 [m
   genericTest("lazy partition pruning reads only necessary partition data") { spec =>[m
     withSQLConf([m
[31m-        SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true",[m
[32m+[m[32m        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",[m
         SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "0") {[m
       withTable("test") {[m
         withTempDir { dir =>[m
[36m@@ -151,7 +151,7 @@[m [mclass PartitionedTablePerfStatsSuite[m
 [m
   genericTest("lazy partition pruning with file status caching enabled") { spec =>[m
     withSQLConf([m
[31m-        SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true",[m
[32m+[m[32m        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",[m
         SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "9999999") {[m
       withTable("test") {[m
         withTempDir { dir =>[m
[36m@@ -192,7 +192,7 @@[m [mclass PartitionedTablePerfStatsSuite[m
 [m
   genericTest("file status caching respects refresh table and refreshByPath") { spec =>[m
     withSQLConf([m
[31m-        SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true",[m
[32m+[m[32m        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",[m
         SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "9999999") {[m
       withTable("test") {[m
         withTempDir { dir =>[m
[36m@@ -221,7 +221,7 @@[m [mclass PartitionedTablePerfStatsSuite[m
 [m
   genericTest("file status cache respects size limit") { spec =>[m
     withSQLConf([m
[31m-        SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true",[m
[32m+[m[32m        SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true",[m
         SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key -> "1" /* 1 byte */) {[m
       withTable("test") {[m
         withTempDir { dir =>[m
[36m@@ -239,7 +239,7 @@[m [mclass PartitionedTablePerfStatsSuite[m
   }[m
 [m
   test("hive table: files read and cached when filesource partition management is off") {[m
[31m-    withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "false") {[m
[32m+[m[32m    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {[m
       withTable("test") {[m
         withTempDir { dir =>[m
           setupPartitionedHiveTable("test", dir)[m
[36m@@ -268,7 +268,7 @@[m [mclass PartitionedTablePerfStatsSuite[m
   }[m
 [m
   test("datasource table: all partition data cached in memory when partition management is off") {[m
[31m-    withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "false") {[m
[32m+[m[32m    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {[m
       withTable("test") {[m
         withTempDir { dir =>[m
           setupPartitionedDatasourceTable("test", dir)[m
