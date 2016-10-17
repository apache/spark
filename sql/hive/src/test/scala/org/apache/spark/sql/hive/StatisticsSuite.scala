/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.io.{File, PrintWriter}

import scala.reflect.ClassTag

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.execution.command.{AnalyzeTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class StatisticsSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  test("parse analyze commands") {
    def assertAnalyzeCommand(analyzeCommand: String, c: Class[_]) {
      val parsed = spark.sessionState.sqlParser.parsePlan(analyzeCommand)
      val operators = parsed.collect {
        case a: AnalyzeTableCommand => a
        case o => o
      }

      assert(operators.size === 1)
      if (operators(0).getClass() != c) {
        fail(
          s"""$analyzeCommand expected command: $c, but got ${operators(0)}
             |parsed command:
             |$parsed
           """.stripMargin)
      }
    }

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS noscan",
      classOf[AnalyzeTableCommand])

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS nOscAn",
      classOf[AnalyzeTableCommand])
  }

  test("MetastoreRelations fallback to HDFS for size estimation") {
    val enableFallBackToHdfsForStats = spark.sessionState.conf.fallBackToHdfsForStatsEnabled
    try {
      withTempDir { tempDir =>

        // EXTERNAL OpenCSVSerde table pointing to LOCATION

        val file1 = new File(tempDir + "/data1")
        val writer1 = new PrintWriter(file1)
        writer1.write("1,2")
        writer1.close()

        val file2 = new File(tempDir + "/data2")
        val writer2 = new PrintWriter(file2)
        writer2.write("1,2")
        writer2.close()

        sql(
          s"""CREATE EXTERNAL TABLE csv_table(page_id INT, impressions INT)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
              \"separatorChar\" = \",\",
              \"quoteChar\"     = \"\\\"\",
              \"escapeChar\"    = \"\\\\\")
            LOCATION '$tempDir'
          """)

        spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, true)

        val relation = spark.sessionState.catalog.lookupRelation(TableIdentifier("csv_table"))
          .asInstanceOf[MetastoreRelation]

        val properties = relation.hiveQlTable.getParameters
        assert(properties.get("totalSize").toLong <= 0, "external table totalSize must be <= 0")
        assert(properties.get("rawDataSize").toLong <= 0, "external table rawDataSize must be <= 0")

        val sizeInBytes = relation.statistics.sizeInBytes
        assert(sizeInBytes === BigInt(file1.length() + file2.length()))
      }
    } finally {
      spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, enableFallBackToHdfsForStats)
      sql("DROP TABLE csv_table ")
    }
  }

  test("analyze MetastoreRelations") {
    def queryTotalSize(tableName: String): BigInt =
      spark.sessionState.catalog.lookupRelation(TableIdentifier(tableName)).statistics.sizeInBytes

    // Non-partitioned table
    sql("CREATE TABLE analyzeTable (key STRING, value STRING)").collect()
    sql("INSERT INTO TABLE analyzeTable SELECT * FROM src").collect()
    sql("INSERT INTO TABLE analyzeTable SELECT * FROM src").collect()

    sql("ANALYZE TABLE analyzeTable COMPUTE STATISTICS noscan")

    assert(queryTotalSize("analyzeTable") === BigInt(11624))

    sql("DROP TABLE analyzeTable").collect()

    // Partitioned table
    sql(
      """
        |CREATE TABLE analyzeTable_part (key STRING, value STRING) PARTITIONED BY (ds STRING)
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-01')
        |SELECT * FROM src
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-02')
        |SELECT * FROM src
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-03')
        |SELECT * FROM src
      """.stripMargin).collect()

    assert(queryTotalSize("analyzeTable_part") === spark.sessionState.conf.defaultSizeInBytes)

    sql("ANALYZE TABLE analyzeTable_part COMPUTE STATISTICS noscan")

    assert(queryTotalSize("analyzeTable_part") === BigInt(17436))

    sql("DROP TABLE analyzeTable_part").collect()

    // Try to analyze a temp table
    sql("""SELECT * FROM src""").createOrReplaceTempView("tempTable")
    intercept[AnalysisException] {
      sql("ANALYZE TABLE tempTable COMPUTE STATISTICS")
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tempTable"), ignoreIfNotExists = true, purge = false)
  }

  test("analyzing views is not supported") {
    def assertAnalyzeUnsupported(analyzeCommand: String): Unit = {
      val err = intercept[AnalysisException] {
        sql(analyzeCommand)
      }
      assert(err.message.contains("ANALYZE TABLE is not supported"))
    }

    val tableName = "tbl"
    withTable(tableName) {
      spark.range(10).write.saveAsTable(tableName)
      val viewName = "view"
      withView(viewName) {
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $tableName")
        assertAnalyzeUnsupported(s"ANALYZE TABLE $viewName COMPUTE STATISTICS")
        assertAnalyzeUnsupported(s"ANALYZE TABLE $viewName COMPUTE STATISTICS FOR COLUMNS id")
      }
    }
  }

  private def checkTableStats(
      stats: Option[Statistics],
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Unit = {
    if (hasSizeInBytes || expectedRowCounts.nonEmpty) {
      assert(stats.isDefined)
      assert(stats.get.sizeInBytes > 0)
      assert(stats.get.rowCount === expectedRowCounts)
    } else {
      assert(stats.isEmpty)
    }
  }

  private def checkTableStats(
      tableName: String,
      isDataSourceTable: Boolean,
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Option[Statistics] = {
    val df = sql(s"SELECT * FROM $tableName")
    val stats = df.queryExecution.analyzed.collect {
      case rel: MetastoreRelation =>
        checkTableStats(rel.catalogTable.stats, hasSizeInBytes, expectedRowCounts)
        assert(!isDataSourceTable, "Expected a Hive serde table, but got a data source table")
        rel.catalogTable.stats
      case rel: LogicalRelation =>
        checkTableStats(rel.catalogTable.get.stats, hasSizeInBytes, expectedRowCounts)
        assert(isDataSourceTable, "Expected a data source table, but got a Hive serde table")
        rel.catalogTable.get.stats
    }
    assert(stats.size == 1)
    stats.head
  }

  test("test table-level statistics for hive tables created in HiveExternalCatalog") {
    val textTable = "textTable"
    withTable(textTable) {
      // Currently Spark's statistics are self-contained, we don't have statistics until we use
      // the `ANALYZE TABLE` command.
      sql(s"CREATE TABLE $textTable (key STRING, value STRING) STORED AS TEXTFILE")
      checkTableStats(
        textTable,
        isDataSourceTable = false,
        hasSizeInBytes = false,
        expectedRowCounts = None)
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      checkTableStats(
        textTable,
        isDataSourceTable = false,
        hasSizeInBytes = false,
        expectedRowCounts = None)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      val fetchedStats1 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats2 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
    }
  }

  test("test elimination of the influences of the old stats") {
    val textTable = "textTable"
    withTable(textTable) {
      sql(s"CREATE TABLE $textTable (key STRING, value STRING) STORED AS TEXTFILE")
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats1 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = Some(500))

      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      // when the total size is not changed, the old row count is kept
      val fetchedStats2 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1 == fetchedStats2)

      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      // update total size and remove the old and invalid row count
      val fetchedStats3 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = None)
      assert(fetchedStats3.get.sizeInBytes > fetchedStats2.get.sizeInBytes)
    }
  }

  test("test statistics of LogicalRelation converted from MetastoreRelation") {
    val parquetTable = "parquetTable"
    val orcTable = "orcTable"
    withTable(parquetTable, orcTable) {
      sql(s"CREATE TABLE $parquetTable (key STRING, value STRING) STORED AS PARQUET")
      sql(s"CREATE TABLE $orcTable (key STRING, value STRING) STORED AS ORC")
      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      sql(s"INSERT INTO TABLE $orcTable SELECT * FROM src")

      // the default value for `spark.sql.hive.convertMetastoreParquet` is true, here we just set it
      // for robustness
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkTableStats(
          parquetTable, isDataSourceTable = true, hasSizeInBytes = false, expectedRowCounts = None)
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
        checkTableStats(
          parquetTable,
          isDataSourceTable = true,
          hasSizeInBytes = true,
          expectedRowCounts = Some(500))
      }
      withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "true") {
        checkTableStats(
          orcTable, isDataSourceTable = true, hasSizeInBytes = false, expectedRowCounts = None)
        sql(s"ANALYZE TABLE $orcTable COMPUTE STATISTICS")
        checkTableStats(
          orcTable, isDataSourceTable = true, hasSizeInBytes = true, expectedRowCounts = Some(500))
      }
    }
  }

  test("test table-level statistics for data source table created in HiveExternalCatalog") {
    val parquetTable = "parquetTable"
    withTable(parquetTable) {
      sql(s"CREATE TABLE $parquetTable (key STRING, value STRING) USING PARQUET")
      val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(parquetTable))
      assert(DDLUtils.isDatasourceTable(catalogTable))

      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      checkTableStats(
        parquetTable, isDataSourceTable = true, hasSizeInBytes = false, expectedRowCounts = None)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
      val fetchedStats1 = checkTableStats(
        parquetTable, isDataSourceTable = true, hasSizeInBytes = true, expectedRowCounts = None)

      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
      val fetchedStats2 = checkTableStats(
        parquetTable, isDataSourceTable = true, hasSizeInBytes = true, expectedRowCounts = None)
      assert(fetchedStats2.get.sizeInBytes > fetchedStats1.get.sizeInBytes)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
      val fetchedStats3 = checkTableStats(
        parquetTable,
        isDataSourceTable = true,
        hasSizeInBytes = true,
        expectedRowCounts = Some(1000))
      assert(fetchedStats3.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
    }
  }

  test("statistics collection of a table with zero column") {
    val table_no_cols = "table_no_cols"
    withTable(table_no_cols) {
      val rddNoCols = sparkContext.parallelize(1 to 10).map(_ => Row.empty)
      val dfNoCols = spark.createDataFrame(rddNoCols, StructType(Seq.empty))
      dfNoCols.write.format("json").saveAsTable(table_no_cols)
      sql(s"ANALYZE TABLE $table_no_cols COMPUTE STATISTICS")
      checkTableStats(
        table_no_cols,
        isDataSourceTable = true,
        hasSizeInBytes = true,
        expectedRowCounts = Some(10))
    }
  }

  private def getStatsBeforeAfterUpdate(isAnalyzeColumns: Boolean): (Statistics, Statistics) = {
    val tableName = "tbl"
    var statsBeforeUpdate: Statistics = null
    var statsAfterUpdate: Statistics = null
    withTable(tableName) {
      val tableIndent = TableIdentifier(tableName, Some("default"))
      val catalog = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
      sql(s"CREATE TABLE $tableName (key int) USING PARQUET")
      sql(s"INSERT INTO $tableName SELECT 1")
      if (isAnalyzeColumns) {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key")
      } else {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      // Table lookup will make the table cached.
      catalog.lookupRelation(tableIndent)
      statsBeforeUpdate = catalog.getCachedDataSourceTable(tableIndent)
        .asInstanceOf[LogicalRelation].catalogTable.get.stats.get

      sql(s"INSERT INTO $tableName SELECT 2")
      if (isAnalyzeColumns) {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key")
      } else {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      catalog.lookupRelation(tableIndent)
      statsAfterUpdate = catalog.getCachedDataSourceTable(tableIndent)
        .asInstanceOf[LogicalRelation].catalogTable.get.stats.get
    }
    (statsBeforeUpdate, statsAfterUpdate)
  }

  test("test refreshing table stats of cached data source table by `ANALYZE TABLE` statement") {
    val (statsBeforeUpdate, statsAfterUpdate) = getStatsBeforeAfterUpdate(isAnalyzeColumns = false)

    assert(statsBeforeUpdate.sizeInBytes > 0)
    assert(statsBeforeUpdate.rowCount == Some(1))

    assert(statsAfterUpdate.sizeInBytes > statsBeforeUpdate.sizeInBytes)
    assert(statsAfterUpdate.rowCount == Some(2))
  }

  test("test refreshing column stats of cached data source table by `ANALYZE TABLE` statement") {
    val (statsBeforeUpdate, statsAfterUpdate) = getStatsBeforeAfterUpdate(isAnalyzeColumns = true)

    assert(statsBeforeUpdate.sizeInBytes > 0)
    assert(statsBeforeUpdate.rowCount == Some(1))
    StatisticsTest.checkColStat(
      dataType = IntegerType,
      colStat = statsBeforeUpdate.colStats("key"),
      expectedColStat = ColumnStat(InternalRow(0L, 1, 1, 1L)),
      rsd = spark.sessionState.conf.ndvMaxError)

    assert(statsAfterUpdate.sizeInBytes > statsBeforeUpdate.sizeInBytes)
    assert(statsAfterUpdate.rowCount == Some(2))
    StatisticsTest.checkColStat(
      dataType = IntegerType,
      colStat = statsAfterUpdate.colStats("key"),
      expectedColStat = ColumnStat(InternalRow(0L, 2, 1, 2L)),
      rsd = spark.sessionState.conf.ndvMaxError)
  }

  private lazy val (testDataFrame, expectedColStatsSeq) = {
    import testImplicits._

    val intSeq = Seq(1, 2)
    val stringSeq = Seq("a", "bb")
    val binarySeq = Seq("a", "bb").map(_.getBytes)
    val booleanSeq = Seq(true, false)
    val data = intSeq.indices.map { i =>
      (intSeq(i), stringSeq(i), binarySeq(i), booleanSeq(i))
    }
    val df: DataFrame = data.toDF("c1", "c2", "c3", "c4")
    val expectedColStatsSeq: Seq[(StructField, ColumnStat)] = df.schema.map { f =>
      val colStat = f.dataType match {
        case IntegerType =>
          ColumnStat(InternalRow(0L, intSeq.max, intSeq.min, intSeq.distinct.length.toLong))
        case StringType =>
          ColumnStat(InternalRow(0L, stringSeq.map(_.length).sum / stringSeq.length.toDouble,
            stringSeq.map(_.length).max.toInt, stringSeq.distinct.length.toLong))
        case BinaryType =>
          ColumnStat(InternalRow(0L, binarySeq.map(_.length).sum / binarySeq.length.toDouble,
            binarySeq.map(_.length).max.toInt))
        case BooleanType =>
          ColumnStat(InternalRow(0L, booleanSeq.count(_.equals(true)).toLong,
            booleanSeq.count(_.equals(false)).toLong))
      }
      (f, colStat)
    }
    (df, expectedColStatsSeq)
  }

  private def checkColStats(
      tableName: String,
      isDataSourceTable: Boolean,
      expectedColStatsSeq: Seq[(StructField, ColumnStat)]): Unit = {
    val readback = spark.table(tableName)
    val stats = readback.queryExecution.analyzed.collect {
      case rel: MetastoreRelation =>
        assert(!isDataSourceTable, "Expected a Hive serde table, but got a data source table")
        rel.catalogTable.stats.get
      case rel: LogicalRelation =>
        assert(isDataSourceTable, "Expected a data source table, but got a Hive serde table")
        rel.catalogTable.get.stats.get
    }
    assert(stats.length == 1)
    val columnStats = stats.head.colStats
    assert(columnStats.size == expectedColStatsSeq.length)
    expectedColStatsSeq.foreach { case (field, expectedColStat) =>
      StatisticsTest.checkColStat(
        dataType = field.dataType,
        colStat = columnStats(field.name),
        expectedColStat = expectedColStat,
        rsd = spark.sessionState.conf.ndvMaxError)
    }
  }

  test("generate and load column-level stats for data source table") {
    val dsTable = "dsTable"
    withTable(dsTable) {
      testDataFrame.write.format("parquet").saveAsTable(dsTable)
      sql(s"ANALYZE TABLE $dsTable COMPUTE STATISTICS FOR COLUMNS c1, c2, c3, c4")
      checkColStats(dsTable, isDataSourceTable = true, expectedColStatsSeq)
    }
  }

  test("generate and load column-level stats for hive serde table") {
    val hTable = "hTable"
    val tmp = "tmp"
    withTable(hTable, tmp) {
      testDataFrame.write.format("parquet").saveAsTable(tmp)
      sql(s"CREATE TABLE $hTable (c1 int, c2 string, c3 binary, c4 boolean) STORED AS TEXTFILE")
      sql(s"INSERT INTO $hTable SELECT * FROM $tmp")
      sql(s"ANALYZE TABLE $hTable COMPUTE STATISTICS FOR COLUMNS c1, c2, c3, c4")
      checkColStats(hTable, isDataSourceTable = false, expectedColStatsSeq)
    }
  }

  // When caseSensitive is on, for columns with only case difference, they are different columns
  // and we should generate column stats for all of them.
  private def checkCaseSensitiveColStats(columnName: String): Unit = {
    val tableName = "tbl"
    withTable(tableName) {
      val column1 = columnName.toLowerCase
      val column2 = columnName.toUpperCase
      withSQLConf("spark.sql.caseSensitive" -> "true") {
        sql(s"CREATE TABLE $tableName (`$column1` int, `$column2` double) USING PARQUET")
        sql(s"INSERT INTO $tableName SELECT 1, 3.0")
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS `$column1`, `$column2`")
        val readback = spark.table(tableName)
        val relations = readback.queryExecution.analyzed.collect { case rel: LogicalRelation =>
          val columnStats = rel.catalogTable.get.stats.get.colStats
          assert(columnStats.size == 2)
          StatisticsTest.checkColStat(
            dataType = IntegerType,
            colStat = columnStats(column1),
            expectedColStat = ColumnStat(InternalRow(0L, 1, 1, 1L)),
            rsd = spark.sessionState.conf.ndvMaxError)
          StatisticsTest.checkColStat(
            dataType = DoubleType,
            colStat = columnStats(column2),
            expectedColStat = ColumnStat(InternalRow(0L, 3.0d, 3.0d, 1L)),
            rsd = spark.sessionState.conf.ndvMaxError)
          rel
        }
        assert(relations.size == 1)
      }
    }
  }

  test("check column statistics for case sensitive column names") {
    checkCaseSensitiveColStats(columnName = "c1")
  }

  test("check column statistics for case sensitive non-ascii column names") {
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkCaseSensitiveColStats(columnName = "列c")
    // scalastyle:on
  }

  test("estimates the size of a test MetastoreRelation") {
    val df = sql("""SELECT * FROM src""")
    val sizes = df.queryExecution.analyzed.collect { case mr: MetastoreRelation =>
      mr.statistics.sizeInBytes
    }
    assert(sizes.size === 1, s"Size wrong for:\n ${df.queryExecution}")
    assert(sizes(0).equals(BigInt(5812)),
      s"expected exact size 5812 for test table 'src', got: ${sizes(0)}")
  }

  test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Row],
        ct: ClassTag[_]): Unit = {
      before()

      var df = sql(query)

      // Assert src has a size smaller than the threshold.
      val sizes = df.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) => r.statistics.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold
        && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      var bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${df.queryExecution}")

      checkAnswer(df, expectedAnswer) // check correctness of output

      spark.sessionState.conf.settings.synchronized {
        val tmp = spark.sessionState.conf.autoBroadcastJoinThreshold

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1""")
        df = sql(query)
        bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
        assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

        val shj = df.queryExecution.sparkPlan.collect { case j: SortMergeJoinExec => j }
        assert(shj.size === 1,
          "SortMergeJoin should be planned when BroadcastHashJoin is turned off")

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp""")
      }

      after()
    }

    /** Tests for MetastoreRelation */
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)(Row(238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[MetastoreRelation]]
    )
  }

  test("auto converts to broadcast left semi join, by size estimate of a relation") {
    val leftSemiJoinQuery =
      """SELECT * FROM src a
        |left semi JOIN src b ON a.key=86 and a.key = b.key""".stripMargin
    val answer = Row(86, "val_86")

    var df = sql(leftSemiJoinQuery)

    // Assert src has a size smaller than the threshold.
    val sizes = df.queryExecution.analyzed.collect {
      case r if implicitly[ClassTag[MetastoreRelation]].runtimeClass
        .isAssignableFrom(r.getClass) =>
        r.statistics.sizeInBytes
    }
    assert(sizes.size === 2 && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold
      && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
      s"query should contain two relations, each of which has size smaller than autoConvertSize")

    // Using `sparkPlan` because for relevant patterns in HashJoin to be
    // matched, other strategies need to be applied.
    var bhj = df.queryExecution.sparkPlan.collect {
      case j: BroadcastHashJoinExec => j
    }
    assert(bhj.size === 1,
      s"actual query plans do not contain broadcast join: ${df.queryExecution}")

    checkAnswer(df, answer) // check correctness of output

    spark.sessionState.conf.settings.synchronized {
      val tmp = spark.sessionState.conf.autoBroadcastJoinThreshold

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1")
      df = sql(leftSemiJoinQuery)
      bhj = df.queryExecution.sparkPlan.collect {
        case j: BroadcastHashJoinExec => j
      }
      assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

      val shj = df.queryExecution.sparkPlan.collect {
        case j: SortMergeJoinExec => j
      }
      assert(shj.size === 1,
        "SortMergeJoinExec should be planned when BroadcastHashJoin is turned off")

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp")
    }

  }
}
