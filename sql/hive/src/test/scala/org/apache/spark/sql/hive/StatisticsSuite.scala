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
import java.util.UUID

import scala.reflect.ClassTag

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, GreaterThan, In, IsNotNull, Literal, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.DataTypes

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

        val sizeInBytes = relation.statistics().sizeInBytes
        assert(sizeInBytes === BigInt(file1.length() + file2.length()))
      }
    } finally {
      spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, enableFallBackToHdfsForStats)
      sql("DROP TABLE csv_table ")
    }
  }

  test("MetastoreRelations partition pruning enabled for stats estimation.") {
    val enablePartitionPruning = spark.sessionState.conf.prunedPartitionStatsEnabled
    val enableFallBackToHdfsForStats = spark.sessionState.conf.fallBackToHdfsForStatsEnabled
    try {
      withTempDir { tempDir =>

        def writeNRecords(n: Int): File = {
          val file1 = new File(tempDir, UUID.randomUUID.toString)
          val writer1 = new PrintWriter(file1)
          for(i <- 1 to n) {
            writer1.write("1,2")
          }
          writer1.close()
          file1
        }

        val largeFile = writeNRecords(1000)
        val smallFile = writeNRecords(10)

        spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, true)

        sql(
          s"""CREATE TABLE csv_table(page_id INT, impressions INT)
            PARTITIONED BY (dateint INT, hour INT)
            ROW FORMAT delimited fields terminated by ','
          """)

        sql(
          s"""LOAD DATA LOCAL INPATH '${largeFile.getAbsolutePath}' INTO TABLE csv_table partition
               (dateint=20160717, hour=10)
          """)
        sql(
          s"""LOAD DATA LOCAL INPATH '${largeFile.getAbsolutePath}' INTO TABLE csv_table partition
               (dateint=20160717, hour=11)
          """)
        sql(
          s"""LOAD DATA LOCAL INPATH '${smallFile.getAbsolutePath}' INTO TABLE csv_table partition
               (dateint=20160718, hour=10)
          """)
        sql(
          s"""LOAD DATA LOCAL INPATH '${smallFile.getAbsolutePath}' INTO TABLE csv_table partition
               (dateint=20160718, hour=11)
          """)

        val relation = spark.sessionState.catalog.lookupRelation(TableIdentifier("csv_table"))
          .asInstanceOf[MetastoreRelation]

        val hadoopConf = spark.sessionState.newHadoopConf
        val fs: FileSystem = relation.hiveQlTable.getPath.getFileSystem(hadoopConf)

        val totalSize = relation.getSize
        val lLen = fs.getContentSummary(new Path(relation.hiveQlTable.getPath,
          "dateint=20160717")).getLength/2
        val sLen = fs.getContentSummary(new Path(relation.hiveQlTable.getPath,
          "dateint=20160718")).getLength/2

        def assertion(filter: Filter, partitionPrunedSize: Long, message: String): Unit = {
          assert(relation.statistics(Some(Seq(filter))).sizeInBytes == BigInt(totalSize), message)

          spark.conf.set(SQLConf.ENABLE_PRUNED_PARTITION_STATS.key, true)
          val sizeInBytes = relation.statistics(Some(Seq(filter))).sizeInBytes
          assert(sizeInBytes == BigInt(partitionPrunedSize), message)
          spark.conf.set(SQLConf.ENABLE_PRUNED_PARTITION_STATS.key, false)
        }

        val dateInt = relation.partitionKeys.find(_.name.equals("dateint")).get
        val hour = relation.partitionKeys.find(_.name.equals("hour")).get
        val literalDate17 = Literal.create(20160717, DataTypes.IntegerType)
        val literalDate18 = Literal.create(20160718, DataTypes.IntegerType)
        val literalHour10 = Literal.create(10, DataTypes.IntegerType)
        val literalHour11 = Literal.create(11, DataTypes.IntegerType)

        val equalToDate17 = EqualTo(dateInt, literalDate17)
        val equalToDate18 = EqualTo(dateInt, literalDate18)
        val equalToHour10 = EqualTo(hour, literalHour10)
        val equalToHour11 = EqualTo(hour, literalHour11)

        val hour10Or11 = Or(equalToHour10, equalToHour11)
        val date17AndHour10Or11 = And(equalToDate17, hour10Or11)
        val date18AndHour11 = And(equalToDate18, equalToHour11)
        val hour10orDate18AndHour11 = Or(equalToHour10, date18AndHour11)

        val notNull = IsNotNull(literalHour10)
        val notNullAndDate18AndHour11 = And(notNull, date18AndHour11)

        val not = Not(hour10orDate18AndHour11)
        val greaterThanHour10 = GreaterThan(hour, literalHour10)
        val date18AndGreaterThanHour10 = And(equalToDate18, greaterThanHour10)

        val date18andHourIn1011 = And(equalToDate18, In(hour, List(literalHour10, literalHour11)))

        val testFilters = Seq(
          // equality check
          (new Filter(equalToDate17, null), lLen * 2, "equalToDate17"),

          // And and Or check
          (new Filter(date17AndHour10Or11, null), lLen * 2, "date17AndHour10Or11"),

          // And and Or check, but or has only hour so it should be ignored.
          (new Filter(hour10orDate18AndHour11, null), sLen, "hour10orDate18AndHour11"),

          // not Null should not blacklist partition column
          (new Filter(notNullAndDate18AndHour11, null), sLen, "notNullAndDate18AndHour11"),

          // Not should blacklist partition col, falling back to total size.
          (new Filter(not, null), totalSize, "not"),

          // unsupported operator should fall back to total size.
          (new Filter(greaterThanHour10, null), totalSize, "greaterThanHour10"),

          // unsupported operator should only blacklist col used in that operator.
          (new Filter(date18AndGreaterThanHour10, null), sLen * 2, "date18AndGreaterThanHour10"),

          // In Check with And
          (new Filter(date18andHourIn1011, null), sLen * 2, "date18andHourIn1011")
        )

        testFilters.foreach(tuple => assertion(tuple._1, tuple._2, tuple._3))
      }
    } finally {
      spark.conf.set(SQLConf.ENABLE_PRUNED_PARTITION_STATS.key, enablePartitionPruning)
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
      TableIdentifier("tempTable"), ignoreIfNotExists = true)
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
