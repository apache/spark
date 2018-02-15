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

package org.apache.spark.sql.hive.execution

import java.io.File
import java.net.URI

import org.apache.spark.SparkException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, HiveHash, HiveHashFunction}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.IntegerType

/**
 * Tests Spark's support for Hive bucketing
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL+BucketedTables
 */
class HiveBucketingSuite extends HiveComparisonTest with SQLTestUtils with TestHiveSingleton {
  private val (table1, table2, table3) = ("table1", "table2", "table3")
  private val (partitionedTable1, partitionedTable2) = ("partitionedTable1", "partitionedTable2")

  private val bucketSpec = Some(BucketSpec(8, Seq("key"), Seq("key", "value")))

  override def beforeAll(): Unit = {
    super.beforeAll()

    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      createTable(table1, isPartitioned = false, bucketSpec)
      createTable(table2, isPartitioned = false, bucketSpec)
      createTable(table3, isPartitioned = false, None)
      createTable(partitionedTable1, isPartitioned = true, bucketSpec)
      createTable(partitionedTable2, isPartitioned = true, bucketSpec)
    }
  }

  override def afterAll(): Unit = {
    try {
      Seq(table1, table2, partitionedTable1, partitionedTable2)
        .foreach(table => sql(s"DROP TABLE IF EXISTS $table"))
    } finally {
      super.afterAll()
    }
  }

  private def createTable(
      tableName: String,
      isPartitioned: Boolean,
      bucketSpec: Option[BucketSpec]): Unit = {

    val bucketClause =
      bucketSpec.map(b =>
        s"CLUSTERED BY (${b.bucketColumnNames.mkString(",")}) " +
          s"SORTED BY (${b.sortColumnNames.map(_ + " ASC").mkString(" ,")}) " +
          s"INTO ${b.numBuckets} buckets"
      ).getOrElse("")

    val partitionClause = if (isPartitioned) "PARTITIONED BY(part STRING)" else ""

    sql(s"""
           |CREATE TABLE IF NOT EXISTS $tableName (key int, value string) $partitionClause
           |$bucketClause ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
           |""".stripMargin)

    val data = 0 until 20
    val df = if (isPartitioned) {
      (data.map(i => (i, i.toString, "part0")) ++ data.map(i => (i, i.toString, "part1")))
        .toDF("key", "value", "part")
    } else {
      data.map(i => (i, i.toString)).toDF("key", "value")
    }
    df.write.mode(SaveMode.Overwrite).insertInto(tableName)
  }

  case class BucketedTableTestSpec(
      bucketSpec: Option[BucketSpec] = bucketSpec,
      expectShuffle: Boolean = false,
      expectSort: Boolean = false)

  private def testBucketing(
      queryString: String,
      bucketedTableTestSpecLeft: BucketedTableTestSpec = BucketedTableTestSpec(),
      bucketedTableTestSpecRight: BucketedTableTestSpec = BucketedTableTestSpec(),
      bucketingEnabled: Boolean = true): Unit = {

    def validateChildNode(
        child: SparkPlan,
        bucketSpec: Option[BucketSpec],
        expectedShuffle: Boolean,
        expectedSort: Boolean): Unit = {
      val exchange = child.find(_.isInstanceOf[ShuffleExchangeExec])
      assert(if (expectedShuffle) exchange.isDefined else exchange.isEmpty)

      val sort = child.find(_.isInstanceOf[SortExec])
      assert(if (expectedSort) sort.isDefined else sort.isEmpty)

      val tableScanNode = child.find(_.isInstanceOf[HiveTableScanExec])
      assert(tableScanNode.isDefined)
      val tableScan = tableScanNode.get.asInstanceOf[HiveTableScanExec]

      bucketSpec match {
        case Some(spec) if bucketingEnabled =>
          assert(tableScan.outputPartitioning.isInstanceOf[HashPartitioning])
          val part = tableScan.outputPartitioning.asInstanceOf[HashPartitioning]
          assert(part.hashingFunctionClass == classOf[HiveHash])
          assert(part.numPartitions == spec.numBuckets)

          for ((columnName, index) <- spec.bucketColumnNames.zipWithIndex) {
            part.expressions(index) match {
              case a: AttributeReference => assert(a.name == columnName)
            }
          }

          import org.apache.spark.sql.catalyst.dsl.expressions._
          part.semanticEquals(HashPartitioning(Seq('key), spec.numBuckets, classOf[HiveHash]))

          val ordering = tableScan.outputOrdering
          for ((columnName, sortOrder) <- spec.sortColumnNames.zip(ordering)) {
            assert(sortOrder.direction === Ascending)
            sortOrder.child match {
              case a: AttributeReference => assert(a.name == columnName)
            }
          }
        case _ => // do nothing
      }
    }

    val BucketedTableTestSpec(bucketSpecLeft, shuffleLeft, sortLeft) = bucketedTableTestSpecLeft
    val BucketedTableTestSpec(bucketSpecRight, shuffleRight, sortRight) = bucketedTableTestSpecRight

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.BUCKETING_ENABLED.key -> bucketingEnabled.toString) {

      val plan = sql(queryString).queryExecution.executedPlan
      val joinNode = plan.find(_.isInstanceOf[SortMergeJoinExec])
      assert(joinNode.isDefined)
      val joinOperator = joinNode.get.asInstanceOf[SortMergeJoinExec]

      validateChildNode(joinOperator.left, bucketSpecLeft, shuffleLeft, sortLeft)
      validateChildNode(joinOperator.right, bucketSpecRight, shuffleRight, sortRight)
    }
  }

  test("Join two bucketed tables with bucketing enabled") {
    testBucketing(s"SELECT * FROM $table1 a JOIN $table2 b ON a.key = b.key")
  }

  test("Join two bucketed tables with bucketing DISABLED") {
    testBucketing(
      s"SELECT * FROM $table1 a JOIN $table2 b ON a.key = b.key",
      BucketedTableTestSpec(expectShuffle = true, expectSort = true),
      BucketedTableTestSpec(expectShuffle = true, expectSort = true),
      bucketingEnabled = false)
  }

  test("Join a regular table with a bucketed table") {
    testBucketing(
      s"SELECT * FROM $table3 a JOIN $table1 b ON a.key = b.key",
      BucketedTableTestSpec(bucketSpec = None, expectShuffle = true, expectSort = true))
  }

  test("Join two bucketed tables but not over their bucketing column(s)") {
    testBucketing(
      s"SELECT a.value, b.value FROM $table1 a JOIN $table2 b ON a.value = b.value",
      BucketedTableTestSpec(expectShuffle = true, expectSort = true),
      BucketedTableTestSpec(expectShuffle = true, expectSort = true))
  }

  test("Join where predicate has other clauses in addition to bucketing column(s)") {
    testBucketing(
      s"SELECT a.value, b.value " +
        s"FROM $table1 a JOIN $table2 b " +
        s"ON a.key = b.key AND a.value = b.value",
      BucketedTableTestSpec(expectShuffle = true, expectSort = true),
      BucketedTableTestSpec(expectShuffle = true, expectSort = true))
  }

  test("Join two partitioned tables (single partition scan) with bucketing enabled") {
    testBucketing(
      s"""
          SELECT * FROM $partitionedTable1 a JOIN $partitionedTable2 b
          ON a.key = b.key AND a.part = "part0" AND b.part = "part0"
        """.stripMargin
    )
  }

  test("Join two partitioned tables with single partition scanned on one side and multiple " +
    "partition scanned on the other one") {
    testBucketing(
      s"""
         SELECT * FROM $partitionedTable1 a JOIN $partitionedTable2 b
         ON a.key = b.key AND b.part = "part0"
       """.stripMargin,
      BucketedTableTestSpec(expectSort = true)
    )
  }

  test("Join partitioned tables with non-partitioned table (both bucketed)") {
    testBucketing(
      s"""
         SELECT * FROM $table1 a JOIN $partitionedTable2 b ON a.key = b.key AND b.part = "part0"
       """.stripMargin
    )
  }

  test("Join two partitioned tables (multiple partitions scan) with bucketing enabled") {
    testBucketing(
      s"SELECT * FROM $partitionedTable1 a JOIN $partitionedTable2 b ON a.key = b.key",
      BucketedTableTestSpec(expectSort = true),
      BucketedTableTestSpec(expectSort = true)
    )
  }

  private def validateBucketingAndSorting(numBuckets: Int, dir: URI): Unit = {
    val bucketFiles = new File(dir).listFiles().filter(_.getName.startsWith("part-"))
      .sortWith((x, y) => x.getName < y.getName)
    assert(bucketFiles.length === numBuckets)

    bucketFiles.zipWithIndex.foreach { case(bucketFile, bucketId) =>
      val rows = spark.read.format("text").load(bucketFile.getAbsolutePath).collect()
      var prevKey: Option[Int] = None
      rows.foreach(row => {
        val key = row.getString(0).split("\t")(0).toInt
        assert(HiveHashFunction.hash(key, IntegerType, seed = 0) % numBuckets === bucketId)

        if (prevKey.isDefined) {
          assert(prevKey.get <= key)
        }
        prevKey = Some(key)
      })
    }
  }

  test("Write data to a non-partitioned bucketed table") {
    val numBuckets = 8
    val tableName = "nonPartitionedBucketed"

    withTable(tableName) {
      val session = spark.sessionState
      sql(s"""
             |CREATE TABLE $tableName (key int, value string)
             |CLUSTERED BY (key) SORTED BY (key ASC) into $numBuckets buckets
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
             |""".stripMargin)

      (0 until 100)
        .map(i => (i, i.toString)).toDF("key", "value")
        .write.mode(SaveMode.Overwrite).insertInto(tableName)

      val dir = session.catalog.defaultTablePath(session.sqlParser.parseTableIdentifier(tableName))
      validateBucketingAndSorting(numBuckets, dir)
    }
  }

  test("Write data to a bucketed table with static partition") {
    val numBuckets = 8
    val tableName = "bucketizedTable"
    val sourceTableName = "sourceTable"

    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable(tableName, sourceTableName) {
        sql(s"""
               |CREATE TABLE $tableName (key int, value string)
               |PARTITIONED BY(part1 STRING, part2 STRING)
               |CLUSTERED BY (key) SORTED BY (key ASC) into $numBuckets buckets
               |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
               |""".stripMargin)

        (0 until 100)
          .map(i => (i, i.toString))
          .toDF("key", "value")
          .createOrReplaceTempView(sourceTableName)

        sql(s"""
               |INSERT OVERWRITE TABLE $tableName PARTITION(part1="val1", part2="val2")
               |SELECT key, value
               |FROM $sourceTableName
               |""".stripMargin)

        val dir = spark.sessionState.catalog.getPartition(
          spark.sessionState.sqlParser.parseTableIdentifier(tableName),
          Map("part1" -> "val1", "part2" -> "val2")
        ).location

        validateBucketingAndSorting(numBuckets, dir)
      }
    }
  }

  test("Write data to a bucketed table with dynamic partitions") {
    val numBuckets = 7
    val tableName = "bucketizedTable"

    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable(tableName) {
        sql(s"""
               |CREATE TABLE $tableName (key int, value string)
               |PARTITIONED BY(part1 STRING, part2 STRING)
               |CLUSTERED BY (key) SORTED BY (key ASC) into $numBuckets buckets
               |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
               |""".stripMargin)

        (0 until 100)
          .map(i => (i, i.toString, (if (i > 50) i % 2 else 2 - i % 2).toString, (i % 3).toString))
          .toDF("key", "value", "part1", "part2")
          .write.mode(SaveMode.Overwrite).insertInto(tableName)

        (0 until 2).zip(0 until 3).foreach { case (part1, part2) =>
          val dir = spark.sessionState.catalog.getPartition(
            spark.sessionState.sqlParser.parseTableIdentifier(tableName),
            Map("part1" -> part1.toString, "part2" -> part2.toString)
          ).location

          validateBucketingAndSorting(numBuckets, dir)
        }
      }
    }
  }

  test("Write data to a bucketed table with dynamic partitions (along with static partitions)") {
    val numBuckets = 8
    val tableName = "bucketizedTable"
    val sourceTableName = "sourceTable"
    val part1StaticValue = "0"

    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable(tableName, sourceTableName) {
        sql(s"""
               |CREATE TABLE $tableName (key int, value string)
               |PARTITIONED BY(part1 STRING, part2 STRING)
               |CLUSTERED BY (key) SORTED BY (key ASC) into $numBuckets buckets
               |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
               |""".stripMargin)

        (0 until 100)
          .map(i => (i, i.toString, (i % 3).toString))
          .toDF("key", "value", "part")
          .createOrReplaceTempView(sourceTableName)

        sql(s"""
               |INSERT OVERWRITE TABLE $tableName PARTITION(part1="$part1StaticValue", part2)
               |SELECT key, value, part
               |FROM $sourceTableName
               |""".stripMargin)

        (0 until 3).foreach { case part2 =>
          val dir = spark.sessionState.catalog.getPartition(
            spark.sessionState.sqlParser.parseTableIdentifier(tableName),
            Map("part1" -> part1StaticValue, "part2" -> part2.toString)
          ).location

          validateBucketingAndSorting(numBuckets, dir)
        }
      }
    }
  }

  test("Appends to bucketed table should NOT be allowed as it breaks bucketing guarantee") {
    val numBuckets = 8
    val tableName = "nonPartitionedBucketed"

    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $tableName (key int, value string)
             |CLUSTERED BY (key) SORTED BY (key ASC) into $numBuckets buckets
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
             |""".stripMargin)

      val df = (0 until 100).map(i => (i, i.toString)).toDF("key", "value")
      val e = intercept[SparkException] {
        df.write.mode(SaveMode.Append).insertInto(tableName)
      }
      assert(e.getMessage.contains("Appending data to hive bucketed table is not allowed"))
    }
  }

  test("Fail the query if number of files produced != number of buckets") {
    val numBuckets = 8
    val tableName = "nonPartitionedBucketed"

    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $tableName (key int, value string)
             |CLUSTERED BY (key) SORTED BY (key ASC) into $numBuckets buckets
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
             |""".stripMargin)

      val df = (0 until (numBuckets / 2)).map(i => (i, i.toString)).toDF("key", "value")
      val e = intercept[SparkException] {
        df.write.mode(SaveMode.Overwrite).insertInto(tableName)
      }
      assert(e.getMessage.contains("Potentially missing bucketed output files"))
    }
  }
}
