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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.plans.logical.BasicColStats
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types._

class StatisticsColumnSuite extends StatisticsTest {

  test("parse analyze column commands") {
    val table = "table"
    assertAnalyzeCommand(
      s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS key, value",
      classOf[AnalyzeColumnCommand])

    val noColumnError = intercept[AnalysisException] {
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS")
    }
    assert(noColumnError.message == "Need to specify the columns to analyze. Usage: " +
      "ANALYZE TABLE tbl COMPUTE STATISTICS FOR COLUMNS key, value")

    withTable(table) {
      sql(s"CREATE TABLE $table (key INT, value STRING)")
      val invalidColError = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS k")
      }
      assert(invalidColError.message == s"Invalid column name: k")

      val duplicateColError = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS key, value, key")
      }
      assert(duplicateColError.message == s"Duplicate column name: key")

      withSQLConf("spark.sql.caseSensitive" -> "true") {
        val invalidErr = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS keY")
        }
        assert(invalidErr.message == s"Invalid column name: keY")
      }

      withSQLConf("spark.sql.caseSensitive" -> "false") {
        val duplicateErr = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS key, value, vaLue")
        }
        assert(duplicateErr.message == s"Duplicate column name: vaLue")
      }
    }
  }

  test("basic statistics for integral type columns") {
    val rdd = sparkContext.parallelize(Seq("1", null, "2", "3", null)).map { i =>
      if (i != null) Row(i.toByte, i.toShort, i.toInt, i.toLong) else Row(i, i, i, i)
    }
    val schema = StructType(
      StructField(name = "c1", dataType = ByteType, nullable = true) ::
        StructField(name = "c2", dataType = ShortType, nullable = true) ::
        StructField(name = "c3", dataType = IntegerType, nullable = true) ::
        StructField(name = "c4", dataType = LongType, nullable = true) :: Nil)
    val expectedBasicStats = BasicColStats(
      dataType = ByteType, numNulls = 2, max = Some(3), min = Some(1), ndv = Some(3))
    val statsSeq = Seq(
      ("c1", expectedBasicStats),
      ("c2", expectedBasicStats.copy(dataType = ShortType)),
      ("c3", expectedBasicStats.copy(dataType = IntegerType)),
      ("c4", expectedBasicStats.copy(dataType = LongType)))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for fractional type columns") {
    val rdd = sparkContext.parallelize(Seq(null, "1.01", "2.02", "3.03")).map { i =>
      if (i != null) Row(i.toFloat, i.toDouble, Decimal(i)) else Row(i, i, i)
    }
    val schema = StructType(
      StructField(name = "c1", dataType = FloatType, nullable = true) ::
        StructField(name = "c2", dataType = DoubleType, nullable = true) ::
        StructField(name = "c3", dataType = DecimalType.SYSTEM_DEFAULT, nullable = true) :: Nil)
    val expectedBasicStats = BasicColStats(
      dataType = FloatType, numNulls = 1, max = Some(3.03), min = Some(1.01), ndv = Some(3))
    val statsSeq = Seq(
      ("c1", expectedBasicStats),
      ("c2", expectedBasicStats.copy(dataType = DoubleType)),
      ("c3", expectedBasicStats.copy(dataType = DecimalType.SYSTEM_DEFAULT)))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for string column") {
    val rdd = sparkContext.parallelize(Seq(null, "a", "bbbb", "cccc")).map(Row(_))
    val schema = StructType(StructField(name = "c1", dataType = StringType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = StringType, numNulls = 1,
      maxColLen = Some(4), avgColLen = Some(2.25), ndv = Some(3))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for binary column") {
    val rdd = sparkContext.parallelize(Seq(null, "a", "bbbb", "cccc")).map { i =>
      if (i != null) Row(i.getBytes) else Row(i)
    }
    val schema = StructType(StructField(name = "c1", dataType = BinaryType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = BinaryType, numNulls = 1,
      maxColLen = Some(4), avgColLen = Some(2.25))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for boolean column") {
    val rdd = sparkContext.parallelize(Seq(null, true, false, true)).map(Row(_))
    val schema =
      StructType(StructField(name = "c1", dataType = BooleanType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = BooleanType, numNulls = 1,
      numTrues = Some(2), numFalses = Some(1))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for date column") {
    val rdd = sparkContext.parallelize(Seq(null, "1970-01-01", "1970-02-02")).map { i =>
      if (i != null) Row(Date.valueOf(i)) else Row(i)
    }
    val schema =
      StructType(StructField(name = "c1", dataType = DateType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = DateType, numNulls = 1,
      max = Some(32), min = Some(0), ndv = Some(2))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for timestamp column") {
    val rdd = sparkContext.parallelize(Seq(null, "1970-01-01 00:00:00", "1970-01-01 00:00:05"))
      .map(i => if (i != null) Row(Timestamp.valueOf(i)) else Row(i))
    val schema =
      StructType(StructField(name = "c1", dataType = TimestampType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = TimestampType, numNulls = 1,
      max = Some(5000000), min = Some(0), ndv = Some(2))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for null columns") {
    val rdd = sparkContext.parallelize(Seq(Row(null, null)))
    val schema = StructType(
      StructField(name = "c1", dataType = LongType, nullable = true) ::
        StructField(name = "c2", dataType = TimestampType, nullable = true) :: Nil)
    val expectedBasicStats = BasicColStats(dataType = LongType, numNulls = 1,
      max = None, min = None, ndv = Some(0))
    val statsSeq = Seq(
      ("c1", expectedBasicStats),
      ("c2", expectedBasicStats.copy(dataType = TimestampType)))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for columns with different types") {
    val rdd = sparkContext.parallelize(Seq(
      Row(1, 1.01, "a", "a".getBytes, true, Date.valueOf("1970-01-01"),
        Timestamp.valueOf("1970-01-01 00:00:00"), 5.toLong),
      Row(2, 2.02, "bb", "bb".getBytes, false, Date.valueOf("1970-02-02"),
        Timestamp.valueOf("1970-01-01 00:00:05"), 4.toLong)))
    val schema = StructType(Seq(
      StructField(name = "c1", dataType = IntegerType, nullable = false),
      StructField(name = "c2", dataType = DoubleType, nullable = false),
      StructField(name = "c3", dataType = StringType, nullable = false),
      StructField(name = "c4", dataType = BinaryType, nullable = false),
      StructField(name = "c5", dataType = BooleanType, nullable = false),
      StructField(name = "c6", dataType = DateType, nullable = false),
      StructField(name = "c7", dataType = TimestampType, nullable = false),
      StructField(name = "c8", dataType = LongType, nullable = false)))
    val statsSeq = Seq(
      ("c1", BasicColStats(dataType = IntegerType, numNulls = 0, max = Some(2), min = Some(1),
        ndv = Some(2))),
      ("c2", BasicColStats(dataType = DoubleType, numNulls = 0, max = Some(2.02), min = Some(1.01),
        ndv = Some(2))),
      ("c3", BasicColStats(dataType = StringType, numNulls = 0, maxColLen = Some(2),
        avgColLen = Some(1.5), ndv = Some(2))),
      ("c4", BasicColStats(dataType = BinaryType, numNulls = 0, maxColLen = Some(2),
        avgColLen = Some(1.5))),
      ("c5", BasicColStats(dataType = BooleanType, numNulls = 0, numTrues = Some(1),
        numFalses = Some(1), ndv = Some(2))),
      ("c6", BasicColStats(dataType = DateType, numNulls = 0, max = Some(32), min = Some(0),
        ndv = Some(2))),
      ("c7", BasicColStats(dataType = TimestampType, numNulls = 0, max = Some(5000000),
        min = Some(0), ndv = Some(2))),
      ("c8", BasicColStats(dataType = LongType, numNulls = 0, max = Some(5), min = Some(4),
        ndv = Some(2))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("update table-level stats while collecting column-level stats") {
    val table = "tbl"
    val tmpTable = "tmp"
    withTable(table, tmpTable) {
      val rdd = sparkContext.parallelize(Seq(Row(1)))
      val df = spark.createDataFrame(rdd, StructType(Seq(
        StructField(name = "c1", dataType = IntegerType, nullable = false))))
      df.write.format("json").saveAsTable(tmpTable)

      sql(s"CREATE TABLE $table (c1 int)")
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      val fetchedStats1 = checkTableStats(tableName = table, isDataSourceTable = false,
        hasSizeInBytes = true, expectedRowCounts = Some(1))

      // update table between analyze table and analyze column commands
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats2 = checkTableStats(tableName = table, isDataSourceTable = false,
        hasSizeInBytes = true, expectedRowCounts = Some(2))
      assert(fetchedStats2.get.sizeInBytes > fetchedStats1.get.sizeInBytes)

      val basicColStats = fetchedStats2.get.basicColStats("c1")
      checkColStats(colStats = basicColStats, expectedColStats = BasicColStats(
        dataType = IntegerType, numNulls = 0, max = Some(1), min = Some(1), ndv = Some(1)))
    }
  }
}
