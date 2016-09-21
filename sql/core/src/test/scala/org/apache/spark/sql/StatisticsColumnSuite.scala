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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.ColumnStats
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types._

class StatisticsColumnSuite extends StatisticsTest {
  import testImplicits._

  test("parse analyze column commands") {
    def assertAnalyzeColumnCommand(analyzeCommand: String, c: Class[_]) {
      val parsed = spark.sessionState.sqlParser.parsePlan(analyzeCommand)
      val operators = parsed.collect {
        case a: AnalyzeColumnCommand => a
        case o => o
      }
      assert(operators.size == 1)
      if (operators.head.getClass != c) {
        fail(
          s"""$analyzeCommand expected command: $c, but got ${operators.head}
             |parsed command:
             |$parsed
           """.stripMargin)
      }
    }

    val table = "table"
    assertAnalyzeColumnCommand(
      s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS key, value",
      classOf[AnalyzeColumnCommand])

    intercept[ParseException] {
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS")
    }
  }

  test("check correctness of columns") {
    val table = "tbl"
    val colName1 = "abc"
    val colName2 = "x.yz"
    val quotedColName2 = s"`$colName2`"
    withTable(table) {
      sql(s"CREATE TABLE $table ($colName1 int, $quotedColName2 string) USING PARQUET")

      val invalidColError = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS key")
      }
      assert(invalidColError.message == "Invalid column name: key.")

      withSQLConf("spark.sql.caseSensitive" -> "true") {
        val invalidErr = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS ${colName1.toUpperCase}")
        }
        assert(invalidErr.message == s"Invalid column name: ${colName1.toUpperCase}.")
      }

      withSQLConf("spark.sql.caseSensitive" -> "false") {
        val columnsToAnalyze = Seq(colName2.toUpperCase, colName1, colName2)
        val columnStats = spark.sessionState.computeColumnStats(table, columnsToAnalyze)
        assert(columnStats.contains(colName1))
        assert(columnStats.contains(colName2))
        // check deduplication
        assert(columnStats.size == 2)
        assert(!columnStats.contains(colName2.toUpperCase))
      }
    }
  }

  private def getNonNullValues[T](values: Seq[Option[T]]): Seq[T] = {
    values.filter(_.isDefined).map(_.get)
  }

  test("column-level statistics for integral type columns") {
    val values = (0 to 5).map { i =>
      if (i % 2 == 0) None else Some(i)
    }
    val data = values.map { i =>
      (i.map(_.toByte), i.map(_.toShort), i.map(_.toInt), i.map(_.toLong))
    }

    val df = data.toDF("c1", "c2", "c3", "c4")
    val nonNullValues = getNonNullValues[Int](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        max = Some(nonNullValues.max),
        min = Some(nonNullValues.min),
        ndv = Some(nonNullValues.distinct.length.toLong))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for fractional type columns") {
    val values = (0 to 5).map { i =>
      if (i == 0) None else Some(i + i * 0.01d)
    }
    val data = values.map { i =>
      (i.map(_.toFloat), i.map(_.toDouble), i.map(Decimal(_)))
    }

    val df = data.toDF("c1", "c2", "c3")
    val nonNullValues = getNonNullValues[Double](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        max = Some(nonNullValues.max),
        min = Some(nonNullValues.min),
        ndv = Some(nonNullValues.distinct.length.toLong))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for string column") {
    val values = Seq(None, Some("a"), Some("bbbb"), Some("cccc"), Some(""))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[String](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        ndv = Some(nonNullValues.distinct.length.toLong),
        maxColLen = Some(nonNullValues.map(_.length).max.toLong),
        avgColLen = Some(nonNullValues.map(_.length).sum / nonNullValues.length.toDouble))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for binary column") {
    val values = Seq(None, Some("a"), Some("bbbb"), Some("cccc"), Some("")).map(_.map(_.getBytes))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Array[Byte]](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        maxColLen = Some(nonNullValues.map(_.length).max.toLong),
        avgColLen = Some(nonNullValues.map(_.length).sum / nonNullValues.length.toDouble))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for boolean column") {
    val values = Seq(None, Some(true), Some(false), Some(true))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Boolean](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        numTrues = Some(nonNullValues.count(_.equals(true)).toLong),
        numFalses = Some(nonNullValues.count(_.equals(false)).toLong))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for date column") {
    val values = Seq(None, Some("1970-01-01"), Some("1970-02-02")).map(_.map(Date.valueOf))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Date](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        // Internally, DateType is represented as the number of days from 1970-01-01.
        max = Some(nonNullValues.map(DateTimeUtils.fromJavaDate).max),
        min = Some(nonNullValues.map(DateTimeUtils.fromJavaDate).min),
        ndv = Some(nonNullValues.distinct.length.toLong))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for timestamp column") {
    val values = Seq(None, Some("1970-01-01 00:00:00"), Some("1970-01-01 00:00:05")).map { i =>
      i.map(Timestamp.valueOf)
    }
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Timestamp](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = ColumnStats(
        dataType = f.dataType,
        numNulls = values.count(_.isEmpty),
        // Internally, TimestampType is represented as the number of days from 1970-01-01
        max = Some(nonNullValues.map(DateTimeUtils.fromJavaTimestamp).max),
        min = Some(nonNullValues.map(DateTimeUtils.fromJavaTimestamp).min),
        ndv = Some(nonNullValues.distinct.length.toLong))
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for null columns") {
    val values = Seq(None, None)
    val data = values.map { i =>
      (i.map(_.toString), i.map(_.toString.toInt))
    }
    val df = data.toDF("c1", "c2")
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = f.dataType match {
        case StringType =>
          ColumnStats(
            dataType = f.dataType,
            numNulls = values.count(_.isEmpty),
            ndv = Some(0),
            maxColLen = None,
            avgColLen = None)
        case IntegerType =>
          ColumnStats(
            dataType = f.dataType,
            numNulls = values.count(_.isEmpty),
            max = None,
            min = None,
            ndv = Some(0))
      }
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for columns with different types") {
    val intSeq = Seq(1, 2)
    val doubleSeq = Seq(1.01d, 2.02d)
    val stringSeq = Seq("a", "bb")
    val binarySeq = Seq("a", "bb").map(_.getBytes)
    val booleanSeq = Seq(true, false)
    val dateSeq = Seq("1970-01-01", "1970-02-02").map(Date.valueOf)
    val timestampSeq = Seq("1970-01-01 00:00:00", "1970-01-01 00:00:05").map(Timestamp.valueOf)
    val longSeq = Seq(5L, 4L)

    val data = intSeq.indices.map { i =>
      (intSeq(i), doubleSeq(i), stringSeq(i), binarySeq(i), booleanSeq(i), dateSeq(i),
        timestampSeq(i), longSeq(i))
    }
    val df = data.toDF("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")
    val expectedColStatsSeq = df.schema.map { f =>
      val colStats = f.dataType match {
        case IntegerType =>
          ColumnStats(dataType = f.dataType, numNulls = 0, max = Some(intSeq.max),
            min = Some(intSeq.min), ndv = Some(intSeq.distinct.length.toLong))
        case DoubleType =>
          ColumnStats(dataType = f.dataType, numNulls = 0, max = Some(doubleSeq.max),
            min = Some(doubleSeq.min), ndv = Some(doubleSeq.distinct.length.toLong))
        case StringType =>
          ColumnStats(dataType = f.dataType, numNulls = 0,
            maxColLen = Some(stringSeq.map(_.length).max.toLong),
            avgColLen = Some(stringSeq.map(_.length).sum / stringSeq.length.toDouble),
            ndv = Some(stringSeq.distinct.length.toLong))
        case BinaryType =>
          ColumnStats(dataType = f.dataType, numNulls = 0,
            maxColLen = Some(binarySeq.map(_.length).max.toLong),
            avgColLen = Some(binarySeq.map(_.length).sum / binarySeq.length.toDouble))
        case BooleanType =>
          ColumnStats(dataType = f.dataType, numNulls = 0,
            numTrues = Some(booleanSeq.count(_.equals(true)).toLong),
            numFalses = Some(booleanSeq.count(_.equals(false)).toLong))
        case DateType =>
          ColumnStats(dataType = f.dataType, numNulls = 0,
            max = Some(dateSeq.map(DateTimeUtils.fromJavaDate).max),
            min = Some(dateSeq.map(DateTimeUtils.fromJavaDate).min),
            ndv = Some(dateSeq.distinct.length.toLong))
        case TimestampType =>
          ColumnStats(dataType = f.dataType, numNulls = 0,
            max = Some(timestampSeq.map(DateTimeUtils.fromJavaTimestamp).max),
            min = Some(timestampSeq.map(DateTimeUtils.fromJavaTimestamp).min),
            ndv = Some(timestampSeq.distinct.length.toLong))
        case LongType =>
          ColumnStats(dataType = f.dataType, numNulls = 0, max = Some(longSeq.max),
            min = Some(longSeq.min), ndv = Some(longSeq.distinct.length.toLong))
      }
      (f.name, colStats)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("update table-level stats while collecting column-level stats") {
    val table = "tbl"
    val tmpTable = "tmp"
    withTable(table, tmpTable) {
      val values = Seq(1)
      val df = values.toDF("c1")
      df.write.format("json").saveAsTable(tmpTable)

      sql(s"CREATE TABLE $table (c1 int) USING PARQUET")
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      val fetchedStats1 =
        checkTableStats(tableName = table, expectedRowCount = Some(values.length))

      // update table-level stats between analyze table and analyze column commands
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats2 =
        checkTableStats(tableName = table, expectedRowCount = Some(values.length * 2))

      val colStats = fetchedStats2.get.colStats("c1")
      checkColStats(colStats = colStats, expectedColStats = ColumnStats(
        dataType = IntegerType,
        numNulls = 0,
        max = Some(values.max),
        min = Some(values.min),
        ndv = Some(values.distinct.length.toLong)))
    }
  }

  test("analyze column stats independently") {
    val table = "tbl"
    withTable(table) {
      sql(s"CREATE TABLE $table (c1 int, c2 long) USING PARQUET")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats1 = checkTableStats(tableName = table, expectedRowCount = Some(0))
      assert(fetchedStats1.get.colStats.size == 1)
      val expected1 = ColumnStats(dataType = IntegerType, numNulls = 0, ndv = Some(0L))
      checkColStats(colStats = fetchedStats1.get.colStats("c1"), expectedColStats = expected1)

      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c2")
      val fetchedStats2 = checkTableStats(tableName = table, expectedRowCount = Some(0))
      // column c1 is kept in the stats
      assert(fetchedStats2.get.colStats.size == 2)
      checkColStats(colStats = fetchedStats2.get.colStats("c1"), expectedColStats = expected1)
      val expected2 = ColumnStats(dataType = LongType, numNulls = 0, ndv = Some(0L))
      checkColStats(colStats = fetchedStats2.get.colStats("c2"), expectedColStats = expected2)
    }
  }
}
