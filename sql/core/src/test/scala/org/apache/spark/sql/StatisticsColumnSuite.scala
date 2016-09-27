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

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.test.SQLTestData.ArrayData
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

  test("analyzing columns of non-atomic types is not supported") {
    val tableName = "tbl"
    withTable(tableName) {
      Seq(ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3)))).toDF().write.saveAsTable(tableName)
      val err = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS data")
      }
      assert(err.message.contains("Analyzing columns is not supported"))
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
        val tableIdent = TableIdentifier(table, Some("default"))
        val relation = spark.sessionState.catalog.lookupRelation(tableIdent)
        val columnStats =
          AnalyzeColumnCommand(tableIdent, columnsToAnalyze).computeColStats(spark, relation)._2
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
      val colStat = ColumnStat(InternalRow(
        values.count(_.isEmpty).toLong,
        nonNullValues.max,
        nonNullValues.min,
        nonNullValues.distinct.length.toLong))
      (f, colStat)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for fractional type columns") {
    val values: Seq[Option[Decimal]] = (0 to 5).map { i =>
      if (i == 0) None else Some(Decimal(i + i * 0.01))
    }
    val data = values.map { i =>
      (i.map(_.toFloat), i.map(_.toDouble), i)
    }

    val df = data.toDF("c1", "c2", "c3")
    val nonNullValues = getNonNullValues[Decimal](values)
    val numNulls = values.count(_.isEmpty).toLong
    val ndv = nonNullValues.distinct.length.toLong
    val expectedColStatsSeq = df.schema.map { f =>
      val colStat = f.dataType match {
        case floatType: FloatType =>
          ColumnStat(InternalRow(numNulls, nonNullValues.max.toFloat, nonNullValues.min.toFloat,
            ndv))
        case doubleType: DoubleType =>
          ColumnStat(InternalRow(numNulls, nonNullValues.max.toDouble, nonNullValues.min.toDouble,
            ndv))
        case decimalType: DecimalType =>
          ColumnStat(InternalRow(numNulls, nonNullValues.max, nonNullValues.min, ndv))
      }
      (f, colStat)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for string column") {
    val values = Seq(None, Some("a"), Some("bbbb"), Some("cccc"), Some(""))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[String](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStat = ColumnStat(InternalRow(
        values.count(_.isEmpty).toLong,
        nonNullValues.map(_.length).sum / nonNullValues.length.toDouble,
        nonNullValues.map(_.length).max.toLong,
        nonNullValues.distinct.length.toLong))
      (f, colStat)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for binary column") {
    val values = Seq(None, Some("a"), Some("bbbb"), Some("cccc"), Some("")).map(_.map(_.getBytes))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Array[Byte]](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStat = ColumnStat(InternalRow(
        values.count(_.isEmpty).toLong,
        nonNullValues.map(_.length).sum / nonNullValues.length.toDouble,
        nonNullValues.map(_.length).max.toLong))
      (f, colStat)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for boolean column") {
    val values = Seq(None, Some(true), Some(false), Some(true))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Boolean](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStat = ColumnStat(InternalRow(
        values.count(_.isEmpty).toLong,
        nonNullValues.count(_.equals(true)).toLong,
        nonNullValues.count(_.equals(false)).toLong))
      (f, colStat)
    }
    checkColStats(df, expectedColStatsSeq)
  }

  test("column-level statistics for date column") {
    val values = Seq(None, Some("1970-01-01"), Some("1970-02-02")).map(_.map(Date.valueOf))
    val df = values.toDF("c1")
    val nonNullValues = getNonNullValues[Date](values)
    val expectedColStatsSeq = df.schema.map { f =>
      val colStat = ColumnStat(InternalRow(
        values.count(_.isEmpty).toLong,
        // Internally, DateType is represented as the number of days from 1970-01-01.
        nonNullValues.map(DateTimeUtils.fromJavaDate).max,
        nonNullValues.map(DateTimeUtils.fromJavaDate).min,
        nonNullValues.distinct.length.toLong))
      (f, colStat)
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
      val colStat = ColumnStat(InternalRow(
        values.count(_.isEmpty).toLong,
        // Internally, TimestampType is represented as the number of days from 1970-01-01
        nonNullValues.map(DateTimeUtils.fromJavaTimestamp).max,
        nonNullValues.map(DateTimeUtils.fromJavaTimestamp).min,
        nonNullValues.distinct.length.toLong))
      (f, colStat)
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
      (f, ColumnStat(InternalRow(values.count(_.isEmpty).toLong, null, null, 0L)))
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
      val colStat = f.dataType match {
        case IntegerType =>
          ColumnStat(InternalRow(0L, intSeq.max, intSeq.min, intSeq.distinct.length.toLong))
        case DoubleType =>
          ColumnStat(InternalRow(0L, doubleSeq.max, doubleSeq.min,
              doubleSeq.distinct.length.toLong))
        case StringType =>
          ColumnStat(InternalRow(0L, stringSeq.map(_.length).sum / stringSeq.length.toDouble,
                stringSeq.map(_.length).max.toLong, stringSeq.distinct.length.toLong))
        case BinaryType =>
          ColumnStat(InternalRow(0L, binarySeq.map(_.length).sum / binarySeq.length.toDouble,
                binarySeq.map(_.length).max.toLong))
        case BooleanType =>
          ColumnStat(InternalRow(0L, booleanSeq.count(_.equals(true)).toLong,
              booleanSeq.count(_.equals(false)).toLong))
        case DateType =>
          ColumnStat(InternalRow(0L, dateSeq.map(DateTimeUtils.fromJavaDate).max,
                dateSeq.map(DateTimeUtils.fromJavaDate).min, dateSeq.distinct.length.toLong))
        case TimestampType =>
          ColumnStat(InternalRow(0L, timestampSeq.map(DateTimeUtils.fromJavaTimestamp).max,
                timestampSeq.map(DateTimeUtils.fromJavaTimestamp).min,
                timestampSeq.distinct.length.toLong))
        case LongType =>
          ColumnStat(InternalRow(0L, longSeq.max, longSeq.min, longSeq.distinct.length.toLong))
      }
      (f, colStat)
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
      checkTableStats(tableName = table, expectedRowCount = Some(values.length))

      // update table-level stats between analyze table and analyze column commands
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats =
        checkTableStats(tableName = table, expectedRowCount = Some(values.length * 2))

      val colStat = fetchedStats.get.colStats("c1")
      checkColStat(dataType = IntegerType, colStat = colStat, expectedColStat =
        ColumnStat(InternalRow.fromSeq(
          Seq(0L, values.max, values.min, values.distinct.length.toLong))))
    }
  }

  test("analyze column stats independently") {
    val table = "tbl"
    withTable(table) {
      sql(s"CREATE TABLE $table (c1 int, c2 long) USING PARQUET")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats1 = checkTableStats(tableName = table, expectedRowCount = Some(0))
      assert(fetchedStats1.get.colStats.size == 1)
      val expected1 = ColumnStat(InternalRow(0L, null, null, 0L))
      checkColStat(dataType = IntegerType, colStat = fetchedStats1.get.colStats("c1"),
        expectedColStat = expected1)

      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c2")
      val fetchedStats2 = checkTableStats(tableName = table, expectedRowCount = Some(0))
      // column c1 is kept in the stats
      assert(fetchedStats2.get.colStats.size == 2)
      checkColStat(dataType = IntegerType, colStat = fetchedStats2.get.colStats("c1"),
        expectedColStat = expected1)
      val expected2 = ColumnStat(InternalRow(0L, null, null, 0L))
      checkColStat(dataType = LongType, colStat = fetchedStats2.get.colStats("c2"),
        expectedColStat = expected2)
    }
  }
}
