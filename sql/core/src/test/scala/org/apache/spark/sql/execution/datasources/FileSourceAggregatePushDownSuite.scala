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

package org.apache.spark.sql.execution.datasources

import java.sql.{Date, Timestamp}
import java.time.LocalTime

import scala.reflect.ClassTag

import org.apache.orc.OrcConf

import org.apache.spark.{SparkConf, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, Row}
import org.apache.spark.sql.catalyst.optimizer.CollapseGroupedSumOfCount
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.datasources.orc.OrcTest
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, TimeType}
import org.apache.spark.tags.SlowSQLTest

/**
 * A test suite that tests aggregate push down for Parquet and ORC.
 */
trait FileSourceAggregatePushDownSuite
  extends FileBasedDataSourceTest
  with SharedSparkSession
  with ExplainSuiteHelper {

  import testImplicits._

  protected def format: String
  // The SQL config key for enabling aggregate push down.
  protected val aggPushDownEnabledKey: String

  test("nested column: Max(top level column) not push down") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withSQLConf(aggPushDownEnabledKey -> "true") {
      withDataSourceTable(data, "t") {
        val max = sql("SELECT Max(_1) FROM t")
        checkPushedInfo(max, "PushedAggregation: []")
      }
    }
  }

  test("nested column: Count(top level column) push down") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withSQLConf(aggPushDownEnabledKey -> "true") {
      withDataSourceTable(data, "t") {
        val count = sql("SELECT Count(_1) FROM t")
        checkPushedInfo(count, "PushedAggregation: [COUNT(_1)]")
        checkAnswer(count, Seq(Row(10)))
      }
    }
  }

  test("nested column: Max(nested sub-field) not push down") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withSQLConf(aggPushDownEnabledKey-> "true") {
      withDataSourceTable(data, "t") {
        val max = sql("SELECT Max(_1._2[0]) FROM t")
        checkPushedInfo(max, "PushedAggregation: []")
      }
    }
  }

  test("nested column: Count(nested sub-field) not push down") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withSQLConf(aggPushDownEnabledKey -> "true") {
      withDataSourceTable(data, "t") {
        val count = sql("SELECT Count(_1._2[0]) FROM t")
        checkPushedInfo(count, "PushedAggregation: []")
        checkAnswer(count, Seq(Row(10)))
      }
    }
  }

  test("Max(partition column): not push down") {
    withTempPath { dir =>
      spark.range(10).selectExpr("id", "id % 3 as p")
        .write.partitionBy("p").format(format).save(dir.getCanonicalPath)
      withTempView("tmp") {
        spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
        withSQLConf(aggPushDownEnabledKey -> "true") {
          val max = sql("SELECT Max(p) FROM tmp")
          checkPushedInfo(max, "PushedAggregation: []")
          checkAnswer(max, Seq(Row(2)))
        }
      }
    }
  }

  test("Count(partition column): push down") {
    withTempPath { dir =>
      spark.range(10).selectExpr("if(id % 2 = 0, null, id) AS n", "id % 3 as p")
        .write.partitionBy("p").format(format).save(dir.getCanonicalPath)
      withTempView("tmp") {
        spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
        val enableVectorizedReader = Seq("false", "true")
        for (testVectorizedReader <- enableVectorizedReader) {
          withSQLConf(aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> testVectorizedReader) {
            val count = sql("SELECT COUNT(p) FROM tmp")
            checkPushedInfo(count, "PushedAggregation: [COUNT(p)]")
            checkAnswer(count, Seq(Row(10)))
          }
        }
      }
    }
  }

  test("filter alias over aggregate") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 6))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val selectAgg = sql("SELECT min(_1) + max(_1) as res FROM t having res > 1")
        checkPushedInfo(selectAgg, "PushedAggregation: [MIN(_1), MAX(_1)]")
        checkAnswer(selectAgg, Seq(Row(7)))
      }
    }
  }

  test("alias over aggregate") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 6))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val selectAgg = sql("SELECT min(_1) + 1 as minPlus1, min(_1) + 2 as minPlus2 FROM t")
        checkPushedInfo(selectAgg, "PushedAggregation: [MIN(_1)]")
        checkAnswer(selectAgg, Seq(Row(-1, 0)))
      }
    }
  }

  test("aggregate over alias push down") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 6))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val df = spark.table("t")
        val query = df.select($"_1".as("col1")).agg(min($"col1"))
        checkPushedInfo(query, "PushedAggregation: [MIN(_1)]")
        checkAnswer(query, Seq(Row(-2)))
      }
    }
  }

  test("query with group by not push down") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 7))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        // aggregate not pushed down if there is group by
        val selectAgg = sql("SELECT min(_1) FROM t GROUP BY _3 ")
        checkPushedInfo(selectAgg, "PushedAggregation: []")
        checkAnswer(selectAgg, Seq(Row(-2), Row(0), Row(2), Row(3)))
      }
    }
  }

  test("aggregate with data filter cannot be pushed down") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 7))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        // aggregate not pushed down if there is filter
        val selectAgg = sql("SELECT min(_3) FROM t WHERE _1 > 0")
        checkPushedInfo(selectAgg, "PushedAggregation: []")
        checkAnswer(selectAgg, Seq(Row(2)))
      }
    }
  }

  test("aggregate with partition filter can be pushed down") {
    withTempPath { dir =>
      spark.range(10).selectExpr("id", "id % 3 as p")
        .write.partitionBy("p").format(format).save(dir.getCanonicalPath)
      withTempView("tmp") {
        spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
        Seq("false", "true").foreach { enableVectorizedReader =>
          withSQLConf(aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> enableVectorizedReader) {
            val max = sql("SELECT max(id), min(id), count(id) FROM tmp WHERE p = 0")
            checkPushedInfo(max, "PushedAggregation: [MAX(id), MIN(id), COUNT(id)]")
            checkAnswer(max, Seq(Row(9, 0, 4)))
          }
        }
      }
    }
  }

  test("aggregate with partition group by can be pushed down") {
    withTempPath { dir =>
      spark.range(10).selectExpr("id", "id % 3 as P")
        .write.partitionBy("p").format(format).save(dir.getCanonicalPath)
      withTempView("tmp") {
        spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp");
        val query = "SELECT count(*), count(id), p, max(id), p, count(p), max(id)," +
          "  min(id), p FROM tmp group by p"
        var expected = Array.empty[Row]
        withSQLConf(aggPushDownEnabledKey -> "false") {
            expected = sql(query).collect()
        }
        Seq("false", "true").foreach { enableVectorizedReader =>
          withSQLConf(aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> enableVectorizedReader) {
            val df = sql(query)
            val expected_plan_fragment =
              "PushedAggregation: [COUNT(*), COUNT(id), MAX(id), COUNT(p), MIN(id)], " +
                "PushedFilters: [], PushedGroupBy: [p]"
            checkPushedInfo(df, expected_plan_fragment)
            checkAnswer(df, expected)
          }
        }
      }
    }
  }

  test("aggregate with multi partition group by columns can be pushed down") {
    withTempPath { dir =>
      Seq((10, 1, 2, 5, 6), (2, 1, 2, 5, 6), (3, 2, 1, 4, 8), (4, 2, 1, 4, 9),
        (5, 2, 1, 5, 8), (6, 2, 1, 4, 8), (1, 1, 2, 5, 6), (4, 1, 2, 5, 6),
        (3, 2, 2, 9, 10), (-4, 2, 2, 9, 10), (6, 2, 2, 9, 10))
        .toDF("value", "p1", "p2", "p3", "p4")
        .write
        .partitionBy("p2", "p1", "p4", "p3")
        .format(format)
        .save(dir.getCanonicalPath)

      withTempView("tmp") {
        spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
        val query = "SELECT count(*), count(value), max(value), min(value)," +
          " p4, p2, p3, p1 FROM tmp GROUP BY p1, p2, p3, p4"
        var expected = Array.empty[Row]
        withSQLConf(aggPushDownEnabledKey -> "false") {
          expected = sql(query).collect()
        }
        Seq("false", "true").foreach { enableVectorizedReader =>
          withSQLConf(aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> enableVectorizedReader) {
            val df = sql(query)
            val expected_plan_fragment =
              "PushedAggregation: [COUNT(*), COUNT(value), MAX(value), MIN(value)]," +
                " PushedFilters: [], PushedGroupBy: [p1, p2, p3, p4]"
            checkPushedInfo(df, expected_plan_fragment)
            checkAnswer(df, expected)
          }
        }
      }
    }
  }

  test("SPARK-57043: preserve pushed count before collapsing its grouped rollup") {
    withTempPath { dir =>
      Seq((10, 1, 2), (2, 1, 2), (3, 2, 1), (4, 2, 1), (5, 2, 2))
        .toDF("value", "p1", "p2")
        .write
        .partitionBy("p1", "p2")
        .format(format)
        .save(dir.getCanonicalPath)

      withTempView("tmp") {
        spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
        val query =
          """
            |SELECT p1, SUM(cnt)
            |FROM (SELECT p1, p2, COUNT(*) AS cnt FROM tmp GROUP BY p1, p2)
            |GROUP BY p1
            |""".stripMargin
        var expected = Array.empty[Row]
        withSQLConf(aggPushDownEnabledKey -> "false") {
          expected = sql(query).collect()
        }
        withSQLConf(aggPushDownEnabledKey -> "true") {
          val df = sql(query)
          checkPushedInfo(
            df,
            "PushedAggregation: [COUNT(*)], PushedFilters: [], PushedGroupBy: [p1, p2]")
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("SPARK-57043: collapse grouped count rollup after rejected scan push down") {
    val data = Seq((1, 1), (1, 1), (1, 2), (2, 1), (2, 2))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val df = sql(
          """
            |SELECT _1, SUM(cnt)
            |FROM (SELECT _1, _2, COUNT(*) AS cnt FROM t GROUP BY _1, _2)
            |GROUP BY _1
            |""".stripMargin)
        checkPushedInfo(df, "PushedAggregation: []")
        assert(df.queryExecution.optimizedPlan.collect { case _: Aggregate => true }.size == 1)
        checkAnswer(df, Seq(Row(1, 3L), Row(2, 2L)))
      }
      withSQLConf(
          aggPushDownEnabledKey -> "true",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> CollapseGroupedSumOfCount.ruleName) {
        val df = sql(
          """
            |SELECT _1, SUM(cnt)
            |FROM (SELECT _1, _2, COUNT(*) AS cnt FROM t GROUP BY _1, _2)
            |GROUP BY _1
            |""".stripMargin)
        assert(df.queryExecution.optimizedPlan.collect { case _: Aggregate => true }.size == 2)
        checkAnswer(df, Seq(Row(1, 3L), Row(2, 2L)))
      }
    }
  }

  test("push down only if all the aggregates can be pushed down") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 7))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        // not push down since sum can't be pushed down
        val selectAgg = sql("SELECT min(_1), sum(_3) FROM t")
        checkPushedInfo(selectAgg, "PushedAggregation: []")
        checkAnswer(selectAgg, Seq(Row(-2, 41)))
      }
    }
  }

  test("aggregate push down - MIN/MAX/COUNT") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 6))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val selectAgg = sql("SELECT min(_3), min(_3), max(_3), min(_1), max(_1), max(_1)," +
          " count(*), count(_1), count(_2), count(_3) FROM t")
        val expected_plan_fragment =
          "PushedAggregation: [MIN(_3), " +
            "MAX(_3), " +
            "MIN(_1), " +
            "MAX(_1), " +
            "COUNT(*), " +
            "COUNT(_1), " +
            "COUNT(_2), " +
            "COUNT(_3)]"
        checkPushedInfo(selectAgg, expected_plan_fragment)
        checkAnswer(selectAgg, Seq(Row(2, 2, 19, -2, 9, 9, 6, 6, 4, 6)))
      }
    }
  }

  test("aggregate not push down - MIN/MAX/COUNT with CASE WHEN") {
    val data = Seq((-2, "abc", 2), (3, "def", 4), (6, "ghi", 2), (0, null, 19),
      (9, "mno", 7), (2, null, 6))
    withDataSourceTable(data, "t") {
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val selectAgg = sql(
          """
            |SELECT
            |  min(CASE WHEN _1 < 0 THEN 0 ELSE _1 END),
            |  min(CASE WHEN _3 > 5 THEN 1 ELSE 0 END),
            |  max(CASE WHEN _1 < 0 THEN 0 ELSE _1 END),
            |  max(CASE WHEN NOT(_3 > 5) THEN 1 ELSE 0 END),
            |  count(CASE WHEN _1 < 0 AND _2 IS NOT NULL THEN 0 ELSE _1 END),
            |  count(CASE WHEN _3 != 5 OR _2 IS NULL THEN 1 ELSE 0 END)
            |FROM t
          """.stripMargin)
        checkPushedInfo(selectAgg, "PushedAggregation: []")
        checkAnswer(selectAgg, Seq(Row(0, 0, 9, 1, 6, 6)))
      }
    }
  }

  private def testPushDownForAllDataTypes(
      inputRows: Seq[Row],
      expectedMinWithAllTypes: Seq[Row],
      expectedMinWithOutTSAndBinary: Seq[Row],
      expectedMaxWithAllTypes: Seq[Row],
      expectedMaxWithOutTSAndBinary: Seq[Row],
      expectedCount: Seq[Row]): Unit = {

    val schema = StructType(List(StructField("StringCol", StringType, true),
      StructField("BooleanCol", BooleanType, false),
      StructField("ByteCol", ByteType, false),
      StructField("BinaryCol", BinaryType, false),
      StructField("ShortCol", ShortType, false),
      StructField("IntegerCol", IntegerType, true),
      StructField("LongCol", LongType, false),
      StructField("FloatCol", FloatType, false),
      StructField("DoubleCol", DoubleType, false),
      StructField("DecimalCol", DecimalType(25, 5), true),
      StructField("DateCol", DateType, false),
      StructField("TimestampCol", TimestampType, false)).toArray)

    val rdd = sparkContext.parallelize(inputRows)
    withTempPath { file =>
      spark.createDataFrame(rdd, schema).write.format(format).save(file.getCanonicalPath)
      withTempView("test") {
        spark.read.format(format).load(file.getCanonicalPath).createOrReplaceTempView("test")
        Seq("false", "true").foreach { enableVectorizedReader =>
          withSQLConf(aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> enableVectorizedReader,
            SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {

            val testMinWithAllTypes = sql("SELECT min(StringCol), min(BooleanCol), min(ByteCol), " +
              "min(BinaryCol), min(ShortCol), min(IntegerCol), min(LongCol), min(FloatCol), " +
              "min(DoubleCol), min(DecimalCol), min(DateCol), min(TimestampCol) FROM test")

            // INT96 (Timestamp) sort order is undefined, parquet doesn't return stats for this type
            // so aggregates are not pushed down
            // In addition, Parquet Binary min/max could be truncated, so we disable aggregate
            // push down for Parquet Binary (could be Spark StringType, BinaryType or DecimalType).
            // Also do not push down for ORC with same reason.
            checkPushedInfo(testMinWithAllTypes, "PushedAggregation: []")
            checkAnswer(testMinWithAllTypes, expectedMinWithAllTypes)

            val testMinWithOutTSAndBinary = sql("SELECT min(BooleanCol), min(ByteCol), " +
              "min(ShortCol), min(IntegerCol), min(LongCol), min(FloatCol), " +
              "min(DoubleCol), min(DateCol) FROM test")

            var expected_plan_fragment =
              "PushedAggregation: [MIN(BooleanCol), " +
                "MIN(ByteCol), " +
                "MIN(ShortCol), " +
                "MIN(IntegerCol), " +
                "MIN(LongCol), " +
                "MIN(FloatCol), " +
                "MIN(DoubleCol), " +
                "MIN(DateCol)]"
            checkPushedInfo(testMinWithOutTSAndBinary, expected_plan_fragment)
            checkAnswer(testMinWithOutTSAndBinary, expectedMinWithOutTSAndBinary)

            val testMaxWithAllTypes = sql("SELECT max(StringCol), max(BooleanCol), " +
              "max(ByteCol), max(BinaryCol), max(ShortCol), max(IntegerCol), max(LongCol), " +
              "max(FloatCol), max(DoubleCol), max(DecimalCol), max(DateCol), max(TimestampCol) " +
              "FROM test")

            // INT96 (Timestamp) sort order is undefined, parquet doesn't return stats for this type
            // so aggregates are not pushed down
            // In addition, Parquet Binary min/max could be truncated, so we disable aggregate
            // push down for Parquet Binary (could be Spark StringType, BinaryType or DecimalType).
            // Also do not push down for ORC with same reason.
            checkPushedInfo(testMaxWithAllTypes, "PushedAggregation: []")
            checkAnswer(testMaxWithAllTypes, expectedMaxWithAllTypes)

            val testMaxWithoutTSAndBinary = sql("SELECT max(BooleanCol), max(ByteCol), " +
              "max(ShortCol), max(IntegerCol), max(LongCol), max(FloatCol), " +
              "max(DoubleCol), max(DateCol) FROM test")

            expected_plan_fragment =
              "PushedAggregation: [MAX(BooleanCol), " +
                "MAX(ByteCol), " +
                "MAX(ShortCol), " +
                "MAX(IntegerCol), " +
                "MAX(LongCol), " +
                "MAX(FloatCol), " +
                "MAX(DoubleCol), " +
                "MAX(DateCol)]"
            checkPushedInfo(testMaxWithoutTSAndBinary, expected_plan_fragment)
            checkAnswer(testMaxWithoutTSAndBinary, expectedMaxWithOutTSAndBinary)

            val testCount = sql("SELECT count(StringCol), count(BooleanCol)," +
              " count(ByteCol), count(BinaryCol), count(ShortCol), count(IntegerCol)," +
              " count(LongCol), count(FloatCol), count(DoubleCol)," +
              " count(DecimalCol), count(DateCol), count(TimestampCol) FROM test")
            expected_plan_fragment =
              "PushedAggregation: [" +
                "COUNT(StringCol), " +
                "COUNT(BooleanCol), " +
                "COUNT(ByteCol), " +
                "COUNT(BinaryCol), " +
                "COUNT(ShortCol), " +
                "COUNT(IntegerCol), " +
                "COUNT(LongCol), " +
                "COUNT(FloatCol), " +
                "COUNT(DoubleCol), " +
                "COUNT(DecimalCol), " +
                "COUNT(DateCol), " +
                "COUNT(TimestampCol)]"
            checkPushedInfo(testCount, expected_plan_fragment)
            checkAnswer(testCount, expectedCount)
          }
        }
      }
    }
  }

  test("aggregate push down - different data types") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
    }

    implicit class StringToTs(s: String) {
      def ts: Timestamp = Timestamp.valueOf(s)
    }

    val rows =
      Seq(
        Row(
          "a string",
          true,
          10.toByte,
          "Spark SQL".getBytes,
          12.toShort,
          3,
          Long.MaxValue,
          0.15.toFloat,
          0.75D,
          Decimal("12.345678"),
          ("2021-01-01").date,
          ("2015-01-01 23:50:59.123").ts),
        Row(
          "test string",
          false,
          1.toByte,
          "Parquet".getBytes,
          2.toShort,
          null,
          Long.MinValue,
          0.25.toFloat,
          0.85D,
          Decimal("1.2345678"),
          ("2015-01-01").date,
          ("2021-01-01 23:50:59.123").ts),
        Row(
          null,
          true,
          10000.toByte,
          "Spark ML".getBytes,
          222.toShort,
          113,
          11111111L,
          0.25.toFloat,
          0.75D,
          Decimal("12345.678"),
          ("2004-06-19").date,
          ("1999-08-26 10:43:59.123").ts)
      )

    testPushDownForAllDataTypes(
      rows,
      Seq(Row("a string", false, 1.toByte,
        "Parquet".getBytes, 2.toShort, 3, -9223372036854775808L, 0.15.toFloat, 0.75D,
        1.23457, ("2004-06-19").date, ("1999-08-26 10:43:59.123").ts)),
      Seq(Row(false, 1.toByte,
        2.toShort, 3, -9223372036854775808L, 0.15.toFloat, 0.75D, ("2004-06-19").date)),
      Seq(Row("test string", true, 16.toByte,
        "Spark SQL".getBytes, 222.toShort, 113, 9223372036854775807L, 0.25.toFloat, 0.85D,
        12345.678, ("2021-01-01").date, ("2021-01-01 23:50:59.123").ts)),
      Seq(Row(true, 16.toByte,
        222.toShort, 113, 9223372036854775807L, 0.25.toFloat, 0.85D, ("2021-01-01").date)),
      Seq(Row(2, 3, 3, 3, 3, 2, 3, 3, 3, 3, 3, 3))
    )

    // Test for 0 row (empty file)
    val nullRow = Row.fromSeq((1 to 12).map(_ => null))
    val nullRowWithOutTSAndBinary = Row.fromSeq((1 to 8).map(_ => null))
    val zeroCount = Row.fromSeq((1 to 12).map(_ => 0))
    testPushDownForAllDataTypes(Seq.empty, Seq(nullRow), Seq(nullRowWithOutTSAndBinary),
      Seq(nullRow), Seq(nullRowWithOutTSAndBinary), Seq(zeroCount))
  }

  private def testTimeAggPushDown(
      precision: Int,
      times: Seq[LocalTime],
      expectedMin: LocalTime,
      expectedMax: LocalTime): Unit = {
    val schema = StructType(Seq(StructField("TimeCol", TimeType(precision))))
    // One null row in addition to the non-null `times`, so COUNT(TimeCol) excludes it but
    // COUNT(*) includes it.
    val rows = times.map(Row(_)) :+ Row(null)
    val rdd = sparkContext.parallelize(rows)
    withTempPath { file =>
      spark.createDataFrame(rdd, schema).write.format(format).save(file.getCanonicalPath)
      withTempView("time_test") {
        spark.read.format(format).load(file.getCanonicalPath)
          .createOrReplaceTempView("time_test")
        Seq("false", "true").foreach { enableVectorizedReader =>
          withSQLConf(aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> enableVectorizedReader) {
            val df = sql(
              "SELECT min(TimeCol), max(TimeCol), count(TimeCol), count(*) FROM time_test")
            checkPushedInfo(df,
              "PushedAggregation: [MIN(TimeCol), MAX(TimeCol), COUNT(TimeCol), COUNT(*)]")
            checkAnswer(df,
              Seq(Row(expectedMin, expectedMax, times.length, times.length + 1)))
          }
        }
      }
    }
  }

  test("SPARK-57568: aggregate push down - TIME of different precisions") {
    // Parquet stores TIME with precision 0..6 as INT64 TIME(MICROS) and precision 7..9 as
    // INT64 TIME(NANOS); ORC stores TIME as the raw nanos-of-day LONG. To keep a single shared
    // expectation valid across both engines, each value is chosen to be exactly representable at
    // its column's precision, so no engine-specific truncation can diverge.
    // precision 0 (seconds)
    testTimeAggPushDown(0,
      Seq(LocalTime.of(1, 2, 3), LocalTime.of(23, 59, 59), LocalTime.of(10, 30, 0)),
      LocalTime.of(1, 2, 3), LocalTime.of(23, 59, 59))
    // precision 6 (microseconds)
    testTimeAggPushDown(6,
      Seq(LocalTime.of(1, 2, 3, 123456000), LocalTime.of(23, 59, 59, 999999000),
        LocalTime.of(10, 30, 0, 500000)),
      LocalTime.of(1, 2, 3, 123456000), LocalTime.of(23, 59, 59, 999999000))
    // precision 7 (hundreds of nanoseconds)
    testTimeAggPushDown(7,
      Seq(LocalTime.of(1, 2, 3, 123456700), LocalTime.of(23, 59, 59, 999999900),
        LocalTime.of(10, 30, 0, 100)),
      LocalTime.of(1, 2, 3, 123456700), LocalTime.of(23, 59, 59, 999999900))
    // precision 9 (nanoseconds)
    testTimeAggPushDown(9,
      Seq(LocalTime.of(1, 2, 3, 123456789), LocalTime.of(23, 59, 59, 999999999),
        LocalTime.of(10, 30, 0, 1)),
      LocalTime.of(1, 2, 3, 123456789), LocalTime.of(23, 59, 59, 999999999))
  }

  test("SPARK-57568: aggregate push down - TIME over an empty file") {
    // Aggregating a TIME column over zero rows: MIN/MAX return NULL and COUNT returns 0 on every
    // engine. This pins the no-data path, which the precision test above (always >= 1 row) does
    // not reach. An all-null but non-empty file is intentionally not asserted here: that is
    // pre-existing, type-agnostic behavior shared by all push-down types (Parquet rejects MIN/MAX
    // push-down on an all-null block while ORC returns NULL), unchanged by this PR.
    Seq(6, 9).foreach { precision =>
      val schema = StructType(Seq(StructField("TimeCol", TimeType(precision))))
      val rdd = sparkContext.parallelize(Seq.empty[Row])
      withTempPath { file =>
        spark.createDataFrame(rdd, schema).write.format(format).save(file.getCanonicalPath)
        withTempView("time_empty") {
          spark.read.format(format).load(file.getCanonicalPath)
            .createOrReplaceTempView("time_empty")
          Seq("false", "true").foreach { enableVectorizedReader =>
            withSQLConf(aggPushDownEnabledKey -> "true",
              vectorizedReaderEnabledKey -> enableVectorizedReader) {
              val df = sql(
                "SELECT min(TimeCol), max(TimeCol), count(TimeCol), count(*) FROM time_empty")
              checkPushedInfo(df,
                "PushedAggregation: [MIN(TimeCol), MAX(TimeCol), COUNT(TimeCol), COUNT(*)]")
              checkAnswer(df, Seq(Row(null, null, 0, 0)))
            }
          }
        }
      }
    }
  }

  test("SPARK-57568: aggregate not push down - TIME with filter or expression") {
    val schema = StructType(Seq(StructField("TimeCol", TimeType(6))))
    val rows = Seq(
      Row(LocalTime.of(1, 2, 3)),
      Row(LocalTime.of(23, 59, 59)),
      Row(LocalTime.of(10, 30, 0)))
    val rdd = sparkContext.parallelize(rows)
    withTempPath { file =>
      spark.createDataFrame(rdd, schema).write.format(format).save(file.getCanonicalPath)
      withTempView("time_neg_test") {
        spark.read.format(format).load(file.getCanonicalPath)
          .createOrReplaceTempView("time_neg_test")
        withSQLConf(aggPushDownEnabledKey -> "true") {
          // A data filter on a non-partition column prevents push down.
          val withFilter =
            sql("SELECT min(TimeCol) FROM time_neg_test WHERE TimeCol > TIME'05:00:00'")
          checkPushedInfo(withFilter, "PushedAggregation: []")
          checkAnswer(withFilter, Seq(Row(LocalTime.of(10, 30, 0))))

          // Aggregating over an expression (not a plain column) prevents push down. The two CASE
          // branches differ so the optimizer cannot fold the expression back into a column.
          val withExpr = sql(
            "SELECT max(CASE WHEN TimeCol > TIME'05:00:00' THEN TimeCol ELSE TIME'00:00:00' END) " +
              "FROM time_neg_test")
          checkPushedInfo(withExpr, "PushedAggregation: []")
          checkAnswer(withExpr, Seq(Row(LocalTime.of(23, 59, 59))))

          // Aggregate push down disabled by config.
          withSQLConf(aggPushDownEnabledKey -> "false") {
            val disabled =
              sql("SELECT min(TimeCol), max(TimeCol), count(TimeCol) FROM time_neg_test")
            checkPushedInfo(disabled, "PushedAggregation: []")
            checkAnswer(disabled,
              Seq(Row(LocalTime.of(1, 2, 3), LocalTime.of(23, 59, 59), 3)))
          }
        }
      }
    }
  }

  test("column name case sensitivity") {
    Seq("false", "true").foreach { enableVectorizedReader =>
      withSQLConf(aggPushDownEnabledKey -> "true",
        vectorizedReaderEnabledKey -> enableVectorizedReader) {
        withTempPath { dir =>
          spark.range(10).selectExpr("id", "id % 3 as p")
            .write.partitionBy("p").format(format).save(dir.getCanonicalPath)
          withTempView("tmp") {
            spark.read.format(format).load(dir.getCanonicalPath).createOrReplaceTempView("tmp")
            val selectAgg = sql("SELECT max(iD), min(Id) FROM tmp")
            checkPushedInfo(selectAgg,
              "PushedAggregation: [MAX(id), MIN(id)]")
            checkAnswer(selectAgg, Seq(Row(9, 0)))
          }
        }
      }
    }
  }

  protected def checkPushedInfo(df: DataFrame, expectedPlanFragment: String): Unit = {
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        checkKeywordsExistsInExplain(df, expectedPlanFragment)
    }
  }
}

abstract class ParquetAggregatePushDownSuite
  extends FileSourceAggregatePushDownSuite with ParquetTest {

  override def format: String = "parquet"
  override protected val aggPushDownEnabledKey: String =
    SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key
}

@SlowSQLTest
class ParquetV1AggregatePushDownSuite extends ParquetAggregatePushDownSuite {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
}

@SlowSQLTest
class ParquetV2AggregatePushDownSuite extends ParquetAggregatePushDownSuite {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  // Aggregate push-down only happens with the DSv2 file source, so this test lives in the V2
  // suite. It writes a Parquet file with column statistics disabled, which leaves both the
  // min/max values and the number of nulls unavailable for push-down.
  test("SPARK-57746: aggregate push-down fails when Parquet statistics are missing") {
    Seq("false", "true").foreach { enableVectorizedReader =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).selectExpr("CAST(id AS INT) AS i").coalesce(1)
          .write.option("parquet.column.statistics.enabled", "false").parquet(path)
        withTempView("t") {
          spark.read.parquet(path).createOrReplaceTempView("t")
          withSQLConf(
            aggPushDownEnabledKey -> "true",
            vectorizedReaderEnabledKey -> enableVectorizedReader) {
            checkErrorMatchPVals(
              exception = interceptAggPushDownError("SELECT min(i) FROM t"),
              condition = "PARQUET_AGGREGATE_PUSH_DOWN_UNSUPPORTED.NO_MIN_MAX",
              parameters = Map("filePath" -> ".*", "config" -> aggPushDownEnabledKey))
            checkErrorMatchPVals(
              exception = interceptAggPushDownError("SELECT count(i) FROM t"),
              condition = "PARQUET_AGGREGATE_PUSH_DOWN_UNSUPPORTED.NO_NUM_NULLS",
              parameters = Map("filePath" -> ".*", "config" -> aggPushDownEnabledKey))
          }
        }
      }
    }
  }

  // The error originates in the executor-side partition reader, so it may be wrapped in a
  // higher-level exception. Walk the cause chain to find the structured Spark exception.
  private def interceptAggPushDownError(query: String): SparkUnsupportedOperationException = {
    val e = intercept[Exception](spark.sql(query).collect())
    var cause: Throwable = e
    while (cause != null && !cause.isInstanceOf[SparkUnsupportedOperationException]) {
      cause = cause.getCause
    }
    assert(cause != null,
      s"Expected a SparkUnsupportedOperationException but got: $e")
    cause.asInstanceOf[SparkUnsupportedOperationException]
  }
}

abstract class OrcAggregatePushDownSuite extends OrcTest with FileSourceAggregatePushDownSuite {

  override def format: String = "orc"
  override protected val aggPushDownEnabledKey: String =
    SQLConf.ORC_AGGREGATE_PUSHDOWN_ENABLED.key
}

@SlowSQLTest
class OrcV1AggregatePushDownSuite extends OrcAggregatePushDownSuite {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "orc")
}

@SlowSQLTest
class OrcV2AggregatePushDownSuite extends OrcAggregatePushDownSuite {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  // The footer statistics must be those of the column that the reader reads, which the reader
  // finds by name or by position (see `OrcUtils.requestedColumnIds`). Each check runs the query
  // without aggregate push-down too, so the reader gives the same answer.
  private def checkAggPushDown(df: => DataFrame, pushed: String, expected: Row): Unit = {
    withSQLConf(aggPushDownEnabledKey -> "false") {
      checkAnswer(df, expected)
    }
    withSQLConf(aggPushDownEnabledKey -> "true") {
      checkPushedInfo(df, s"PushedAggregation: [$pushed]")
      checkAnswer(df, expected)
    }
  }

  test("SPARK-61499: aggregate push-down with a user schema that differs from the file schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1, 10L, Some(100)), (2, 20L, None)).toDF("x", "a", "b").coalesce(1).write.orc(path)
      // The data schema lists `b` (as `B`) first, and adds `z`, which the file does not have,
      // after the last column of the file.
      def df: DataFrame = spark.read.schema("B INT, x INT, a LONG, z INT").orc(path)
        .selectExpr("max(a)", "min(B)", "min(x)", "count(B)", "count(z)", "max(z)")
      checkAggPushDown(df, "MAX(a), MIN(B), MIN(x), COUNT(B), COUNT(z), MAX(z)",
        Row(20L, 100, 1, 1, 0, null))
    }
  }

  test("SPARK-61499: aggregate push-down matches columns by position like the reader") {
    // A file written by an old version of Hive has the column names `_col0`, `_col1`, ...
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1, 10), (2, 20)).toDF("_col0", "_col1").coalesce(1).write.orc(path)
      def df: DataFrame = spark.read.schema("y INT, x INT, z INT").orc(path)
        .selectExpr("max(y)", "max(x)", "count(z)", "max(z)")
      checkAggPushDown(df, "MAX(y), MAX(x), COUNT(z), MAX(z)", Row(2, 20, 0, null))
    }
    // With `orc.force.positional.evolution`, the reader also matches columns by position.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1, 10), (2, 20)).toDF("a", "b").coalesce(1).write.orc(path)
      Seq("true" -> Row(2, 20), "false" -> Row(20, 2)).foreach { case (positional, expected) =>
        def df: DataFrame = spark.read.schema("b INT, a INT")
          .option(OrcConf.FORCE_POSITIONAL_EVOLUTION.getAttribute, positional)
          .orc(path)
          .selectExpr("max(b)", "max(a)")
        checkAggPushDown(df, "MAX(b), MAX(a)", expected)
      }
    }
  }

  test("SPARK-61499: aggregate push-down matches column names like the reader") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 10L, 100L), (2L, 20L, 200L)).toDF("C", "b", "a").coalesce(1).write.orc(path)
      // Case-insensitive mode: `c` is the file column `C`.
      def df: DataFrame = spark.read.schema("a LONG, b LONG, c LONG").orc(path)
        .selectExpr("max(a)", "max(b)", "min(c)")
      checkAggPushDown(df, "MAX(a), MAX(b), MIN(c)", Row(200L, 20L, 1L))
    }
    // The reader lower-cases names with Locale.ROOT, so `id` and `${dotlessI}d` are different
    // columns, although String.equalsIgnoreCase says they are equal.
    val dotlessI = 0x0131.toChar.toString
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).selectExpr(s"id * 2 AS `${dotlessI}d`", "id").coalesce(1).write.orc(path)
      def df: DataFrame = spark.read.schema(s"id LONG, `${dotlessI}d` LONG").orc(path)
        .selectExpr("max(id)", s"max(`${dotlessI}d`)")
      checkAggPushDown(df, s"MAX(id), MAX(`${dotlessI}d`)", Row(2L, 4L))
    }
  }

  test("SPARK-61499: aggregate push-down in case-sensitive mode") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        sql("SELECT * FROM VALUES (1L, 10L, 100L), (2L, NULL, 200L) AS t(a, A, b)").coalesce(1)
          .write.orc(path)
        // `a` and `A` are different columns, and the file column `b` is not `B`.
        def df: DataFrame = spark.read.schema("A LONG, a LONG, B LONG").orc(path)
          .selectExpr("max(a)", "max(A)", "count(A)", "count(B)")
        checkAggPushDown(df, "MAX(a), MAX(A), COUNT(A), COUNT(B)", Row(2L, 10L, 1, 0))
      }
    }
  }

  test("SPARK-61499: aggregate push-down fails on an ambiguous case-insensitive column match") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        spark.range(3).selectExpr("id AS A", "id * 2 AS a").coalesce(1).write.orc(path)
      }
      // The reader fails in the same way for this file.
      Seq("false", "true").foreach { pushDown =>
        withSQLConf(aggPushDownEnabledKey -> pushDown) {
          val df = spark.read.schema("a LONG").orc(path).selectExpr("max(a)")
          if (pushDown == "true") checkPushedInfo(df, "PushedAggregation: [MAX(a)]")
          checkError(
            exception = interceptCause[SparkRuntimeException](df.collect()),
            condition = "_LEGACY_ERROR_TEMP_2093",
            parameters = Map("requiredFieldName" -> "a", "matchedOrcFields" -> "[A, a]"))
        }
      }
    }
  }

  test("SPARK-61499: aggregate push-down is not used for a column with an existence default") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq(7L, 7L).toDF("x").coalesce(1).write.orc(path)
      // The reader returns the existence default for `z`, which the file does not have.
      val readSchema = new StructType(Array(
        StructField("z", LongType).withExistenceDefaultValue("CAST(7 AS BIGINT)")))
      withSQLConf(aggPushDownEnabledKey -> "true") {
        val df = spark.read.schema(readSchema).orc(path).selectExpr("count(z)", "max(z)")
        checkPushedInfo(df, "PushedAggregation: []")
        checkAnswer(df, Row(2, 7L))
      }
    }
  }

  // The error comes from the partition reader, so it may be wrapped in another exception.
  private def interceptCause[T <: Throwable](f: => Any)(implicit tag: ClassTag[T]): T = {
    val e = intercept[Exception](f)
    var cause: Throwable = e
    while (cause != null && !tag.runtimeClass.isInstance(cause)) {
      cause = cause.getCause
    }
    assert(cause != null, s"Expected a ${tag.runtimeClass.getSimpleName} but got: $e")
    cause.asInstanceOf[T]
  }
}
