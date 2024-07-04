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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.orc.OrcTest
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.tags.SlowSQLTest

/**
 * A test suite that tests aggregate push down for Parquet and ORC.
 */
trait FileSourceAggregatePushDownSuite
  extends QueryTest
  with FileBasedDataSourceTest
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

  private def checkPushedInfo(df: DataFrame, expectedPlanFragment: String): Unit = {
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

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")
}
