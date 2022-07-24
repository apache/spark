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

import java.io.{ByteArrayOutputStream, File}
import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.{Locale, UUID}
import java.util.concurrent.atomic.AtomicLong

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Uuid}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LeafNode, LocalRelation, LogicalPlan, OneRowRelation, Statistics}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.execution.{FilterExec, LogicalRDD, QueryExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.test.SQLTestData.{ArrayStringWrapper, ContainerStringWrapper, DecimalData, StringWrapper, TestData2}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

class DataFrameSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("analysis error should be eagerly reported") {
    intercept[Exception] { testData.select("nonExistentName") }
    intercept[Exception] {
      testData.groupBy("key").agg(Map("nonExistentName" -> "sum"))
    }
    intercept[Exception] {
      testData.groupBy("nonExistentName").agg(Map("key" -> "sum"))
    }
    intercept[Exception] {
      testData.groupBy($"abcd").agg(Map("key" -> "sum"))
    }
  }

  test("dataframe toString") {
    assert(testData.toString === "[key: int, value: string]")
    assert(testData("key").toString === "key")
    assert($"test".toString === "test")
  }

  test("rename nested groupby") {
    val df = Seq((1, (1, 1))).toDF()

    checkAnswer(
      df.groupBy("_1").agg(sum("_2._1")).toDF("key", "total"),
      Row(1, 1) :: Nil)
  }

  test("access complex data") {
    assert(complexData.filter(complexData("a").getItem(0) === 2).count() == 1)
    if (!conf.ansiEnabled) {
      assert(complexData.filter(complexData("m").getItem("1") === 1).count() == 1)
    }
    assert(complexData.filter(complexData("s").getField("key") === 1).count() == 1)
  }

  test("table scan") {
    checkAnswer(
      testData,
      testData.collect().toSeq)
  }

  test("empty data frame") {
    assert(spark.emptyDataFrame.columns.toSeq === Seq.empty[String])
    assert(spark.emptyDataFrame.count() === 0)
  }

  test("head, take and tail") {
    assert(testData.take(2) === testData.collect().take(2))
    assert(testData.head(2) === testData.collect().take(2))
    assert(testData.tail(2) === testData.collect().takeRight(2))
    assert(testData.head(2).head.schema === testData.schema)
  }

  test("dataframe alias") {
    val df = Seq(Tuple1(1)).toDF("c").as("t")
    val dfAlias = df.alias("t2")
    df.col("t.c")
    dfAlias.col("t2.c")
  }

  test("simple explode") {
    val df = Seq(Tuple1("a b c"), Tuple1("d e")).toDF("words")

    checkAnswer(
      df.explode("words", "word") { word: String => word.split(" ").toSeq }.select($"word"),
      Row("a") :: Row("b") :: Row("c") :: Row("d") ::Row("e") :: Nil
    )
  }

  test("explode") {
    val df = Seq((1, "a b c"), (2, "a b"), (3, "a")).toDF("number", "letters")
    val df2 =
      df.explode($"letters") {
        case Row(letters: String) => letters.split(" ").map(Tuple1(_)).toSeq
      }

    checkAnswer(
      df2
        .select($"_1" as Symbol("letter"), $"number")
        .groupBy($"letter")
        .agg(count_distinct($"number")),
      Row("a", 3) :: Row("b", 2) :: Row("c", 1) :: Nil
    )
  }

  test("Star Expansion - CreateStruct and CreateArray") {
    val structDf = testData2.select("a", "b").as("record")
    // CreateStruct and CreateArray in aggregateExpressions
    assert(structDf.groupBy($"a").agg(min(struct($"record.*"))).
      sort("a").first() == Row(1, Row(1, 1)))
    assert(structDf.groupBy($"a").agg(min(array($"record.*"))).
      sort("a").first() == Row(1, Seq(1, 1)))

    // CreateStruct and CreateArray in project list (unresolved alias)
    assert(structDf.select(struct($"record.*")).first() == Row(Row(1, 1)))
    assert(structDf.select(array($"record.*")).first().getAs[Seq[Int]](0) === Seq(1, 1))

    // CreateStruct and CreateArray in project list (alias)
    assert(structDf.select(struct($"record.*").as("a")).first() == Row(Row(1, 1)))
    assert(structDf.select(array($"record.*").as("a")).first().getAs[Seq[Int]](0) === Seq(1, 1))
  }

  test("Star Expansion - hash") {
    val structDf = testData2.select("a", "b").as("record")
    checkAnswer(
      structDf.groupBy($"a", $"b").agg(min(hash($"a", $"*"))),
      structDf.groupBy($"a", $"b").agg(min(hash($"a", $"a", $"b"))))

    checkAnswer(
      structDf.groupBy($"a", $"b").agg(hash($"a", $"*")),
      structDf.groupBy($"a", $"b").agg(hash($"a", $"a", $"b")))

    checkAnswer(
      structDf.select(hash($"*")),
      structDf.select(hash($"record.*")))

    checkAnswer(
      structDf.select(hash($"a", $"*")),
      structDf.select(hash($"a", $"record.*")))
  }

  test("Star Expansion - xxhash64") {
    val structDf = testData2.select("a", "b").as("record")
    checkAnswer(
      structDf.groupBy($"a", $"b").agg(min(xxhash64($"a", $"*"))),
      structDf.groupBy($"a", $"b").agg(min(xxhash64($"a", $"a", $"b"))))

    checkAnswer(
      structDf.groupBy($"a", $"b").agg(xxhash64($"a", $"*")),
      structDf.groupBy($"a", $"b").agg(xxhash64($"a", $"a", $"b")))

    checkAnswer(
      structDf.select(xxhash64($"*")),
      structDf.select(xxhash64($"record.*")))

    checkAnswer(
      structDf.select(xxhash64($"a", $"*")),
      structDf.select(xxhash64($"a", $"record.*")))
  }

  private def assertDecimalSumOverflow(
      df: DataFrame, ansiEnabled: Boolean, expectedAnswer: Row): Unit = {
    if (!ansiEnabled) {
      checkAnswer(df, expectedAnswer)
    } else {
      val e = intercept[SparkException] {
        df.collect()
      }
      assert(e.getCause.isInstanceOf[ArithmeticException])
      assert(e.getCause.getMessage.contains("cannot be represented as Decimal") ||
        e.getCause.getMessage.contains("Overflow in sum of decimals"))
    }
  }

  test("SPARK-28224: Aggregate sum big decimal overflow") {
    val largeDecimals = spark.sparkContext.parallelize(
      DecimalData(BigDecimal("1"* 20 + ".123"), BigDecimal("1"* 20 + ".123")) ::
        DecimalData(BigDecimal("9"* 20 + ".123"), BigDecimal("9"* 20 + ".123")) :: Nil).toDF()

    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf((SQLConf.ANSI_ENABLED.key, ansiEnabled.toString)) {
        val structDf = largeDecimals.select("a").agg(sum("a"))
        assertDecimalSumOverflow(structDf, ansiEnabled, Row(null))
      }
    }
  }

  test("SPARK-28067: sum of null decimal values") {
    Seq("true", "false").foreach { wholeStageEnabled =>
      withSQLConf((SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStageEnabled)) {
        Seq("true", "false").foreach { ansiEnabled =>
          withSQLConf((SQLConf.ANSI_ENABLED.key, ansiEnabled)) {
            val df = spark.range(1, 4, 1).select(expr(s"cast(null as decimal(38,18)) as d"))
            checkAnswer(df.agg(sum($"d")), Row(null))
          }
        }
      }
    }
  }

  def checkAggResultsForDecimalOverflow(aggFn: Column => Column): Unit = {
    Seq("true", "false").foreach { wholeStageEnabled =>
      withSQLConf((SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStageEnabled)) {
        Seq(true, false).foreach { ansiEnabled =>
          withSQLConf((SQLConf.ANSI_ENABLED.key, ansiEnabled.toString)) {
            val df0 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")
            val df1 = Seq(
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")
            val df = df0.union(df1)
            val df2 = df.withColumnRenamed("decNum", "decNum2").
              join(df, "intNum").agg(aggFn($"decNum"))

            val expectedAnswer = Row(null)
            assertDecimalSumOverflow(df2, ansiEnabled, expectedAnswer)

            val decStr = "1" + "0" * 19
            val d1 = spark.range(0, 12, 1, 1)
            val d2 = d1.select(expr(s"cast('$decStr' as decimal (38, 18)) as d")).agg(aggFn($"d"))
            assertDecimalSumOverflow(d2, ansiEnabled, expectedAnswer)

            val d3 = spark.range(0, 1, 1, 1).union(spark.range(0, 11, 1, 1))
            val d4 = d3.select(expr(s"cast('$decStr' as decimal (38, 18)) as d")).agg(aggFn($"d"))
            assertDecimalSumOverflow(d4, ansiEnabled, expectedAnswer)

            val d5 = d3.select(expr(s"cast('$decStr' as decimal (38, 18)) as d"),
              lit(1).as("key")).groupBy("key").agg(aggFn($"d").alias("aggd")).select($"aggd")
            assertDecimalSumOverflow(d5, ansiEnabled, expectedAnswer)

            val nullsDf = spark.range(1, 4, 1).select(expr(s"cast(null as decimal(38,18)) as d"))

            val largeDecimals = Seq(BigDecimal("1"* 20 + ".123"), BigDecimal("9"* 20 + ".123")).
              toDF("d")
            assertDecimalSumOverflow(
              nullsDf.union(largeDecimals).agg(aggFn($"d")), ansiEnabled, expectedAnswer)

            val df3 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("50000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")

            val df4 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")

            val df5 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("20000000000000000000"), 2)).toDF("decNum", "intNum")

            val df6 = df3.union(df4).union(df5)
            val df7 = df6.groupBy("intNum").agg(sum("decNum"), countDistinct("decNum")).
              filter("intNum == 1")
            assertDecimalSumOverflow(df7, ansiEnabled, Row(1, null, 2))
          }
        }
      }
    }
  }

  test("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow") {
    checkAggResultsForDecimalOverflow(c => sum(c))
  }

  test("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow") {
    checkAggResultsForDecimalOverflow(c => avg(c))
  }

  test("Star Expansion - ds.explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1", "1,2"), ("2", "4"), ("3", "7,8,9")).toDF("prefix", "csv")
    val e = intercept[AnalysisException] {
      df.explode($"*") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      }.queryExecution.assertAnalyzed()
    }
    assert(e.getMessage.contains("Invalid usage of '*' in explode/json_tuple/UDTF"))

    checkAnswer(
      df.explode($"prefix", $"csv") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      },
      Row("1", "1,2", "1:1") ::
        Row("1", "1,2", "1:2") ::
        Row("2", "4", "2:4") ::
        Row("3", "7,8,9", "3:7") ::
        Row("3", "7,8,9", "3:8") ::
        Row("3", "7,8,9", "3:9") :: Nil)
  }

  test("Star Expansion - explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1,2"), ("4"), ("7,8,9")).toDF("csv")
    val e = intercept[AnalysisException] {
      df.select(explode($"*"))
    }
    assert(e.getMessage.contains("Invalid usage of '*' in expression 'explode'"))
  }

  test("explode on output of array-valued function") {
    val df = Seq(("1,2"), ("4"), ("7,8,9")).toDF("csv")
    checkAnswer(
      df.select(explode(split($"csv", pattern = ","))),
      Row("1") :: Row("2") :: Row("4") :: Row("7") :: Row("8") :: Row("9") :: Nil)
  }

  test("Star Expansion - explode alias and star") {
    val df = Seq((Array("a"), 1)).toDF("a", "b")

    checkAnswer(
      df.select(explode($"a").as("a"), $"*"),
      Row("a", Seq("a"), 1) :: Nil)
  }

  test("sort after generate with join=true") {
    val df = Seq((Array("a"), 1)).toDF("a", "b")

    checkAnswer(
      df.select($"*", explode($"a").as("c")).sortWithinPartitions("b", "c"),
      Row(Seq("a"), 1, "a") :: Nil)
  }

  test("selectExpr") {
    checkAnswer(
      testData.selectExpr("abs(key)", "value"),
      testData.collect().map(row => Row(math.abs(row.getInt(0)), row.getString(1))).toSeq)
  }

  test("selectExpr with alias") {
    checkAnswer(
      testData.selectExpr("key as k").select("k"),
      testData.select("key").collect().toSeq)
  }

  test("selectExpr with udtf") {
    val df = Seq((Map("1" -> 1), 1)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("explode(a)"),
      Row("1", 1) :: Nil)
  }

  test("filterExpr") {
    val res = testData.collect().filter(_.getInt(0) > 90).toSeq
    checkAnswer(testData.filter("key > 90"), res)
    checkAnswer(testData.filter("key > 9.0e1"), res)
    checkAnswer(testData.filter("key > .9e+2"), res)
    checkAnswer(testData.filter("key > 0.9e+2"), res)
    checkAnswer(testData.filter("key > 900e-1"), res)
    checkAnswer(testData.filter("key > 900.0E-1"), res)
    checkAnswer(testData.filter("key > 9.e+1"), res)
  }

  test("filterExpr using where") {
    checkAnswer(
      testData.where("key > 50"),
      testData.collect().filter(_.getInt(0) > 50).toSeq)
  }

  test("repartition") {
    intercept[IllegalArgumentException] {
      testData.select("key").repartition(0)
    }

    checkAnswer(
      testData.select("key").repartition(10).select("key"),
      testData.select("key").collect().toSeq)
  }

  test("repartition with SortOrder") {
    // passing SortOrder expressions to .repartition() should result in an informative error

    def checkSortOrderErrorMsg[T](data: => Dataset[T]): Unit = {
      val ex = intercept[IllegalArgumentException](data)
      assert(ex.getMessage.contains("repartitionByRange"))
    }

    checkSortOrderErrorMsg {
      Seq(0).toDF("a").repartition(2, $"a".asc)
    }

    checkSortOrderErrorMsg {
      Seq((0, 0)).toDF("a", "b").repartition(2, $"a".asc, $"b")
    }
  }

  test("repartitionByRange") {
    val data1d = Random.shuffle(0.to(9))
    val data2d = data1d.map(i => (i, data1d.size - i))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, $"val".asc)
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(i, i)))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, $"val".desc)
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(i, data1d.size - 1 - i)))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, lit(42))
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(0, i)))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, lit(null), $"val".asc, rand())
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(i, i)))

    // .repartitionByRange() assumes .asc by default if no explicit sort order is specified
    checkAnswer(
      data2d.toDF("a", "b").repartitionByRange(data2d.size, $"a".desc, $"b")
        .select(spark_partition_id().as("id"), $"a", $"b"),
      data2d.toDF("a", "b").repartitionByRange(data2d.size, $"a".desc, $"b".asc)
        .select(spark_partition_id().as("id"), $"a", $"b"))

    // at least one partition-by expression must be specified
    intercept[IllegalArgumentException] {
      data1d.toDF("val").repartitionByRange(data1d.size)
    }
    intercept[IllegalArgumentException] {
      data1d.toDF("val").repartitionByRange(data1d.size, Seq.empty: _*)
    }
  }

  test("coalesce") {
    intercept[IllegalArgumentException] {
      testData.select("key").coalesce(0)
    }

    assert(testData.select("key").coalesce(1).rdd.partitions.size === 1)

    checkAnswer(
      testData.select("key").coalesce(1).select("key"),
      testData.select("key").collect().toSeq)

    assert(spark.emptyDataFrame.coalesce(1).rdd.partitions.size === 0)
  }

  test("convert $\"attribute name\" into unresolved attribute") {
    checkAnswer(
      testData.where($"key" === lit(1)).select($"value"),
      Row("1"))
  }

  test("convert Scala Symbol 'attrname into unresolved attribute") {
    checkAnswer(
      testData.where($"key" === lit(1)).select("value"),
      Row("1"))
  }

  test("select *") {
    checkAnswer(
      testData.select($"*"),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      testData.where($"key" === lit(1)).select("value"),
      Row("1"))
  }

  test("select with functions") {
    checkAnswer(
      testData.select(sum("value"), avg("value"), count(lit(1))),
      Row(5050.0, 50.5, 100))

    checkAnswer(
      testData2.select($"a" + $"b", $"a" < $"b"),
      Seq(
        Row(2, false),
        Row(3, true),
        Row(3, false),
        Row(4, false),
        Row(4, false),
        Row(5, false)))

    checkAnswer(
      testData2.select(sum_distinct($"a")),
      Row(6))
  }

  test("sorting with null ordering") {
    val data = Seq[java.lang.Integer](2, 1, null).toDF("key")

    checkAnswer(data.orderBy($"key".asc), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy(asc("key")), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy($"key".asc_nulls_first), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy(asc_nulls_first("key")), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy($"key".asc_nulls_last), Row(1) :: Row(2) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(asc_nulls_last("key")), Row(1) :: Row(2) :: Row(null) :: Nil)

    checkAnswer(data.orderBy($"key".desc), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(desc("key")), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy($"key".desc_nulls_first), Row(null) :: Row(2) :: Row(1) :: Nil)
    checkAnswer(data.orderBy(desc_nulls_first("key")), Row(null) :: Row(2) :: Row(1) :: Nil)
    checkAnswer(data.orderBy($"key".desc_nulls_last), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(desc_nulls_last("key")), Row(2) :: Row(1) :: Row(null) :: Nil)
  }

  test("global sorting") {
    checkAnswer(
      testData2.orderBy($"a".asc, $"b".asc),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    checkAnswer(
      testData2.orderBy(asc("a"), desc("b")),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy($"a".asc, $"b".desc),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy($"a".desc, $"b".desc),
      Seq(Row(3, 2), Row(3, 1), Row(2, 2), Row(2, 1), Row(1, 2), Row(1, 1)))

    checkAnswer(
      testData2.orderBy($"a".desc, $"b".asc),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2)))

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(0).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(0).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).reverse.toSeq)

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(1).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(1).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).reverse.toSeq)
  }

  test("limit") {
    checkAnswer(
      testData.limit(10),
      testData.take(10).toSeq)

    checkAnswer(
      arrayData.toDF().limit(1),
      arrayData.take(1).map(r => Row.fromSeq(r.productIterator.toSeq)))

    checkAnswer(
      mapData.toDF().limit(1),
      mapData.take(1).map(r => Row.fromSeq(r.productIterator.toSeq)))

    // SPARK-12340: overstep the bounds of Int in SparkPlan.executeTake
    checkAnswer(
      spark.range(2).toDF().limit(2147483638),
      Row(0) :: Row(1) :: Nil
    )
  }

  test("offset") {
    checkAnswer(
      testData.offset(90),
      testData.collect().drop(90).toSeq)

    checkAnswer(
      arrayData.toDF().offset(99),
      arrayData.collect().drop(99).map(r => Row.fromSeq(r.productIterator.toSeq)))

    checkAnswer(
      mapData.toDF().offset(99),
      mapData.collect().drop(99).map(r => Row.fromSeq(r.productIterator.toSeq)))
  }

  test("limit with offset") {
    checkAnswer(
      testData.limit(10).offset(5),
      testData.take(10).drop(5).toSeq)

    checkAnswer(
      testData.offset(5).limit(10),
      testData.take(15).drop(5).toSeq)
  }

  test("udf") {
    val foo = udf((a: Int, b: String) => a.toString + b)

    checkAnswer(
      // SELECT *, foo(key, value) FROM testData
      testData.select($"*", foo($"key", $"value")).limit(3),
      Row(1, "1", "11") :: Row(2, "2", "22") :: Row(3, "3", "33") :: Nil
    )
  }

  test("callUDF without Hive Support") {
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    df.sparkSession.udf.register("simpleUDF", (v: Int) => v * v)
    checkAnswer(
      df.select($"id", callUDF("simpleUDF", $"value")), // test deprecated one
      Row("id1", 1) :: Row("id2", 16) :: Row("id3", 25) :: Nil)
  }

  test("withColumn") {
    val df = testData.toDF().withColumn("newCol", col("key") + 1)
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol"))
  }

  test("withColumns: public API, with Map input") {
    val df = testData.toDF().withColumns(Map(
      "newCol1" -> (col("key") + 1), "newCol2" -> (col("key")  + 2)
    ))
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1, key + 2)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol1", "newCol2"))
  }

  test("withColumns: internal method") {
    val df = testData.toDF().withColumns(Seq("newCol1", "newCol2"),
      Seq(col("key") + 1, col("key") + 2))
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1, key + 2)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol1", "newCol2"))

    val err = intercept[IllegalArgumentException] {
      testData.toDF().withColumns(Seq("newCol1"),
        Seq(col("key") + 1, col("key") + 2))
    }
    assert(
      err.getMessage.contains("The size of column names: 1 isn't equal to the size of columns: 2"))

    val err2 = intercept[AnalysisException] {
      testData.toDF().withColumns(Seq("newCol1", "newCOL1"),
        Seq(col("key") + 1, col("key") + 2))
    }
    assert(err2.getMessage.contains("Found duplicate column(s)"))
  }

  test("withColumns: internal method, case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = testData.toDF().withColumns(Seq("newCol1", "newCOL1"),
        Seq(col("key") + 1, col("key") + 2))
      checkAnswer(
        df,
        testData.collect().map { case Row(key: Int, value: String) =>
          Row(key, value, key + 1, key + 2)
        }.toSeq)
      assert(df.schema.map(_.name) === Seq("key", "value", "newCol1", "newCOL1"))

      val err = intercept[AnalysisException] {
        testData.toDF().withColumns(Seq("newCol1", "newCol1"),
          Seq(col("key") + 1, col("key") + 2))
      }
      assert(err.getMessage.contains("Found duplicate column(s)"))
    }
  }

  test("withColumns: internal method, given metadata") {
    def buildMetadata(num: Int): Seq[Metadata] = {
      (0 until num).map { n =>
        val builder = new MetadataBuilder
        builder.putLong("key", n.toLong)
        builder.build()
      }
    }

    val df = testData.toDF().withColumns(
      Seq("newCol1", "newCol2"),
      Seq(col("key") + 1, col("key") + 2),
      buildMetadata(2))

    df.select("newCol1", "newCol2").schema.zipWithIndex.foreach { case (col, idx) =>
      assert(col.metadata.getLong("key").toInt === idx)
    }

    val err = intercept[IllegalArgumentException] {
      testData.toDF().withColumns(
        Seq("newCol1", "newCol2"),
        Seq(col("key") + 1, col("key") + 2),
        buildMetadata(1))
    }
    assert(err.getMessage.contains(
      "The size of column names: 2 isn't equal to the size of metadata elements: 1"))
  }

  test("SPARK-36642: withMetadata: replace metadata of a column") {
    val metadata = new MetadataBuilder().putLong("key", 1L).build()
    val df1 = sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
    val df2 = df1.withMetadata("x", metadata)
    assert(df2.schema(0).metadata === metadata)

    val err = intercept[AnalysisException] {
      df1.withMetadata("x1", metadata)
    }
    assert(err.getMessage.contains("Cannot resolve column name"))
  }

  test("replace column using withColumn") {
    val df2 = sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
    val df3 = df2.withColumn("x", df2("x") + 1)
    checkAnswer(
      df3.select("x"),
      Row(2) :: Row(3) :: Row(4) :: Nil)
  }

  test("replace column using withColumns") {
    val df2 = sparkContext.parallelize(Seq((1, 2), (2, 3), (3, 4))).toDF("x", "y")
    val df3 = df2.withColumns(Seq("x", "newCol1", "newCol2"),
      Seq(df2("x") + 1, df2("y"), df2("y") + 1))
    checkAnswer(
      df3.select("x", "newCol1", "newCol2"),
      Row(2, 2, 3) :: Row(3, 3, 4) :: Row(4, 4, 5) :: Nil)
  }

  test("drop column using drop") {
    val df = testData.drop("key")
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("drop columns using drop") {
    val src = Seq((0, 2, 3)).toDF("a", "b", "c")
    val df = src.drop("a", "b")
    checkAnswer(df, Row(3))
    assert(df.schema.map(_.name) === Seq("c"))
  }

  test("drop unknown column (no-op)") {
    val df = testData.drop("random")
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }

  test("drop column using drop with column reference") {
    val col = testData("key")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("SPARK-28189 drop column using drop with column reference with case-insensitive names") {
    // With SQL config caseSensitive OFF, case insensitive column name should work
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val col1 = testData("KEY")
      val df1 = testData.drop(col1)
      checkAnswer(df1, testData.selectExpr("value"))
      assert(df1.schema.map(_.name) === Seq("value"))

      val col2 = testData("Key")
      val df2 = testData.drop(col2)
      checkAnswer(df2, testData.selectExpr("value"))
      assert(df2.schema.map(_.name) === Seq("value"))
    }
  }

  test("drop unknown column (no-op) with column reference") {
    val col = Column("random")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }

  test("drop unknown column with same name with column reference") {
    val col = Column("key")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("drop column after join with duplicate columns using column reference") {
    val newSalary = salary.withColumnRenamed("personId", "id")
    val col = newSalary("id")
    // this join will result in duplicate "id" columns
    val joinedDf = person.join(newSalary,
      person("id") === newSalary("id"), "inner")
    // remove only the "id" column that was associated with newSalary
    val df = joinedDf.drop(col)
    checkAnswer(
      df,
      joinedDf.collect().map {
        case Row(id: Int, name: String, age: Int, idToDrop: Int, salary: Double) =>
          Row(id, name, age, salary)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("id", "name", "age", "salary"))
    assert(df("id") == person("id"))
  }

  test("drop top level columns that contains dot") {
    val df1 = Seq((1, 2)).toDF("a.b", "a.c")
    checkAnswer(df1.drop("a.b"), Row(2))

    // Creates data set: {"a.b": 1, "a": {"b": 3}}
    val df2 = Seq((1)).toDF("a.b").withColumn("a", struct(lit(3) as "b"))
    // Not like select(), drop() parses the column name "a.b" literally without interpreting "."
    checkAnswer(df2.drop("a.b").select("a.b"), Row(3))

    // "`" is treated as a normal char here with no interpreting, "`a`b" is a valid column name.
    assert(df2.drop("`a.b`").columns.size == 2)
  }

  test("drop(name: String) search and drop all top level columns that matches the name") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((3, 4)).toDF("a", "b")
    checkAnswer(df1.crossJoin(df2), Row(1, 2, 3, 4))
    // Finds and drops all columns that match the name (case insensitive).
    checkAnswer(df1.crossJoin(df2).drop("A"), Row(2, 4))
  }

  test("withColumnRenamed") {
    val df = testData.toDF().withColumn("newCol", col("key") + 1)
      .withColumnRenamed("value", "valueRenamed")
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "valueRenamed", "newCol"))
  }

  test("SPARK-20384: Value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(StringWrapper("a"), StringWrapper("b"), StringWrapper("c")))
      .toDF()
    val filtered = df.where("s = \"a\"")
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(StringWrapper("a"))).toDF)
  }

  test("SPARK-20384: Tuple2 of value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(
        (StringWrapper("a1"), StringWrapper("a2")),
        (StringWrapper("b1"), StringWrapper("b2"))))
      .toDF()
    val filtered = df.where("_2.s = \"a2\"")
    checkAnswer(filtered,
      spark.sparkContext.parallelize(Seq((StringWrapper("a1"), StringWrapper("a2")))).toDF)
  }

  test("SPARK-20384: Tuple3 of value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(
        (StringWrapper("a1"), StringWrapper("a2"), StringWrapper("a3")),
        (StringWrapper("b1"), StringWrapper("b2"), StringWrapper("b3"))))
      .toDF()
    val filtered = df.where("_3.s = \"a3\"")
    checkAnswer(filtered,
      spark.sparkContext.parallelize(
        Seq((StringWrapper("a1"), StringWrapper("a2"), StringWrapper("a3")))).toDF)
  }

  test("SPARK-20384: Array value class filter") {
    val ab = ArrayStringWrapper(Seq(StringWrapper("a"), StringWrapper("b")))
    val cd = ArrayStringWrapper(Seq(StringWrapper("c"), StringWrapper("d")))

    val df = spark.sparkContext.parallelize(Seq(ab, cd)).toDF
    val filtered = df.where(array_contains(col("wrappers.s"), "b"))
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(ab)).toDF)
  }

  test("SPARK-20384: Nested value class filter") {
    val a = ContainerStringWrapper(StringWrapper("a"))
    val b = ContainerStringWrapper(StringWrapper("b"))

    val df = spark.sparkContext.parallelize(Seq(a, b)).toDF
    // flat value class, `s` field is not in schema
    val filtered = df.where("wrapper = \"a\"")
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(a)).toDF)
  }

  private lazy val person2: DataFrame = Seq(
    ("Bob", 16, 176),
    ("Alice", 32, 164),
    ("David", 60, 192),
    ("Amy", 24, 180)).toDF("name", "age", "height")

  private lazy val person3: DataFrame = Seq(
    ("Luis", 1, 99),
    ("Luis", 16, 99),
    ("Luis", 16, 176),
    ("Fernando", 32, 99),
    ("Fernando", 32, 164),
    ("David", 60, 99),
    ("Amy", 24, 99)).toDF("name", "age", "height")

  test("describe") {
    val describeResult = Seq(
      Row("count", "4", "4", "4"),
      Row("mean", null, "33.0", "178.0"),
      Row("stddev", null, "19.148542155126762", "11.547005383792516"),
      Row("min", "Alice", "16", "164"),
      Row("max", "David", "60", "192"))

    val emptyDescribeResult = Seq(
      Row("count", "0", "0", "0"),
      Row("mean", null, null, null),
      Row("stddev", null, null, null),
      Row("min", null, null, null),
      Row("max", null, null, null))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    Seq("true", "false").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val describeAllCols = person2.describe()
        assert(getSchemaAsSeq(describeAllCols) === Seq("summary", "name", "age", "height"))
        checkAnswer(describeAllCols, describeResult)
        // All aggregate value should have been cast to string
        describeAllCols.collect().foreach { row =>
          row.toSeq.foreach { value =>
            if (value != null) {
              assert(value.isInstanceOf[String], "expected string but found " + value.getClass)
            }
          }
        }

        val describeOneCol = person2.describe("age")
        assert(getSchemaAsSeq(describeOneCol) === Seq("summary", "age"))
        checkAnswer(describeOneCol, describeResult.map { case Row(s, _, d, _) => Row(s, d) })

        val describeNoCol = person2.select().describe()
        assert(getSchemaAsSeq(describeNoCol) === Seq("summary"))
        checkAnswer(describeNoCol, describeResult.map { case Row(s, _, _, _) => Row(s) })

        val emptyDescription = person2.limit(0).describe()
        assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
        checkAnswer(emptyDescription, emptyDescribeResult)
      }
    }
  }

  test("summary") {
    val summaryResult = Seq(
      Row("count", "4", "4", "4"),
      Row("mean", null, "33.0", "178.0"),
      Row("stddev", null, "19.148542155126762", "11.547005383792516"),
      Row("min", "Alice", "16", "164"),
      Row("25%", null, "16", "164"),
      Row("50%", null, "24", "176"),
      Row("75%", null, "32", "180"),
      Row("max", "David", "60", "192"))

    val emptySummaryResult = Seq(
      Row("count", "0", "0", "0"),
      Row("mean", null, null, null),
      Row("stddev", null, null, null),
      Row("min", null, null, null),
      Row("25%", null, null, null),
      Row("50%", null, null, null),
      Row("75%", null, null, null),
      Row("max", null, null, null))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    Seq("true", "false").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val summaryAllCols = person2.summary()

        assert(getSchemaAsSeq(summaryAllCols) === Seq("summary", "name", "age", "height"))
        checkAnswer(summaryAllCols, summaryResult)
        // All aggregate value should have been cast to string
        summaryAllCols.collect().foreach { row =>
          row.toSeq.foreach { value =>
            if (value != null) {
              assert(value.isInstanceOf[String], "expected string but found " + value.getClass)
            }
          }
        }

        val summaryOneCol = person2.select("age").summary()
        assert(getSchemaAsSeq(summaryOneCol) === Seq("summary", "age"))
        checkAnswer(summaryOneCol, summaryResult.map { case Row(s, _, d, _) => Row(s, d) })

        val summaryNoCol = person2.select().summary()
        assert(getSchemaAsSeq(summaryNoCol) === Seq("summary"))
        checkAnswer(summaryNoCol, summaryResult.map { case Row(s, _, _, _) => Row(s) })

        val emptyDescription = person2.limit(0).summary()
        assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
        checkAnswer(emptyDescription, emptySummaryResult)
      }
    }
  }

  test("SPARK-34165: Add count_distinct to summary") {
    val summaryDF = person3.summary("count", "count_distinct")

    val summaryResult = Seq(
      Row("count", "7", "7", "7"),
      Row("count_distinct", "4", "5", "3"))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)
    assert(getSchemaAsSeq(summaryDF) === Seq("summary", "name", "age", "height"))
    checkAnswer(summaryDF, summaryResult)

    val approxSummaryDF = person3.summary("count", "approx_count_distinct")
    val approxSummaryResult = Seq(
      Row("count", "7", "7", "7"),
      Row("approx_count_distinct", "4", "5", "3"))
    assert(getSchemaAsSeq(summaryDF) === Seq("summary", "name", "age", "height"))
    checkAnswer(approxSummaryDF, approxSummaryResult)
  }

  test("summary advanced") {
    val stats = Array("count", "50.01%", "max", "mean", "min", "25%")
    val orderMatters = person2.summary(stats: _*)
    assert(orderMatters.collect().map(_.getString(0)) === stats)

    val onlyPercentiles = person2.summary("0.1%", "99.9%")
    assert(onlyPercentiles.count() === 2)

    val fooE = intercept[IllegalArgumentException] {
      person2.summary("foo")
    }
    assert(fooE.getMessage === "foo is not a recognised statistic")

    val parseE = intercept[IllegalArgumentException] {
      person2.summary("foo%")
    }
    assert(parseE.getMessage === "Unable to parse foo% as a percentile")
  }

  test("apply on query results (SPARK-5462)") {
    val df = testData.sparkSession.sql("select key from testData")
    checkAnswer(df.select(df("key")), testData.select("key").collect().toSeq)
  }

  test("inputFiles") {
    Seq("csv", "").foreach { useV1List =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
        withTempDir { dir =>
          val df = Seq((1, 22)).toDF("a", "b")

          val parquetDir = new File(dir, "parquet").getCanonicalPath
          df.write.parquet(parquetDir)
          val parquetDF = spark.read.parquet(parquetDir)
          assert(parquetDF.inputFiles.nonEmpty)

          val csvDir = new File(dir, "csv").getCanonicalPath
          df.write.json(csvDir)
          val csvDF = spark.read.json(csvDir)
          assert(csvDF.inputFiles.nonEmpty)

          val unioned = csvDF.union(parquetDF).inputFiles.sorted
          val allFiles = (csvDF.inputFiles ++ parquetDF.inputFiles).distinct.sorted
          assert(unioned === allFiles)
        }
      }
    }
  }

  ignore("show") {
    // This test case is intended ignored, but to make sure it compiles correctly
    testData.select($"*").show()
    testData.select($"*").show(1000)
  }

  test("getRows: truncate = [0, 20]") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = Seq(
      Seq("value"),
      Seq("1"),
      Seq("111111111111111111111"))
    assert(df.getRows(10, 0) === expectedAnswerForFalse)
    val expectedAnswerForTrue = Seq(
      Seq("value"),
      Seq("1"),
      Seq("11111111111111111..."))
    assert(df.getRows(10, 20) === expectedAnswerForTrue)
  }

  test("getRows: truncate = [3, 17]") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = Seq(
      Seq("value"),
      Seq("1"),
      Seq("111"))
    assert(df.getRows(10, 3) === expectedAnswerForFalse)
    val expectedAnswerForTrue = Seq(
      Seq("value"),
      Seq("1"),
      Seq("11111111111111..."))
    assert(df.getRows(10, 17) === expectedAnswerForTrue)
  }

  test("getRows: numRows = 0") {
    val expectedAnswer = Seq(Seq("key", "value"), Seq("1", "1"))
    assert(testData.select($"*").getRows(0, 20) === expectedAnswer)
  }

  test("getRows: array") {
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = Seq(
      Seq("_1", "_2"),
      Seq("[1, 2, 3]", "[1, 2, 3]"),
      Seq("[2, 3, 4]", "[2, 3, 4]"))
    assert(df.getRows(10, 20) === expectedAnswer)
  }

  test("getRows: binary") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()
    val expectedAnswer = Seq(
      Seq("_1", "_2"),
      Seq("[31 32]", "[41 42 43 2E]"),
      Seq("[33 34]", "[31 32 33 34 36]"))
    assert(df.getRows(10, 20) === expectedAnswer)
  }

  test("showString: truncate = [0, 20]") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = """+---------------------+
                                   ||value                |
                                   |+---------------------+
                                   ||1                    |
                                   ||111111111111111111111|
                                   |+---------------------+
                                   |""".stripMargin
    assert(df.showString(10, truncate = 0) === expectedAnswerForFalse)
    val expectedAnswerForTrue = """+--------------------+
                                  ||               value|
                                  |+--------------------+
                                  ||                   1|
                                  ||11111111111111111...|
                                  |+--------------------+
                                  |""".stripMargin
    assert(df.showString(10, truncate = 20) === expectedAnswerForTrue)
  }

  test("showString: truncate = [0, 20], vertical = true") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = "-RECORD 0----------------------\n" +
                                 " value | 1                     \n" +
                                 "-RECORD 1----------------------\n" +
                                 " value | 111111111111111111111 \n"
    assert(df.showString(10, truncate = 0, vertical = true) === expectedAnswerForFalse)
    val expectedAnswerForTrue = "-RECORD 0---------------------\n" +
                                " value | 1                    \n" +
                                "-RECORD 1---------------------\n" +
                                " value | 11111111111111111... \n"
    assert(df.showString(10, truncate = 20, vertical = true) === expectedAnswerForTrue)
  }

  test("showString: truncate = [3, 17]") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = """+-----+
                                   ||value|
                                   |+-----+
                                   ||    1|
                                   ||  111|
                                   |+-----+
                                   |""".stripMargin
    assert(df.showString(10, truncate = 3) === expectedAnswerForFalse)
    val expectedAnswerForTrue = """+-----------------+
                                  ||            value|
                                  |+-----------------+
                                  ||                1|
                                  ||11111111111111...|
                                  |+-----------------+
                                  |""".stripMargin
    assert(df.showString(10, truncate = 17) === expectedAnswerForTrue)
  }

  test("showString: truncate = [3, 17], vertical = true") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = "-RECORD 0----\n" +
                                 " value | 1   \n" +
                                 "-RECORD 1----\n" +
                                 " value | 111 \n"
    assert(df.showString(10, truncate = 3, vertical = true) === expectedAnswerForFalse)
    val expectedAnswerForTrue = "-RECORD 0------------------\n" +
                                " value | 1                 \n" +
                                "-RECORD 1------------------\n" +
                                " value | 11111111111111... \n"
    assert(df.showString(10, truncate = 17, vertical = true) === expectedAnswerForTrue)
  }

  test("showString(negative)") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
    assert(testData.select($"*").showString(-1) === expectedAnswer)
  }

  test("showString(negative), vertical = true") {
    val expectedAnswer = "(0 rows)\n"
    assert(testData.select($"*").showString(-1, vertical = true) === expectedAnswer)
  }

  test("showString(0)") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
    assert(testData.select($"*").showString(0) === expectedAnswer)
  }

  test("showString(Int.MaxValue)") {
    val df = Seq((1, 2), (3, 4)).toDF("a", "b")
    val expectedAnswer = """+---+---+
                           ||  a|  b|
                           |+---+---+
                           ||  1|  2|
                           ||  3|  4|
                           |+---+---+
                           |""".stripMargin
    assert(df.showString(Int.MaxValue) === expectedAnswer)
  }

  test("showString(0), vertical = true") {
    val expectedAnswer = "(0 rows)\n"
    assert(testData.select($"*").showString(0, vertical = true) === expectedAnswer)
  }

  test("showString: array") {
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = """+---------+---------+
                           ||       _1|       _2|
                           |+---------+---------+
                           ||[1, 2, 3]|[1, 2, 3]|
                           ||[2, 3, 4]|[2, 3, 4]|
                           |+---------+---------+
                           |""".stripMargin
    assert(df.showString(10) === expectedAnswer)
  }

  test("showString: array, vertical = true") {
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = "-RECORD 0--------\n" +
                         " _1  | [1, 2, 3] \n" +
                         " _2  | [1, 2, 3] \n" +
                         "-RECORD 1--------\n" +
                         " _1  | [2, 3, 4] \n" +
                         " _2  | [2, 3, 4] \n"
    assert(df.showString(10, vertical = true) === expectedAnswer)
  }

  test("showString: binary") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()
    val expectedAnswer = """+-------+----------------+
                           ||     _1|              _2|
                           |+-------+----------------+
                           ||[31 32]|   [41 42 43 2E]|
                           ||[33 34]|[31 32 33 34 36]|
                           |+-------+----------------+
                           |""".stripMargin
    assert(df.showString(10) === expectedAnswer)
  }

  test("showString: binary, vertical = true") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()
    val expectedAnswer = "-RECORD 0---------------\n" +
                         " _1  | [31 32]          \n" +
                         " _2  | [41 42 43 2E]    \n" +
                         "-RECORD 1---------------\n" +
                         " _1  | [33 34]          \n" +
                         " _2  | [31 32 33 34 36] \n"
    assert(df.showString(10, vertical = true) === expectedAnswer)
  }

  test("showString: minimum column width") {
    val df = Seq(
      (1, 1),
      (2, 2)
    ).toDF()
    val expectedAnswer = """+---+---+
                           || _1| _2|
                           |+---+---+
                           ||  1|  1|
                           ||  2|  2|
                           |+---+---+
                           |""".stripMargin
    assert(df.showString(10) === expectedAnswer)
  }

  test("showString: minimum column width, vertical = true") {
    val df = Seq(
      (1, 1),
      (2, 2)
    ).toDF()
    val expectedAnswer = "-RECORD 0--\n" +
                         " _1  | 1   \n" +
                         " _2  | 1   \n" +
                         "-RECORD 1--\n" +
                         " _1  | 2   \n" +
                         " _2  | 2   \n"
    assert(df.showString(10, vertical = true) === expectedAnswer)
  }

  test("SPARK-33690: showString: escape meta-characters") {
    val df1 = spark.sql("SELECT 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh'")
    assert(df1.showString(1, truncate = 0) ===
      """+--------------------------------------+
        ||aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh|
        |+--------------------------------------+
        ||aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh|
        |+--------------------------------------+
        |""".stripMargin)

    val df2 = spark.sql("SELECT array('aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    assert(df2.showString(1, truncate = 0) ===
      """+---------------------------------------------+
        ||array(aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh)|
        |+---------------------------------------------+
        ||[aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh]     |
        |+---------------------------------------------+
        |""".stripMargin)

    val df3 =
      spark.sql("SELECT map('aaa\nbbb\tccc', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    assert(df3.showString(1, truncate = 0) ===
      """+----------------------------------------------------------+
        ||map(aaa\nbbb\tccc, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh)|
        |+----------------------------------------------------------+
        ||{aaa\nbbb\tccc -> aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh} |
        |+----------------------------------------------------------+
        |""".stripMargin)

    val df4 =
      spark.sql("SELECT named_struct('v', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    assert(df4.showString(1, truncate = 0) ===
      """+-------------------------------------------------------+
        ||named_struct(v, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh)|
        |+-------------------------------------------------------+
        ||{aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh}               |
        |+-------------------------------------------------------+
        |""".stripMargin)
  }

  test("SPARK-34308: printSchema: escape meta-characters") {
    val captured = new ByteArrayOutputStream()

    val df1 = spark.sql("SELECT 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh'")
    Console.withOut(captured) {
      df1.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh: string (nullable = false)
        |
        |""".stripMargin)
    captured.reset()

    val df2 = spark.sql("SELECT array('aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    Console.withOut(captured) {
      df2.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- array(aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh): array (nullable = false)
        | |    |-- element: string (containsNull = false)
        |
        |""".stripMargin)
    captured.reset()

    val df3 =
      spark.sql("SELECT map('aaa\nbbb\tccc', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    Console.withOut(captured) {
      df3.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- map(aaa\nbbb\tccc, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh): map (nullable = false)
        | |    |-- key: string
        | |    |-- value: string (valueContainsNull = false)
        |
        |""".stripMargin)
    captured.reset()

    val df4 =
      spark.sql("SELECT named_struct('v', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    Console.withOut(captured) {
      df4.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- named_struct(v, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh): struct (nullable = false)
        | |    |-- v: string (nullable = false)
        |
        |""".stripMargin)
  }

  test("SPARK-7319 showString") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           ||  1|    1|
                           |+---+-----+
                           |only showing top 1 row
                           |""".stripMargin
    assert(testData.select($"*").showString(1) === expectedAnswer)
  }

  test("SPARK-7319 showString, vertical = true") {
    val expectedAnswer = "-RECORD 0----\n" +
                         " key   | 1   \n" +
                         " value | 1   \n" +
                         "only showing top 1 row\n"
    assert(testData.select($"*").showString(1, vertical = true) === expectedAnswer)
  }

  test("SPARK-23023 Cast rows to strings in showString") {
    val df1 = Seq(Seq(1, 2, 3, 4)).toDF("a")
    assert(df1.showString(10) ===
      s"""+------------+
         ||           a|
         |+------------+
         ||[1, 2, 3, 4]|
         |+------------+
         |""".stripMargin)
    val df2 = Seq(Map(1 -> "a", 2 -> "b")).toDF("a")
    assert(df2.showString(10) ===
      s"""+----------------+
         ||               a|
         |+----------------+
         ||{1 -> a, 2 -> b}|
         |+----------------+
         |""".stripMargin)
    val df3 = Seq(((1, "a"), 0), ((2, "b"), 0)).toDF("a", "b")
    assert(df3.showString(10) ===
      s"""+------+---+
         ||     a|  b|
         |+------+---+
         ||{1, a}|  0|
         ||{2, b}|  0|
         |+------+---+
         |""".stripMargin)
  }

  test("SPARK-7327 show with empty dataFrame") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |""".stripMargin
    assert(testData.select($"*").filter($"key" < 0).showString(1) === expectedAnswer)
  }

  test("SPARK-7327 show with empty dataFrame, vertical = true") {
    assert(testData.select($"*").filter($"key" < 0).showString(1, vertical = true) === "(0 rows)\n")
  }

  test("SPARK-18350 show with session local timezone") {
    val d = Date.valueOf("2016-12-01")
    val ts = Timestamp.valueOf("2016-12-01 00:00:00")
    val df = Seq((d, ts)).toDF("d", "ts")
    val expectedAnswer = """+----------+-------------------+
                           ||d         |ts                 |
                           |+----------+-------------------+
                           ||2016-12-01|2016-12-01 00:00:00|
                           |+----------+-------------------+
                           |""".stripMargin
    assert(df.showString(1, truncate = 0) === expectedAnswer)

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {

      val expectedAnswer = """+----------+-------------------+
                             ||d         |ts                 |
                             |+----------+-------------------+
                             ||2016-12-01|2016-12-01 08:00:00|
                             |+----------+-------------------+
                             |""".stripMargin
      assert(df.showString(1, truncate = 0) === expectedAnswer)
    }
  }

  test("SPARK-18350 show with session local timezone, vertical = true") {
    val d = Date.valueOf("2016-12-01")
    val ts = Timestamp.valueOf("2016-12-01 00:00:00")
    val df = Seq((d, ts)).toDF("d", "ts")
    val expectedAnswer = "-RECORD 0------------------\n" +
                         " d   | 2016-12-01          \n" +
                         " ts  | 2016-12-01 00:00:00 \n"
    assert(df.showString(1, truncate = 0, vertical = true) === expectedAnswer)

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {

      val expectedAnswer = "-RECORD 0------------------\n" +
                           " d   | 2016-12-01          \n" +
                           " ts  | 2016-12-01 08:00:00 \n"
      assert(df.showString(1, truncate = 0, vertical = true) === expectedAnswer)
    }
  }

  test("createDataFrame(RDD[Row], StructType) should convert UDTs (SPARK-6672)") {
    val rowRDD = sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0))))
    val schema = StructType(Array(StructField("point", new ExamplePointUDT(), false)))
    val df = spark.createDataFrame(rowRDD, schema)
    df.rdd.collect()
  }

  test("SPARK-6899: type should match when using codegen") {
    checkAnswer(decimalData.agg(avg("a")), Row(new java.math.BigDecimal(2)))
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    if (!conf.ansiEnabled) {
      assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    }
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
    assert(complexData.filter(complexData("a")(complexData("s")("key")) === 1).count() == 1)
  }

  test("SPARK-7551: support backticks for DataFrame attribute resolution") {
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      val df = spark.read.json(Seq("""{"a.b": {"c": {"d..e": {"f": 1}}}}""").toDS())
      checkAnswer(
        df.select(df("`a.b`.c.`d..e`.`f`")),
        Row(1)
      )

      val df2 = spark.read.json(Seq("""{"a  b": {"c": {"d  e": {"f": 1}}}}""").toDS())
      checkAnswer(
        df2.select(df2("`a  b`.c.d  e.f")),
        Row(1)
      )

      def checkError(testFun: => Unit): Unit = {
        val e = intercept[org.apache.spark.sql.AnalysisException] {
          testFun
        }
        assert(e.getMessage.contains("syntax error in attribute name:"))
      }

      checkError(df("`abc.`c`"))
      checkError(df("`abc`..d"))
      checkError(df("`a`.b."))
      checkError(df("`a.b`.c.`d"))
    }
  }

  test("SPARK-7324 dropDuplicates") {
    val testData = sparkContext.parallelize(
      (2, 1, 2) :: (1, 1, 1) ::
      (1, 2, 1) :: (2, 1, 2) ::
      (2, 2, 2) :: (2, 2, 1) ::
      (2, 1, 1) :: (1, 1, 2) ::
      (1, 2, 2) :: (1, 2, 1) :: Nil).toDF("key", "value1", "value2")

    checkAnswer(
      testData.dropDuplicates(),
      Seq(Row(2, 1, 2), Row(1, 1, 1), Row(1, 2, 1),
        Row(2, 2, 2), Row(2, 1, 1), Row(2, 2, 1),
        Row(1, 1, 2), Row(1, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("key", "value1")),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("value1", "value2")),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("key")),
      Seq(Row(2, 1, 2), Row(1, 1, 1)))

    checkAnswer(
      testData.dropDuplicates(Seq("value1")),
      Seq(Row(2, 1, 2), Row(1, 2, 1)))

    checkAnswer(
      testData.dropDuplicates(Seq("value2")),
      Seq(Row(2, 1, 2), Row(1, 1, 1)))

    checkAnswer(
      testData.dropDuplicates("key", "value1"),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))
  }

  test("SPARK-8621: support empty string column name") {
    val df = Seq(Tuple1(1)).toDF("").as("t")
    // We should allow empty string as column name
    df.col("")
    df.col("t.``")
  }

  test("SPARK-8797: sort by float column containing NaN should not crash") {
    val inputData = Seq.fill(10)(Tuple1(Float.NaN)) ++ (1 to 1000).map(x => Tuple1(x.toFloat))
    val df = Random.shuffle(inputData).toDF("a")
    df.orderBy("a").collect()
  }

  test("SPARK-8797: sort by double column containing NaN should not crash") {
    val inputData = Seq.fill(10)(Tuple1(Double.NaN)) ++ (1 to 1000).map(x => Tuple1(x.toDouble))
    val df = Random.shuffle(inputData).toDF("a")
    df.orderBy("a").collect()
  }

  test("NaN is greater than all other non-NaN numeric values") {
    val maxDouble = Seq(Double.NaN, Double.PositiveInfinity, Double.MaxValue)
      .map(Tuple1.apply).toDF("a").selectExpr("max(a)").first()
    assert(java.lang.Double.isNaN(maxDouble.getDouble(0)))
    val maxFloat = Seq(Float.NaN, Float.PositiveInfinity, Float.MaxValue)
      .map(Tuple1.apply).toDF("a").selectExpr("max(a)").first()
    assert(java.lang.Float.isNaN(maxFloat.getFloat(0)))
  }

  test("SPARK-8072: Better Exception for Duplicate Columns") {
    // only one duplicate column present
    val e = intercept[org.apache.spark.sql.AnalysisException] {
      Seq((1, 2, 3), (2, 3, 4), (3, 4, 5)).toDF("column1", "column2", "column1")
        .write.format("parquet").save("temp")
    }
    assert(e.getMessage.contains("Found duplicate column(s) when inserting into"))
    assert(e.getMessage.contains("column1"))
    assert(!e.getMessage.contains("column2"))

    // multiple duplicate columns present
    val f = intercept[org.apache.spark.sql.AnalysisException] {
      Seq((1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7))
        .toDF("column1", "column2", "column3", "column1", "column3")
        .write.format("json").save("temp")
    }
    assert(f.getMessage.contains("Found duplicate column(s) when inserting into"))
    assert(f.getMessage.contains("column1"))
    assert(f.getMessage.contains("column3"))
    assert(!f.getMessage.contains("column2"))
  }

  test("SPARK-6941: Better error message for inserting into RDD-based Table") {
    withTempDir { dir =>
      withTempView("parquet_base", "json_base", "rdd_base", "indirect_ds", "one_row") {
        val tempParquetFile = new File(dir, "tmp_parquet")
        val tempJsonFile = new File(dir, "tmp_json")

        val df = Seq(Tuple1(1)).toDF()
        val insertion = Seq(Tuple1(2)).toDF("col")

        // pass case: parquet table (HadoopFsRelation)
        df.write.mode(SaveMode.Overwrite).parquet(tempParquetFile.getCanonicalPath)
        val pdf = spark.read.parquet(tempParquetFile.getCanonicalPath)
        pdf.createOrReplaceTempView("parquet_base")

        insertion.write.insertInto("parquet_base")

        // pass case: json table (InsertableRelation)
        df.write.mode(SaveMode.Overwrite).json(tempJsonFile.getCanonicalPath)
        val jdf = spark.read.json(tempJsonFile.getCanonicalPath)
        jdf.createOrReplaceTempView("json_base")
        insertion.write.mode(SaveMode.Overwrite).insertInto("json_base")

        // error cases: insert into an RDD
        df.createOrReplaceTempView("rdd_base")
        val e1 = intercept[AnalysisException] {
          insertion.write.insertInto("rdd_base")
        }
        assert(e1.getMessage.contains("Inserting into an RDD-based table is not allowed."))

        // error case: insert into a logical plan that is not a LeafNode
        val indirectDS = pdf.select("_1").filter($"_1" > 5)
        indirectDS.createOrReplaceTempView("indirect_ds")
        val e2 = intercept[AnalysisException] {
          insertion.write.insertInto("indirect_ds")
        }
        assert(e2.getMessage.contains("Inserting into an RDD-based table is not allowed."))

        // error case: insert into an OneRowRelation
        Dataset.ofRows(spark, OneRowRelation()).createOrReplaceTempView("one_row")
        val e3 = intercept[AnalysisException] {
          insertion.write.insertInto("one_row")
        }
        assert(e3.getMessage.contains("Inserting into an RDD-based table is not allowed."))
      }
    }
  }

  test("SPARK-8608: call `show` on local DataFrame with random columns should return same value") {
    val df = testData.select(rand(33))
    assert(df.showString(5) == df.showString(5))

    // We will reuse the same Expression object for LocalRelation.
    val df1 = (1 to 10).map(Tuple1.apply).toDF().select(rand(33))
    assert(df1.showString(5) == df1.showString(5))
  }

  test("SPARK-8609: local DataFrame with random columns should return same value after sort") {
    checkAnswer(testData.sort(rand(33)), testData.sort(rand(33)))

    // We will reuse the same Expression object for LocalRelation.
    val df = (1 to 10).map(Tuple1.apply).toDF()
    checkAnswer(df.sort(rand(33)), df.sort(rand(33)))
  }

  test("SPARK-9083: sort with non-deterministic expressions") {
    val seed = 33
    val df = (1 to 100).map(Tuple1.apply).toDF("i").repartition(1)
    val random = new XORShiftRandom(seed)
    val expected = (1 to 100).map(_ -> random.nextDouble()).sortBy(_._2).map(_._1)
    val actual = df.sort(rand(seed)).collect().map(_.getInt(0))
    assert(expected === actual)
  }

  test("Sorting columns are not in Filter and Project") {
    checkAnswer(
      upperCaseData.filter($"N" > 1).select("N").filter($"N" < 6).orderBy($"L".asc),
      Row(2) :: Row(3) :: Row(4) :: Row(5) :: Nil)
  }

  test("SPARK-9323: DataFrame.orderBy should support nested column name") {
    val df = spark.read.json(Seq("""{"a": {"b": 1}}""").toDS())
    checkAnswer(df.orderBy("a.b"), Row(Row(1)))
  }

  test("SPARK-9950: correctly analyze grouping/aggregating on struct fields") {
    val df = Seq(("x", (1, 1)), ("y", (2, 2))).toDF("a", "b")
    checkAnswer(df.groupBy("b._1").agg(sum("b._2")), Row(1, 1) :: Row(2, 2) :: Nil)
  }

  test("SPARK-10093: Avoid transformations on executors") {
    val df = Seq((1, 1)).toDF("a", "b")
    df.where($"a" === 1)
      .select($"a", $"b", struct($"b"))
      .orderBy("a")
      .select(struct($"b"))
      .collect()
  }

  test("SPARK-10185: Read multiple Hadoop Filesystem paths and paths with a comma in it") {
    withTempDir { dir =>
      val df1 = Seq((1, 22)).toDF("a", "b")
      val dir1 = new File(dir, "dir,1").getCanonicalPath
      df1.write.format("json").save(dir1)

      val df2 = Seq((2, 23)).toDF("a", "b")
      val dir2 = new File(dir, "dir2").getCanonicalPath
      df2.write.format("json").save(dir2)

      checkAnswer(spark.read.format("json").load(dir1, dir2),
        Row(1, 22) :: Row(2, 23) :: Nil)

      checkAnswer(spark.read.format("json").load(dir1),
        Row(1, 22) :: Nil)
    }
  }

  test("Alias uses internally generated names 'aggOrder' and 'havingCondition'") {
    val df = Seq(1 -> 2).toDF("i", "j")
    val query1 = df.groupBy("i")
      .agg(max("j").as("aggOrder"))
      .orderBy(sum("j"))
    checkAnswer(query1, Row(1, 2))

    // In the plan, there are two attributes having the same name 'havingCondition'
    // One is a user-provided alias name; another is an internally generated one.
    val query2 = df.groupBy("i")
      .agg(max("j").as("havingCondition"))
      .where(sum("j") > 0)
      .orderBy($"havingCondition".asc)
    checkAnswer(query2, Row(1, 2))
  }

  test("SPARK-10316: respect non-deterministic expressions in PhysicalOperation") {
    withTempDir { dir =>
      (1 to 10).toDF("id").write.mode(SaveMode.Overwrite).json(dir.getCanonicalPath)
      val input = spark.read.json(dir.getCanonicalPath)

      val df = input.select($"id", rand(0).as("r"))
      df.as("a").join(df.filter($"r" < 0.5).as("b"), $"a.id" === $"b.id").collect().foreach { row =>
        assert(row.getDouble(1) - row.getDouble(3) === 0.0 +- 0.001)
      }
    }
  }

  test("SPARK-10743: keep the name of expression if possible when do cast") {
    val df = (1 to 10).map(Tuple1.apply).toDF("i").as("src")
    assert(df.select($"src.i".cast(StringType)).columns.head === "i")
    assert(df.select($"src.i".cast(StringType).cast(IntegerType)).columns.head === "i")
  }

  test("SPARK-11301: fix case sensitivity for filter on partitioned columns") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { path =>
        Seq(2012 -> "a").toDF("year", "val").write.partitionBy("year").parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        checkAnswer(df.filter($"yEAr" > 2000).select($"val"), Row("a"))
      }
    }
  }

  /**
   * Verifies that there is no Exchange between the Aggregations for `df`
   */
  private def verifyNonExchangingAgg(df: DataFrame) = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case agg: HashAggregateExec =>
        atFirstAgg = !atFirstAgg
      case _ =>
        if (atFirstAgg) {
          fail("Should not have operators between the two aggregations")
        }
    }
  }

  /**
   * Verifies that there is an Exchange between the Aggregations for `df`
   */
  private def verifyExchangingAgg(df: DataFrame) = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case agg: HashAggregateExec =>
        if (atFirstAgg) {
          fail("Should not have back to back Aggregates")
        }
        atFirstAgg = true
      case e: ShuffleExchangeExec => atFirstAgg = false
      case _ =>
    }
  }

  test("distributeBy and localSort") {
    val original = testData.repartition(1)
    assert(original.rdd.partitions.length == 1)
    val df = original.repartition(5, $"key")
    assert(df.rdd.partitions.length == 5)
    checkAnswer(original.select(), df.select())

    val df2 = original.repartition(10, $"key")
    assert(df2.rdd.partitions.length == 10)
    checkAnswer(original.select(), df2.select())

    // Group by the column we are distributed by. This should generate a plan with no exchange
    // between the aggregates
    val df3 = testData.repartition($"key").groupBy("key").count()
    verifyNonExchangingAgg(df3)
    verifyNonExchangingAgg(testData.repartition($"key", $"value")
      .groupBy("key", "value").count())

    // Grouping by just the first distributeBy expr, need to exchange.
    verifyExchangingAgg(testData.repartition($"key", $"value")
      .groupBy("key").count())

    val data = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData2(i % 10, i))).toDF()

    // Distribute and order by.
    val df4 = data.repartition(5, $"a").sortWithinPartitions($"b".desc)
    // Walk each partition and verify that it is sorted descending and does not contain all
    // the values.
    df4.rdd.foreachPartition { p =>
      // Skip empty partition
      if (p.hasNext) {
        var previousValue: Int = -1
        var allSequential: Boolean = true
        p.foreach { r =>
          val v: Int = r.getInt(1)
          if (previousValue != -1) {
            if (previousValue < v) throw new SparkException("Partition is not ordered.")
            if (v + 1 != previousValue) allSequential = false
          }
          previousValue = v
        }
        if (allSequential) throw new SparkException("Partition should not be globally ordered")
      }
    }

    // Distribute and order by with multiple order bys
    val df5 = data.repartition(2, $"a").sortWithinPartitions($"b".asc, $"a".asc)
    // Walk each partition and verify that it is sorted ascending
    df5.rdd.foreachPartition { p =>
      var previousValue: Int = -1
      var allSequential: Boolean = true
      p.foreach { r =>
        val v: Int = r.getInt(1)
        if (previousValue != -1) {
          if (previousValue > v) throw new SparkException("Partition is not ordered.")
          if (v - 1 != previousValue) allSequential = false
        }
        previousValue = v
      }
      if (allSequential) throw new SparkException("Partition should not be all sequential")
    }

    // Distribute into one partition and order by. This partition should contain all the values.
    val df6 = data.repartition(1, $"a").sortWithinPartitions("b")
    // Walk each partition and verify that it is sorted ascending and not globally sorted.
    df6.rdd.foreachPartition { p =>
      var previousValue: Int = -1
      var allSequential: Boolean = true
      p.foreach { r =>
        val v: Int = r.getInt(1)
        if (previousValue != -1) {
          if (previousValue > v) throw new SparkException("Partition is not ordered.")
          if (v - 1 != previousValue) allSequential = false
        }
        previousValue = v
      }
      if (!allSequential) throw new SparkException("Partition should contain all sequential values")
    }
  }

  test("fix case sensitivity of partition by") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { path =>
        val p = path.getAbsolutePath
        Seq(2012 -> "a").toDF("year", "val").write.partitionBy("yEAr").parquet(p)
        checkAnswer(spark.read.parquet(p).select("YeaR"), Row(2012))
      }
    }
  }

  // This test case is to verify a bug when making a new instance of LogicalRDD.
  test("SPARK-11633: LogicalRDD throws TreeNode Exception: Failed to Copy Node") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val rdd = sparkContext.makeRDD(Seq(Row(1, 3), Row(2, 1)))
      val df = spark.createDataFrame(
        rdd,
        new StructType().add("f1", IntegerType).add("f2", IntegerType))
        .select($"F1", $"f2".as("f2"))
      val df1 = df.as("a")
      val df2 = df.as("b")
      checkAnswer(df1.join(df2, $"a.f2" === $"b.f2"), Row(1, 3, 1, 3) :: Row(2, 1, 2, 1) :: Nil)
    }
  }

  test("SPARK-39748: build the stats for LogicalRDD based on originLogicalPlan") {
    def buildExpectedColumnStats(attrs: Seq[Attribute]): AttributeMap[ColumnStat] = {
      AttributeMap(
        attrs.map {
          case attr if attr.dataType == BooleanType =>
            attr -> ColumnStat(
              distinctCount = Some(2),
              min = Some(false),
              max = Some(true),
              nullCount = Some(0),
              avgLen = Some(1),
              maxLen = Some(1))

          case attr if attr.dataType == ByteType =>
            attr -> ColumnStat(
              distinctCount = Some(2),
              min = Some(1),
              max = Some(2),
              nullCount = Some(0),
              avgLen = Some(1),
              maxLen = Some(1))

          case attr => attr -> ColumnStat()
        }
      )
    }

    val outputList = Seq(
      AttributeReference("cbool", BooleanType)(),
      AttributeReference("cbyte", BooleanType)()
    )

    val expectedSize = 16
    val statsPlan = OutputListAwareStatsTestPlan(
      outputList = outputList,
      rowCount = 2,
      size = Some(expectedSize))

    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      val df = Dataset.ofRows(spark, statsPlan)

      val logicalRDD = LogicalRDD(
        df.logicalPlan.output, spark.sparkContext.emptyRDD, Some(df.queryExecution.analyzed),
        isStreaming = true)(spark)

      val stats = logicalRDD.computeStats()
      val expectedStats = Statistics(sizeInBytes = expectedSize, rowCount = Some(2),
        attributeStats = buildExpectedColumnStats(logicalRDD.output))
      assert(stats === expectedStats)

      // This method re-issues expression IDs for all outputs. We expect column stats to be
      // reflected as well.
      val newLogicalRDD = logicalRDD.newInstance()
      val newStats = newLogicalRDD.computeStats()
      // LogicalRDD.newInstance adds projection to originLogicalPlan, which triggers estimation
      // on sizeInBytes. We don't intend to check the estimated value.
      val newExpectedStats = Statistics(sizeInBytes = newStats.sizeInBytes, rowCount = Some(2),
        attributeStats = buildExpectedColumnStats(newLogicalRDD.output))
      assert(newStats === newExpectedStats)
    }
  }

  test("SPARK-10656: completely support special chars") {
    val df = Seq(1 -> "a").toDF("i_$.a", "d^'a.")
    checkAnswer(df.select(df("*")), Row(1, "a"))
    checkAnswer(df.withColumnRenamed("d^'a.", "a"), Row(1, "a"))
  }

  test("SPARK-11725: correctly handle null inputs for ScalaUDF") {
    val df = sparkContext.parallelize(Seq(
      java.lang.Integer.valueOf(22) -> "John",
      null.asInstanceOf[java.lang.Integer] -> "Lucy")).toDF("age", "name")

    // passing null into the UDF that could handle it
    val boxedUDF = udf[java.lang.Integer, java.lang.Integer] {
      (i: java.lang.Integer) => if (i == null) -10 else null
    }
    checkAnswer(df.select(boxedUDF($"age")), Row(null) :: Row(-10) :: Nil)

    spark.udf.register("boxedUDF",
      (i: java.lang.Integer) => (if (i == null) -10 else null): java.lang.Integer)
    checkAnswer(sql("select boxedUDF(null), boxedUDF(-1)"), Row(-10, null) :: Nil)

    val primitiveUDF = udf((i: Int) => i * 2)
    checkAnswer(df.select(primitiveUDF($"age")), Row(44) :: Row(null) :: Nil)
  }

  test("SPARK-12398 truncated toString") {
    val df1 = Seq((1L, "row1")).toDF("id", "name")
    assert(df1.toString() === "[id: bigint, name: string]")

    val df2 = Seq((1L, "c2", false)).toDF("c1", "c2", "c3")
    assert(df2.toString === "[c1: bigint, c2: string ... 1 more field]")

    val df3 = Seq((1L, "c2", false, 10)).toDF("c1", "c2", "c3", "c4")
    assert(df3.toString === "[c1: bigint, c2: string ... 2 more fields]")

    val df4 = Seq((1L, Tuple2(1L, "val"))).toDF("c1", "c2")
    assert(df4.toString === "[c1: bigint, c2: struct<_1: bigint, _2: string>]")

    val df5 = Seq((1L, Tuple2(1L, "val"), 20.0)).toDF("c1", "c2", "c3")
    assert(df5.toString === "[c1: bigint, c2: struct<_1: bigint, _2: string> ... 1 more field]")

    val df6 = Seq((1L, Tuple2(1L, "val"), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(df6.toString === "[c1: bigint, c2: struct<_1: bigint, _2: string> ... 2 more fields]")

    val df7 = Seq((1L, Tuple3(1L, "val", 2), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(
      df7.toString ===
        "[c1: bigint, c2: struct<_1: bigint, _2: string ... 1 more field> ... 2 more fields]")

    val df8 = Seq((1L, Tuple7(1L, "val", 2, 3, 4, 5, 6), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(
      df8.toString ===
        "[c1: bigint, c2: struct<_1: bigint, _2: string ... 5 more fields> ... 2 more fields]")

    val df9 =
      Seq((1L, Tuple4(1L, Tuple4(1L, 2L, 3L, 4L), 2L, 3L), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(
      df9.toString ===
        "[c1: bigint, c2: struct<_1: bigint," +
          " _2: struct<_1: bigint," +
          " _2: bigint ... 2 more fields> ... 2 more fields> ... 2 more fields]")

  }

  test("reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(
        collect(join.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => true }.size === 1)
      assert(
        collect(join.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => true }.size == 1)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case e: BroadcastExchangeExec => true }.size === 1)
      assert(
        collect(join2.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size == 4)
    }
  }

  test("sameResult() on aggregate") {
    val df = spark.range(100)
    val agg1 = df.groupBy().count()
    val agg2 = df.groupBy().count()
    // two aggregates with different ExprId within them should have same result
    assert(agg1.queryExecution.executedPlan.sameResult(agg2.queryExecution.executedPlan))
    val agg3 = df.groupBy().sum()
    assert(!agg1.queryExecution.executedPlan.sameResult(agg3.queryExecution.executedPlan))
    val df2 = spark.range(101)
    val agg4 = df2.groupBy().count()
    assert(!agg1.queryExecution.executedPlan.sameResult(agg4.queryExecution.executedPlan))
  }

  test("SPARK-12512: support `.` in column name for withColumn()") {
    val df = Seq("a" -> "b").toDF("col.a", "col.b")
    checkAnswer(df.select(df("*")), Row("a", "b"))
    checkAnswer(df.withColumn("col.a", lit("c")), Row("c", "b"))
    checkAnswer(df.withColumn("col.c", lit("c")), Row("a", "b", "c"))
  }

  test("SPARK-12841: cast in filter") {
    checkAnswer(
      Seq(1 -> "a").toDF("i", "j").filter($"i".cast(StringType) === "1"),
      Row(1, "a"))
  }

  test("SPARK-12982: Add table name validation in temp table registration") {
    val df = Seq("foo", "bar").map(Tuple1.apply).toDF("col")
    // invalid table names
    Seq("11111", "t~", "#$@sum", "table!#").foreach { name =>
      withTempView(name) {
        val m = intercept[AnalysisException](df.createOrReplaceTempView(name)).getMessage
        assert(m.contains(s"Invalid view name: $name"))
      }
    }

    // valid table names
    Seq("table1", "`11111`", "`t~`", "`#$@sum`", "`table!#`").foreach { name =>
      withTempView(name) {
        df.createOrReplaceTempView(name)
      }
    }
  }

  test("assertAnalyzed shouldn't replace original stack trace") {
    val e = intercept[AnalysisException] {
      spark.range(1).select($"id" as "a", $"id" as "b").groupBy("a").agg($"b")
    }

    assert(e.getStackTrace.head.getClassName != classOf[QueryExecution].getName)
  }

  test("SPARK-13774: Check error message for non existent path without globbed paths") {
    val uuid = UUID.randomUUID().toString
    val baseDir = Utils.createTempDir()
    try {
      val e = intercept[AnalysisException] {
        spark.read.format("csv").load(
          new File(baseDir, "file").getAbsolutePath,
          new File(baseDir, "file2").getAbsolutePath,
          new File(uuid, "file3").getAbsolutePath,
          uuid).rdd
      }
      assert(e.getMessage.startsWith("Path does not exist"))
    } finally {

    }

   }

  test("SPARK-13774: Check error message for not existent globbed paths") {
    // Non-existent initial path component:
    val nonExistentBasePath = "/" + UUID.randomUUID().toString
    assert(!new File(nonExistentBasePath).exists())
    val e = intercept[AnalysisException] {
      spark.read.format("text").load(s"$nonExistentBasePath/*")
    }
    assert(e.getMessage.startsWith("Path does not exist"))

    // Existent initial path component, but no matching files:
    val baseDir = Utils.createTempDir()
    val childDir = Utils.createTempDir(baseDir.getAbsolutePath)
    assert(childDir.exists())
    try {
      val e1 = intercept[AnalysisException] {
        spark.read.json(s"${baseDir.getAbsolutePath}/*/*-xyz.json").rdd
      }
      assert(e1.getMessage.startsWith("Path does not exist"))
    } finally {
      Utils.deleteRecursively(baseDir)
    }
  }

  test("SPARK-15230: distinct() does not handle column name with dot properly") {
    val df = Seq(1, 1, 2).toDF("column.with.dot")
    checkAnswer(df.distinct(), Row(1) :: Row(2) :: Nil)
  }

  test("SPARK-16181: outer join with isNull filter") {
    val left = Seq("x").toDF("col")
    val right = Seq("y").toDF("col").withColumn("new", lit(true))
    val joined = left.join(right, left("col") === right("col"), "left_outer")

    checkAnswer(joined, Row("x", null, null))
    checkAnswer(joined.filter($"new".isNull), Row("x", null, null))
  }

  test("SPARK-16664: persist with more than 200 columns") {
    val size = 201L
    val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(Seq.range(0, size))))
    val schemas = List.range(0, size).map(a => StructField("name" + a, LongType, true))
    val df = spark.createDataFrame(rdd, StructType(schemas))
    assert(df.persist.take(1).apply(0).toSeq(100).asInstanceOf[Long] == 100)
  }

  test("SPARK-17409: Do Not Optimize Query in CTAS (Data source tables) More Than Once") {
    withTable("bar") {
      withTempView("foo") {
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          sql("select 0 as id").createOrReplaceTempView("foo")
          val df = sql("select * from foo group by id")
          // If we optimize the query in CTAS more than once, the following saveAsTable will fail
          // with the error: `GROUP BY position 0 is not in select list (valid range is [1, 1])`
          df.write.mode("overwrite").saveAsTable("bar")
          checkAnswer(spark.table("bar"), Row(0) :: Nil)
          val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier("bar"))
          assert(tableMetadata.provider == Some("json"),
            "the expected table is a data source table using json")
        }
      }
    }
  }

  test("copy results for sampling with replacement") {
    val df = Seq((1, 0), (2, 0), (3, 0)).toDF("a", "b")
    val sampleDf = df.sample(true, 2.00)
    val d = sampleDf.withColumn("c", monotonically_increasing_id).select($"c").collect
    assert(d.size == d.distinct.size)
  }

  private def verifyNullabilityInFilterExec(
      df: DataFrame,
      expr: String,
      expectedNonNullableColumns: Seq[String]): Unit = {
    val dfWithFilter = df.where(s"isnotnull($expr)").selectExpr(expr)
    dfWithFilter.queryExecution.executedPlan.collect {
      // When the child expression in isnotnull is null-intolerant (i.e. any null input will
      // result in null output), the involved columns are converted to not nullable;
      // otherwise, no change should be made.
      case e: FilterExec =>
        assert(e.output.forall { o =>
          if (expectedNonNullableColumns.contains(o.name)) !o.nullable else o.nullable
        })
    }
  }

  test("SPARK-17957: no change on nullability in FilterExec output") {
    val df = sparkContext.parallelize(Seq(
      null.asInstanceOf[java.lang.Integer] -> java.lang.Integer.valueOf(3),
      java.lang.Integer.valueOf(1) -> null.asInstanceOf[java.lang.Integer],
      java.lang.Integer.valueOf(2) -> java.lang.Integer.valueOf(4))).toDF()

    verifyNullabilityInFilterExec(df,
      expr = "Rand()", expectedNonNullableColumns = Seq.empty[String])
    verifyNullabilityInFilterExec(df,
      expr = "coalesce(_1, _2)", expectedNonNullableColumns = Seq.empty[String])
    verifyNullabilityInFilterExec(df,
      expr = "coalesce(_1, 0) + Rand()", expectedNonNullableColumns = Seq.empty[String])
    verifyNullabilityInFilterExec(df,
      expr = "cast(coalesce(cast(coalesce(_1, _2) as double), 0.0) as int)",
      expectedNonNullableColumns = Seq.empty[String])
  }

  test("SPARK-17957: set nullability to false in FilterExec output") {
    val df = sparkContext.parallelize(Seq(
      null.asInstanceOf[java.lang.Integer] -> java.lang.Integer.valueOf(3),
      java.lang.Integer.valueOf(1) -> null.asInstanceOf[java.lang.Integer],
      java.lang.Integer.valueOf(2) -> java.lang.Integer.valueOf(4))).toDF()

    verifyNullabilityInFilterExec(df,
      expr = "_1 + _2 * 3", expectedNonNullableColumns = Seq("_1", "_2"))
    verifyNullabilityInFilterExec(df,
      expr = "_1 + _2", expectedNonNullableColumns = Seq("_1", "_2"))
    verifyNullabilityInFilterExec(df,
      expr = "_1", expectedNonNullableColumns = Seq("_1"))
    // `constructIsNotNullConstraints` infers the IsNotNull(_2) from IsNotNull(_2 + Rand())
    // Thus, we are able to set nullability of _2 to false.
    // If IsNotNull(_2) is not given from `constructIsNotNullConstraints`, the impl of
    // isNullIntolerant in `FilterExec` needs an update for more advanced inference.
    verifyNullabilityInFilterExec(df,
      expr = "_2 + Rand()", expectedNonNullableColumns = Seq("_2"))
    verifyNullabilityInFilterExec(df,
      expr = "_2 * 3 + coalesce(_1, 0)", expectedNonNullableColumns = Seq("_2"))
    verifyNullabilityInFilterExec(df,
      expr = "cast((_1 + _2) as boolean)", expectedNonNullableColumns = Seq("_1", "_2"))
  }

  test("SPARK-17897: Fixed IsNotNull Constraint Inference Rule") {
    val data = Seq[java.lang.Integer](1, null).toDF("key")
    checkAnswer(data.filter(!$"key".isNotNull), Row(null))
    checkAnswer(data.filter(!(- $"key").isNotNull), Row(null))
  }

  test("SPARK-17957: outer join + na.fill") {
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      val df1 = Seq((1, 2), (2, 3)).toDF("a", "b")
      val df2 = Seq((2, 5), (3, 4)).toDF("a", "c")
      val joinedDf = df1.join(df2, Seq("a"), "outer").na.fill(0)
      val df3 = Seq((3, 1)).toDF("a", "d")
      checkAnswer(joinedDf.join(df3, "a"), Row(3, 0, 4, 1))
    }
  }

  test("SPARK-18070 binary operator should not consider nullability when comparing input types") {
    val rows = Seq(Row(Seq(1), Seq(1)))
    val schema = new StructType()
      .add("array1", ArrayType(IntegerType))
      .add("array2", ArrayType(IntegerType, containsNull = false))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(rows), schema)
    assert(df.filter($"array1" === $"array2").count() == 1)
  }

  test("SPARK-17913: compare long and string type column may return confusing result") {
    val df = Seq(123L -> "123", 19157170390056973L -> "19157170390056971").toDF("i", "j")
    checkAnswer(df.select($"i" === $"j"), Row(true) :: Row(false) :: Nil)
  }

  test("SPARK-19691 Calculating percentile of decimal column fails with ClassCastException") {
    val df = spark.range(1).selectExpr("CAST(id as DECIMAL) as x").selectExpr("percentile(x, 0.5)")
    checkAnswer(df, Row(BigDecimal(0)) :: Nil)
  }

  test("SPARK-20359: catalyst outer join optimization should not throw npe") {
    val df1 = Seq("a", "b", "c").toDF("x")
      .withColumn("y", udf{ (x: String) => x.substring(0, 1) + "!" }.apply($"x"))
    val df2 = Seq("a", "b").toDF("x1")
    df1
      .join(df2, df1("x") === df2("x1"), "left_outer")
      .filter($"x1".isNotNull || !$"y".isin("a!"))
      .count
  }

  // The fix of SPARK-21720 avoid an exception regarding JVM code size limit
  // TODO: When we make a threshold of splitting statements (1024) configurable,
  // we will re-enable this with max threshold to cause an exception
  // See https://github.com/apache/spark/pull/18972/files#r150223463
  ignore("SPARK-19372: Filter can be executed w/o generated code due to JVM code size limit") {
    val N = 400
    val rows = Seq(Row.fromSeq(Seq.fill(N)("string")))
    val schema = StructType(Seq.tabulate(N)(i => StructField(s"_c$i", StringType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(rows), schema)

    val filter = (0 until N)
      .foldLeft(lit(false))((e, index) => e.or(df.col(df.columns(index)) =!= "string"))

    withSQLConf(SQLConf.CODEGEN_FALLBACK.key -> "true") {
      df.filter(filter).count()
    }

    withSQLConf(SQLConf.CODEGEN_FALLBACK.key -> "false") {
      val e = intercept[SparkException] {
        df.filter(filter).count()
      }.getMessage
      assert(e.contains("grows beyond 64 KiB"))
    }
  }

  test("SPARK-20897: cached self-join should not fail") {
    // force to plan sort merge join
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      val df = Seq(1 -> "a").toDF("i", "j")
      val df1 = df.as("t1")
      val df2 = df.as("t2")
      assert(df1.join(df2, $"t1.i" === $"t2.i").cache().count() == 1)
    }
  }

  test("order-by ordinal.") {
    checkAnswer(
      testData2.select(lit(7), $"a", $"b").orderBy(lit(1), lit(2), lit(3)),
      Seq(Row(7, 1, 1), Row(7, 1, 2), Row(7, 2, 1), Row(7, 2, 2), Row(7, 3, 1), Row(7, 3, 2)))
  }

  test("SPARK-22271: mean overflows and returns null for some decimal variables") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df.select($"DecimalCol" cast DecimalType(38, 33))
      .select(col("DecimalCol")).describe()
    val mean = result.select("DecimalCol").where($"summary" === "mean")
    assert(mean.collect().toSet === Set(Row("0.0345678900000000000000000000000000000")))
  }

  test("SPARK-22520: support code generation for large CaseWhen") {
    val N = 30
    var expr1 = when($"id" === lit(0), 0)
    var expr2 = when($"id" === lit(0), 10)
    (1 to N).foreach { i =>
      expr1 = expr1.when($"id" === lit(i), -i)
      expr2 = expr2.when($"id" === lit(i + 10), i)
    }
    val df = spark.range(1).select(expr1, expr2.otherwise(0))
    checkAnswer(df, Row(0, 10) :: Nil)
    assert(df.queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
  }

  test("SPARK-24165: CaseWhen/If - nullability of nested types") {
    val rows = new java.util.ArrayList[Row]()
    rows.add(Row(true, ("x", 1), Seq("x", "y"), Map(0 -> "x")))
    rows.add(Row(false, (null, 2), Seq(null, "z"), Map(0 -> null)))
    val schema = StructType(Seq(
      StructField("cond", BooleanType, true),
      StructField("s", StructType(Seq(
        StructField("val1", StringType, true),
        StructField("val2", IntegerType, false)
      )), false),
      StructField("a", ArrayType(StringType, true)),
      StructField("m", MapType(IntegerType, StringType, true))
    ))

    val sourceDF = spark.createDataFrame(rows, schema)

    def structWhenDF: DataFrame = sourceDF
      .select(when($"cond",
        struct(lit("a").as("val1"), lit(10).as("val2"))).otherwise($"s") as "res")
      .select($"res".getField("val1"))
    def arrayWhenDF: DataFrame = sourceDF
      .select(when($"cond", array(lit("a"), lit("b"))).otherwise($"a") as "res")
      .select($"res".getItem(0))
    def mapWhenDF: DataFrame = sourceDF
      .select(when($"cond", map(lit(0), lit("a"))).otherwise($"m") as "res")
      .select($"res".getItem(0))

    def structIfDF: DataFrame = sourceDF
      .select(expr("if(cond, struct('a' as val1, 10 as val2), s)") as "res")
      .select($"res".getField("val1"))
    def arrayIfDF: DataFrame = sourceDF
      .select(expr("if(cond, array('a', 'b'), a)") as "res")
      .select($"res".getItem(0))
    def mapIfDF: DataFrame = sourceDF
      .select(expr("if(cond, map(0, 'a'), m)") as "res")
      .select($"res".getItem(0))

    def checkResult(): Unit = {
      checkAnswer(structWhenDF, Seq(Row("a"), Row(null)))
      checkAnswer(arrayWhenDF, Seq(Row("a"), Row(null)))
      checkAnswer(mapWhenDF, Seq(Row("a"), Row(null)))
      checkAnswer(structIfDF, Seq(Row("a"), Row(null)))
      checkAnswer(arrayIfDF, Seq(Row("a"), Row(null)))
      checkAnswer(mapIfDF, Seq(Row("a"), Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    checkResult()
    // Test with cached relation, the Project will be evaluated with codegen
    sourceDF.cache()
    checkResult()
  }

  test("Uuid expressions should produce same results at retries in the same DataFrame") {
    val df = spark.range(1).select($"id", new Column(Uuid()))
    checkAnswer(df, df.collect())
  }

  test("SPARK-24313: access map with binary keys") {
    val mapWithBinaryKey = map(lit(Array[Byte](1.toByte)), lit(1))
    checkAnswer(spark.range(1).select(mapWithBinaryKey.getItem(Array[Byte](1.toByte))), Row(1))
  }

  test("SPARK-24781: Using a reference from Dataset in Filter/Sort") {
    val df = Seq(("test1", 0), ("test2", 1)).toDF("name", "id")
    val filter1 = df.select(df("name")).filter(df("id") === 0)
    val filter2 = df.select(col("name")).filter(col("id") === 0)
    checkAnswer(filter1, filter2.collect())

    val sort1 = df.select(df("name")).orderBy(df("id"))
    val sort2 = df.select(col("name")).orderBy(col("id"))
    checkAnswer(sort1, sort2.collect())
  }

  test("SPARK-24781: Using a reference not in aggregation in Filter/Sort") {
     withSQLConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS.key -> "false") {
      val df = Seq(("test1", 0), ("test2", 1)).toDF("name", "id")

      val aggPlusSort1 = df.groupBy(df("name")).agg(count(df("name"))).orderBy(df("name"))
      val aggPlusSort2 = df.groupBy(col("name")).agg(count(col("name"))).orderBy(col("name"))
      checkAnswer(aggPlusSort1, aggPlusSort2.collect())

      val aggPlusFilter1 =
        df.groupBy(df("name")).agg(count(df("name"))).filter(df("name") === "test1")
      val aggPlusFilter2 =
        df.groupBy(col("name")).agg(count(col("name"))).filter(col("name") === "test1")
      checkAnswer(aggPlusFilter1, aggPlusFilter2.collect())
    }
  }

  test("SPARK-25159: json schema inference should only trigger one job") {
    withTempPath { path =>
      // This test is to prove that the `JsonInferSchema` does not use `RDD#toLocalIterator` which
      // triggers one Spark job per RDD partition.
      Seq(1 -> "a", 2 -> "b").toDF("i", "p")
        // The data set has 2 partitions, so Spark will write at least 2 json files.
        // Use a non-splittable compression (gzip), to make sure the json scan RDD has at least 2
        // partitions.
        .write.partitionBy("p").option("compression", "gzip").json(path.getCanonicalPath)

      val numJobs = new AtomicLong(0)
      sparkContext.addSparkListener(new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          numJobs.incrementAndGet()
        }
      })

      val df = spark.read.json(path.getCanonicalPath)
      assert(df.columns === Array("i", "p"))
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(numJobs.get() == 1L)
    }
  }

  test("SPARK-25402 Null handling in BooleanSimplification") {
    val schema = StructType.fromDDL("a boolean, b int")
    val rows = Seq(Row(null, 1))

    val rdd = sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, schema)

    checkAnswer(df.where("(NOT a) OR a"), Seq.empty)
  }

  test("SPARK-25714 Null handling in BooleanSimplification") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      val df = Seq(("abc", 1), (null, 3)).toDF("col1", "col2")
      checkAnswer(
        df.filter("col1 = 'abc' OR (col1 != 'abc' AND col2 == 3)"),
        Row ("abc", 1))
    }
  }

  test("SPARK-25816 ResolveReferences works with nested extractors") {
    val df = Seq((1, Map(1 -> "a")), (2, Map(2 -> "b"))).toDF("key", "map")
    val swappedDf = df.select($"key".as("map"), $"map".as("key"))

    checkAnswer(swappedDf.filter($"key"($"map") > "a"), Row(2, Map(2 -> "b")))
  }

  test("SPARK-26057: attribute deduplication on already analyzed plans") {
    withTempView("a", "b", "v") {
      val df1 = Seq(("1-1", 6)).toDF("id", "n")
      df1.createOrReplaceTempView("a")
      val df3 = Seq("1-1").toDF("id")
      df3.createOrReplaceTempView("b")
      spark.sql(
        """
          |SELECT a.id, n as m
          |FROM a
          |WHERE EXISTS(
          |  SELECT 1
          |  FROM b
          |  WHERE b.id = a.id)
        """.stripMargin).createOrReplaceTempView("v")
      val res = spark.sql(
        """
          |SELECT a.id, n, m
          |  FROM a
          |  LEFT OUTER JOIN v ON v.id = a.id
        """.stripMargin)
      checkAnswer(res, Row("1-1", 6, 6))
    }
  }

  test("SPARK-27671: Fix analysis exception when casting null in nested field in struct") {
    val df = sql("SELECT * FROM VALUES (('a', (10, null))), (('b', (10, 50))), " +
      "(('c', null)) AS tab(x, y)")
    checkAnswer(df, Row("a", Row(10, null)) :: Row("b", Row(10, 50)) :: Row("c", null) :: Nil)

    val cast = sql("SELECT cast(struct(1, null) AS struct<a:int,b:int>)")
    checkAnswer(cast, Row(Row(1, null)) :: Nil)
  }

  test("SPARK-27439: Explain result should match collected result after view change") {
    withTempView("test", "test2", "tmp") {
      spark.range(10).createOrReplaceTempView("test")
      spark.range(5).createOrReplaceTempView("test2")
      spark.sql("select * from test").createOrReplaceTempView("tmp")
      val df = spark.sql("select * from tmp")
      spark.sql("select * from test2").createOrReplaceTempView("tmp")

      val captured = new ByteArrayOutputStream()
      Console.withOut(captured) {
        df.explain(extended = true)
      }
      checkAnswer(df, spark.range(10).toDF)
      val output = captured.toString
      assert(output.contains(
        """== Parsed Logical Plan ==
          |'Project [*]
          |+- 'UnresolvedRelation [tmp]""".stripMargin))
      assert(output.contains(
        """== Physical Plan ==
          |*(1) Range (0, 10, step=1, splits=2)""".stripMargin))
    }
  }

  test("SPARK-29442 Set `default` mode should override the existing mode") {
    val df = Seq(Tuple1(1)).toDF()
    val writer = df.write.mode("overwrite").mode("default")
    val modeField = classOf[DataFrameWriter[Tuple1[Int]]].getDeclaredField("mode")
    modeField.setAccessible(true)
    assert(SaveMode.ErrorIfExists === modeField.get(writer).asInstanceOf[SaveMode])
  }

  test("sample should not duplicated the input data") {
    val df1 = spark.range(10).select($"id" as "id1", $"id" % 5 as "key1")
    val df2 = spark.range(10).select($"id" as "id2", $"id" % 5 as "key2")
    val sampled = df1.join(df2, $"key1" === $"key2")
      .sample(0.5, 42)
      .select("id1", "id2")
    val idTuples = sampled.collect().map(row => row.getLong(0) -> row.getLong(1))
    assert(idTuples.length == idTuples.toSet.size)
  }

  test("groupBy.as") {
    val df1 = Seq((1, 2, 3), (2, 3, 4)).toDF("a", "b", "c")
      .repartition($"a", $"b").sortWithinPartitions("a", "b")
    val df2 = Seq((1, 2, 4), (2, 3, 5)).toDF("a", "b", "c")
      .repartition($"a", $"b").sortWithinPartitions("a", "b")

    implicit val valueEncoder = RowEncoder(df1.schema)

    val df3 = df1.groupBy("a", "b").as[GroupByKey, Row]
      .cogroup(df2.groupBy("a", "b").as[GroupByKey, Row]) { case (_, data1, data2) =>
        data1.zip(data2).map { p =>
          p._1.getInt(2) + p._2.getInt(2)
        }
      }.toDF

    checkAnswer(df3.sort("value"), Row(7) :: Row(9) :: Nil)

    // Assert that no extra shuffle introduced by cogroup.
    val exchanges = collect(df3.queryExecution.executedPlan) {
      case h: ShuffleExchangeExec => h
    }
    assert(exchanges.size == 2)
  }

  test("groupBy.as: custom grouping expressions") {
    val df1 = Seq((1, 2, 3), (2, 3, 4)).toDF("a1", "b", "c")
      .repartition($"a1", $"b").sortWithinPartitions("a1", "b")
    val df2 = Seq((1, 2, 4), (2, 3, 5)).toDF("a1", "b", "c")
      .repartition($"a1", $"b").sortWithinPartitions("a1", "b")

    implicit val valueEncoder = RowEncoder(df1.schema)

    val groupedDataset1 = df1.groupBy(($"a1" + 1).as("a"), $"b").as[GroupByKey, Row]
    val groupedDataset2 = df2.groupBy(($"a1" + 1).as("a"), $"b").as[GroupByKey, Row]

    val df3 = groupedDataset1
      .cogroup(groupedDataset2) { case (_, data1, data2) =>
        data1.zip(data2).map { p =>
          p._1.getInt(2) + p._2.getInt(2)
        }
      }.toDF

    checkAnswer(df3.sort("value"), Row(7) :: Row(9) :: Nil)
  }

  test("groupBy.as: throw AnalysisException for unresolved grouping expr") {
    val df = Seq((1, 2, 3), (2, 3, 4)).toDF("a", "b", "c")

    implicit val valueEncoder = RowEncoder(df.schema)

    val err = intercept[AnalysisException] {
      df.groupBy($"d", $"b").as[GroupByKey, Row]
    }
    assert(err.getErrorClass == "UNRESOLVED_COLUMN")
    assert(err.messageParameters.head == "`d`")
  }

  test("emptyDataFrame should be foldable") {
    val emptyDf = spark.emptyDataFrame.withColumn("id", lit(1L))
    val joined = spark.range(10).join(emptyDf, "id")
    joined.queryExecution.optimizedPlan match {
      case LocalRelation(Seq(id), Nil, _) =>
        assert(id.name == "id")
      case _ =>
        fail("emptyDataFrame should be foldable")
    }
  }

  test("SPARK-30811: CTE should not cause stack overflow when " +
    "it refers to non-existent table with same name") {
    val e = intercept[AnalysisException] {
      sql("WITH t AS (SELECT 1 FROM nonexist.t) SELECT * FROM t")
    }
    assert(e.getMessage.contains("Table or view not found:"))
  }

  test("SPARK-32680: Don't analyze CTAS with unresolved query") {
    val v2Source = classOf[FakeV2Provider].getName
    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE t USING $v2Source AS SELECT * from nonexist")
    }
    assert(e.getMessage.contains("Table or view not found:"))
  }

  test("CalendarInterval reflection support") {
    val df = Seq((1, new CalendarInterval(1, 2, 3))).toDF("a", "b")
    checkAnswer(df.selectExpr("b"), Row(new CalendarInterval(1, 2, 3)))
  }

  test("SPARK-31552: array encoder with different types") {
    // primitives
    val booleans = Array(true, false)
    checkAnswer(Seq(booleans).toDF(), Row(booleans))

    val bytes = Array(1.toByte, 2.toByte)
    checkAnswer(Seq(bytes).toDF(), Row(bytes))
    val shorts = Array(1.toShort, 2.toShort)
    checkAnswer(Seq(shorts).toDF(), Row(shorts))
    val ints = Array(1, 2)
    checkAnswer(Seq(ints).toDF(), Row(ints))
    val longs = Array(1L, 2L)
    checkAnswer(Seq(longs).toDF(), Row(longs))

    val floats = Array(1.0F, 2.0F)
    checkAnswer(Seq(floats).toDF(), Row(floats))
    val doubles = Array(1.0D, 2.0D)
    checkAnswer(Seq(doubles).toDF(), Row(doubles))

    val strings = Array("2020-04-24", "2020-04-25")
    checkAnswer(Seq(strings).toDF(), Row(strings))

    // tuples
    val decOne = Decimal(1, 38, 18)
    val decTwo = Decimal(2, 38, 18)
    val tuple1 = (1, 2.2, "3.33", decOne, Date.valueOf("2012-11-22"))
    val tuple2 = (2, 3.3, "4.44", decTwo, Date.valueOf("2022-11-22"))
    checkAnswer(Seq(Array(tuple1, tuple2)).toDF(), Seq(Seq(tuple1, tuple2)).toDF())

    // case classes
    val gbks = Array(GroupByKey(1, 2), GroupByKey(4, 5))
    checkAnswer(Seq(gbks).toDF(), Row(Array(Row(1, 2), Row(4, 5))))

    // We can move this implicit def to [[SQLImplicits]] when we eventually make fully
    // support for array encoder like Seq and Set
    // For now cases below, decimal/datetime/interval/binary/nested types, etc,
    // are not supported by array
    implicit def newArrayEncoder[T <: Array[_] : TypeTag]: Encoder[T] = ExpressionEncoder()

    // decimals
    val decSpark = Array(decOne, decTwo)
    val decScala = decSpark.map(_.toBigDecimal)
    val decJava = decSpark.map(_.toJavaBigDecimal)
    checkAnswer(Seq(decSpark).toDF(), Row(decJava))
    checkAnswer(Seq(decScala).toDF(), Row(decJava))
    checkAnswer(Seq(decJava).toDF(), Row(decJava))

    // datetimes and intervals
    val dates = strings.map(Date.valueOf)
    checkAnswer(Seq(dates).toDF(), Row(dates))
    val localDates = dates.map(d => DateTimeUtils.daysToLocalDate(DateTimeUtils.fromJavaDate(d)))
    checkAnswer(Seq(localDates).toDF(), Row(dates))

    val timestamps =
      Array(Timestamp.valueOf("2020-04-24 12:34:56"), Timestamp.valueOf("2020-04-24 11:22:33"))
    checkAnswer(Seq(timestamps).toDF(), Row(timestamps))
    val instants =
      timestamps.map(t => DateTimeUtils.microsToInstant(DateTimeUtils.fromJavaTimestamp(t)))
    checkAnswer(Seq(instants).toDF(), Row(timestamps))

    val intervals = Array(new CalendarInterval(1, 2, 3), new CalendarInterval(4, 5, 6))
    checkAnswer(Seq(intervals).toDF(), Row(intervals))

    // binary
    val bins = Array(Array(1.toByte), Array(2.toByte), Array(3.toByte), Array(4.toByte))
    checkAnswer(Seq(bins).toDF(), Row(bins))

    // nested
    val nestedIntArray = Array(Array(1), Array(2))
    checkAnswer(Seq(nestedIntArray).toDF(), Row(nestedIntArray.map(wrapIntArray)))
    val nestedDecArray = Array(decSpark)
    checkAnswer(Seq(nestedDecArray).toDF(), Row(Array(wrapRefArray(decJava))))
  }

  test("SPARK-31750: eliminate UpCast if child's dataType is DecimalType") {
    withTempPath { f =>
      sql("select cast(1 as decimal(38, 0)) as d")
        .write.mode("overwrite")
        .parquet(f.getAbsolutePath)

      val df = spark.read.parquet(f.getAbsolutePath).as[BigDecimal]
      assert(df.schema === new StructType().add(StructField("d", DecimalType(38, 0))))
    }
  }

  test("SPARK-32640: ln(NaN) should return NaN") {
    val df = Seq(Double.NaN).toDF("d")
    checkAnswer(df.selectExpr("ln(d)"), Row(Double.NaN))
  }

  test("SPARK-32761: aggregating multiple distinct CONSTANT columns") {
     checkAnswer(sql("select count(distinct 2), count(distinct 2,3)"), Row(1, 1))
  }

  test("SPARK-32764: -0.0 and 0.0 should be equal") {
    val df = Seq(0.0 -> -0.0).toDF("pos", "neg")
    checkAnswer(df.select($"pos" > $"neg"), Row(false))
  }

  test("SPARK-32635: Replace references with foldables coming only from the node's children") {
    val a = Seq("1").toDF("col1").withColumn("col2", lit("1"))
    val b = Seq("2").toDF("col1").withColumn("col2", lit("2"))
    val aub = a.union(b)
    val c = aub.filter($"col1" === "2").cache()
    val d = Seq("2").toDF("col4")
    val r = d.join(aub, $"col2" === $"col4").select("col4")
    val l = c.select("col2")
    val df = l.join(r, $"col2" === $"col4", "LeftOuter")
    checkAnswer(df, Row("2", "2"))
  }

  test("SPARK-33939: Make Column.named use UnresolvedAlias to assign name") {
    val df = spark.range(1).selectExpr("id as id1", "id as id2")
    val df1 = df.selectExpr("cast(struct(id1, id2).id1 as int)")
    assert(df1.schema.head.name == "CAST(struct(id1, id2).id1 AS INT)")

    val df2 = df.selectExpr("cast(array(struct(id1, id2))[0].id1 as int)")
    assert(df2.schema.head.name == "CAST(array(struct(id1, id2))[0].id1 AS INT)")

    val df3 = df.select(hex(expr("struct(id1, id2).id1")))
    assert(df3.schema.head.name == "hex(struct(id1, id2).id1)")

    // this test is to make sure we don't have a regression.
    val df4 = df.selectExpr("id1 == null")
    assert(df4.schema.head.name == "(id1 = NULL)")
  }

  test("SPARK-33989: Strip auto-generated cast when using Cast.sql") {
    Seq("SELECT id == null FROM VALUES(1) AS t(id)",
      "SELECT floor(1)",
      "SELECT split(struct(c1, c2).c1, ',') FROM VALUES(1, 2) AS t(c1, c2)").foreach { sqlStr =>
      assert(!sql(sqlStr).schema.fieldNames.head.toLowerCase(Locale.getDefault).contains("cast"))
    }

    Seq("SELECT id == CAST(null AS int) FROM VALUES(1) AS t(id)",
      "SELECT floor(CAST(1 AS double))",
      "SELECT split(CAST(struct(c1, c2).c1 AS string), ',') FROM VALUES(1, 2) AS t(c1, c2)"
    ).foreach { sqlStr =>
      assert(sql(sqlStr).schema.fieldNames.head.toLowerCase(Locale.getDefault).contains("cast"))
    }
  }

  test("SPARK-34318: colRegex should work with column names & qualifiers which contain newlines") {
    val df = Seq(1, 2, 3).toDF("test\n_column").as("test\n_table")
    val col1 = df.colRegex("`tes.*\n.*mn`")
    checkAnswer(df.select(col1), Row(1) :: Row(2) :: Row(3) :: Nil)

    val col2 = df.colRegex("test\n_table.`tes.*\n.*mn`")
    checkAnswer(df.select(col2), Row(1) :: Row(2) :: Row(3) :: Nil)
  }

  test("SPARK-34763: col(), $\"<name>\", df(\"name\") should handle quoted column name properly") {
    val df1 = spark.sql("SELECT 'col1' AS `a``b.c`")
    checkAnswer(df1.selectExpr("`a``b.c`"), Row("col1"))
    checkAnswer(df1.select(df1("`a``b.c`")), Row("col1"))
    checkAnswer(df1.select(col("`a``b.c`")), Row("col1"))
    checkAnswer(df1.select($"`a``b.c`"), Row("col1"))

    val df2 = df1.as("d.e`f")
    checkAnswer(df2.selectExpr("`a``b.c`"), Row("col1"))
    checkAnswer(df2.select(df2("`a``b.c`")), Row("col1"))
    checkAnswer(df2.select(col("`a``b.c`")), Row("col1"))
    checkAnswer(df2.select($"`a``b.c`"), Row("col1"))

    checkAnswer(df2.selectExpr("`d.e``f`.`a``b.c`"), Row("col1"))
    checkAnswer(df2.select(df2("`d.e``f`.`a``b.c`")), Row("col1"))
    checkAnswer(df2.select(col("`d.e``f`.`a``b.c`")), Row("col1"))
    checkAnswer(df2.select($"`d.e``f`.`a``b.c`"), Row("col1"))

    val df3 = df1.as("*-#&% ?")
    checkAnswer(df3.selectExpr("`*-#&% ?`.`a``b.c`"), Row("col1"))
    checkAnswer(df3.select(df3("*-#&% ?.`a``b.c`")), Row("col1"))
    checkAnswer(df3.select(col("*-#&% ?.`a``b.c`")), Row("col1"))
    checkAnswer(df3.select($"*-#&% ?.`a``b.c`"), Row("col1"))
  }

  test("SPARK-34776: Nested column pruning should not prune Window produced attributes") {
    val df = Seq(
      ("t1", "123", "bob"),
      ("t1", "456", "bob"),
      ("t2", "123", "sam")
    ).toDF("type", "value", "name")

    val test = df.select(
      $"*",
      struct(count($"*").over(Window.partitionBy($"type", $"value", $"name"))
        .as("count"), $"name").as("name_count")
    ).select(
      $"*",
      max($"name_count").over(Window.partitionBy($"type", $"value")).as("best_name")
    )
    checkAnswer(test.select($"best_name.name"), Row("bob") :: Row("bob") :: Row("sam") :: Nil)
  }

  test("SPARK-34829: Multiple applications of typed ScalaUDFs in higher order functions work") {
    val reverse = udf((s: String) => s.reverse)
    val reverse2 = udf((b: Bar2) => Bar2(b.s.reverse))

    val df = Seq(Array("abc", "def")).toDF("array")
    val test = df.select(transform(col("array"), s => reverse(s)))
    checkAnswer(test, Row(Array("cba", "fed")) :: Nil)

    val df2 = Seq(Array(Bar2("abc"), Bar2("def"))).toDF("array")
    val test2 = df2.select(transform(col("array"), b => reverse2(b)))
    checkAnswer(test2, Row(Array(Row("cba"), Row("fed"))) :: Nil)

    val df3 = Seq(Map("abc" -> 1, "def" -> 2)).toDF("map")
    val test3 = df3.select(transform_keys(col("map"), (s, _) => reverse(s)))
    checkAnswer(test3, Row(Map("cba" -> 1, "fed" -> 2)) :: Nil)

    val df4 = Seq(Map(Bar2("abc") -> 1, Bar2("def") -> 2)).toDF("map")
    val test4 = df4.select(transform_keys(col("map"), (b, _) => reverse2(b)))
    checkAnswer(test4, Row(Map(Row("cba") -> 1, Row("fed") -> 2)) :: Nil)

    val df5 = Seq(Map(1 -> "abc", 2 -> "def")).toDF("map")
    val test5 = df5.select(transform_values(col("map"), (_, s) => reverse(s)))
    checkAnswer(test5, Row(Map(1 -> "cba", 2 -> "fed")) :: Nil)

    val df6 = Seq(Map(1 -> Bar2("abc"), 2 -> Bar2("def"))).toDF("map")
    val test6 = df6.select(transform_values(col("map"), (_, b) => reverse2(b)))
    checkAnswer(test6, Row(Map(1 -> Row("cba"), 2 -> Row("fed"))) :: Nil)

    val reverseThenConcat = udf((s1: String, s2: String) => s1.reverse ++ s2.reverse)
    val reverseThenConcat2 = udf((b1: Bar2, b2: Bar2) => Bar2(b1.s.reverse ++ b2.s.reverse))

    val df7 = Seq((Map(1 -> "abc", 2 -> "def"), Map(1 -> "ghi", 2 -> "jkl"))).toDF("map1", "map2")
    val test7 =
      df7.select(map_zip_with(col("map1"), col("map2"), (_, s1, s2) => reverseThenConcat(s1, s2)))
    checkAnswer(test7, Row(Map(1 -> "cbaihg", 2 -> "fedlkj")) :: Nil)

    val df8 = Seq((Map(1 -> Bar2("abc"), 2 -> Bar2("def")),
      Map(1 -> Bar2("ghi"), 2 -> Bar2("jkl")))).toDF("map1", "map2")
    val test8 =
      df8.select(map_zip_with(col("map1"), col("map2"), (_, b1, b2) => reverseThenConcat2(b1, b2)))
    checkAnswer(test8, Row(Map(1 -> Row("cbaihg"), 2 -> Row("fedlkj"))) :: Nil)

    val df9 = Seq((Array("abc", "def"), Array("ghi", "jkl"))).toDF("array1", "array2")
    val test9 =
      df9.select(zip_with(col("array1"), col("array2"), (s1, s2) => reverseThenConcat(s1, s2)))
    checkAnswer(test9, Row(Array("cbaihg", "fedlkj")) :: Nil)

    val df10 = Seq((Array(Bar2("abc"), Bar2("def")), Array(Bar2("ghi"), Bar2("jkl"))))
      .toDF("array1", "array2")
    val test10 =
      df10.select(zip_with(col("array1"), col("array2"), (b1, b2) => reverseThenConcat2(b1, b2)))
    checkAnswer(test10, Row(Array(Row("cbaihg"), Row("fedlkj"))) :: Nil)
  }

  test("SPARK-39293: The accumulator of ArrayAggregate to handle complex types properly") {
    val reverse = udf((s: String) => s.reverse)

    val df = Seq(Array("abc", "def")).toDF("array")
    val testArray = df.select(
      aggregate(
        col("array"),
        array().cast("array<string>"),
        (acc, s) => concat(acc, array(reverse(s)))))
    checkAnswer(testArray, Row(Array("cba", "fed")) :: Nil)

    val testMap = df.select(
      aggregate(
        col("array"),
        map().cast("map<string, string>"),
        (acc, s) => map_concat(acc, map(s, reverse(s)))))
    checkAnswer(testMap, Row(Map("abc" -> "cba", "def" -> "fed")) :: Nil)
  }

  test("SPARK-34882: Aggregate with multiple distinct null sensitive aggregators") {
    withUserDefinedFunction(("countNulls", true)) {
      spark.udf.register("countNulls", udaf(new Aggregator[JLong, JLong, JLong] {
        def zero: JLong = 0L
        def reduce(b: JLong, a: JLong): JLong = if (a == null) {
          b + 1
        } else {
          b
        }
        def merge(b1: JLong, b2: JLong): JLong = b1 + b2
        def finish(r: JLong): JLong = r
        def bufferEncoder: Encoder[JLong] = Encoders.LONG
        def outputEncoder: Encoder[JLong] = Encoders.LONG
      }))

      val result = testData.selectExpr(
        "countNulls(key)",
        "countNulls(DISTINCT key)",
        "countNulls(key) FILTER (WHERE key > 50)",
        "countNulls(DISTINCT key) FILTER (WHERE key > 50)",
        "count(DISTINCT key)")

      checkAnswer(result, Row(0, 0, 0, 0, 100))
    }
  }

  test("SPARK-35410: SubExpr elimination should not include redundant child exprs " +
    "for conditional expressions") {
    val accum = sparkContext.longAccumulator("call")
    val simpleUDF = udf((s: String) => {
      accum.add(1)
      s
    })
    val df1 = spark.range(5).select(when(functions.length(simpleUDF($"id")) > 0,
      functions.length(simpleUDF($"id"))).otherwise(
        functions.length(simpleUDF($"id")) + 1))
    df1.collect()
    assert(accum.value == 5)

    val nondeterministicUDF = simpleUDF.asNondeterministic()
    val df2 = spark.range(5).select(when(functions.length(nondeterministicUDF($"id")) > 0,
      functions.length(nondeterministicUDF($"id"))).otherwise(
        functions.length(nondeterministicUDF($"id")) + 1))
    df2.collect()
    assert(accum.value == 15)
  }

  test("SPARK-35560: Remove redundant subexpression evaluation in nested subexpressions") {
    Seq(1, Int.MaxValue).foreach { splitThreshold =>
      withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> splitThreshold.toString) {
        val accum = sparkContext.longAccumulator("call")
        val simpleUDF = udf((s: String) => {
          accum.add(1)
          s
        })

        // Common exprs:
        //  1. simpleUDF($"id")
        //  2. functions.length(simpleUDF($"id"))
        // We should only evaluate `simpleUDF($"id")` once, i.e.
        // subExpr1 = simpleUDF($"id");
        // subExpr2 = functions.length(subExpr1);
        val df = spark.range(5).select(
          when(functions.length(simpleUDF($"id")) === 1, lower(simpleUDF($"id")))
            .when(functions.length(simpleUDF($"id")) === 0, upper(simpleUDF($"id")))
            .otherwise(simpleUDF($"id")).as("output"))
        df.collect()
        assert(accum.value == 5)
      }
    }
  }

  test("isLocal should consider CommandResult and LocalRelation") {
    val df1 = sql("SHOW TABLES")
    assert(df1.isLocal)
    val df2 = (1 to 10).toDF()
    assert(df2.isLocal)
  }

  test("SPARK-35886: PromotePrecision should be subexpr replaced") {
    withTable("tbl") {
      sql(
        """
          |CREATE TABLE tbl (
          |  c1 DECIMAL(18,6),
          |  c2 DECIMAL(18,6),
          |  c3 DECIMAL(18,6))
          |USING parquet;
          |""".stripMargin)
      sql("INSERT INTO tbl SELECT 1, 1, 1")
      checkAnswer(sql("SELECT sum(c1 * c3) + sum(c2 * c3) FROM tbl"), Row(2.00000000000) :: Nil)
    }
  }

  test("SPARK-36338: DataFrame.withSequenceColumn should append unique sequence IDs") {
    val ids = spark.range(10).repartition(5)
      .withSequenceColumn("default_index").collect().map(_.getLong(0))
    assert(ids.toSet === Range(0, 10).toSet)
  }

  test("SPARK-35320: Reading JSON with key type different to String in a map should fail") {
    Seq(
      (MapType(IntegerType, StringType), """{"1": "test"}"""),
      (StructType(Seq(StructField("test", MapType(IntegerType, StringType)))),
        """"test": {"1": "test"}"""),
      (ArrayType(MapType(IntegerType, StringType)), """[{"1": "test"}]"""),
      (MapType(StringType, MapType(IntegerType, StringType)), """{"key": {"1" : "test"}}""")
    ).foreach { case (schema, jsonData) =>
      withTempDir { dir =>
        val colName = "col"
        val msg = "can only contain STRING as a key type for a MAP"

        val thrown1 = intercept[AnalysisException](
          spark.read.schema(StructType(Seq(StructField(colName, schema))))
            .json(Seq(jsonData).toDS()).collect())
        assert(thrown1.getMessage.contains(msg))

        val jsonDir = new File(dir, "json").getCanonicalPath
        Seq(jsonData).toDF(colName).write.json(jsonDir)
        val thrown2 = intercept[AnalysisException](
          spark.read.schema(StructType(Seq(StructField(colName, schema))))
            .json(jsonDir).collect())
        assert(thrown2.getMessage.contains(msg))
      }
    }
  }

  test("SPARK-37855: IllegalStateException when transforming an array inside a nested struct") {
    def makeInput(): DataFrame = {
      val innerElement1 = Row(3, 3.12)
      val innerElement2 = Row(4, 2.1)
      val innerElement3 = Row(1, 985.2)
      val innerElement4 = Row(10, 757548.0)
      val innerElement5 = Row(1223, 0.665)

      val outerElement1 = Row(1, Row(List(innerElement1, innerElement2)))
      val outerElement2 = Row(2, Row(List(innerElement3)))
      val outerElement3 = Row(3, Row(List(innerElement4, innerElement5)))

      val data = Seq(
        Row("row1", List(outerElement1)),
        Row("row2", List(outerElement2, outerElement3))
      )

      val schema = new StructType()
        .add("name", StringType)
        .add("outer_array", ArrayType(new StructType()
          .add("id", IntegerType)
          .add("inner_array_struct", new StructType()
            .add("inner_array", ArrayType(new StructType()
              .add("id", IntegerType)
              .add("value", DoubleType)
            ))
          )
        ))

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    val df = makeInput().limit(2)

    val res = df.withColumn("extracted", transform(
      col("outer_array"),
      c1 => {
        struct(
          c1.getField("id").alias("outer_id"),
          transform(
            c1.getField("inner_array_struct").getField("inner_array"),
            c2 => {
              struct(
                c2.getField("value").alias("inner_value")
              )
            }
          )
        )
      }
    ))

    assert(res.collect.length == 2)
  }

  test("SPARK-38285: Fix ClassCastException: GenericArrayData cannot be cast to InternalRow") {
    withTempView("v1") {
      val sqlText =
        """
          |CREATE OR REPLACE TEMP VIEW v1 AS
          |SELECT * FROM VALUES
          |(array(
          |  named_struct('s', 'string1', 'b', array(named_struct('e', 'string2'))),
          |  named_struct('s', 'string4', 'b', array(named_struct('e', 'string5')))
          |  )
          |)
          |v1(o);
          |""".stripMargin
      sql(sqlText)

      val df = sql("SELECT eo.b.e FROM (SELECT explode(o) AS eo FROM v1)")
      checkAnswer(df, Row(Seq("string2")) :: Row(Seq("string5")) :: Nil)
    }
  }

  test("SPARK-37865: Do not deduplicate union output columns") {
    val df1 = Seq((1, 1), (1, 2)).toDF("a", "b")
    val df2 = Seq((2, 2), (2, 3)).toDF("c", "d")

    def sqlQuery(cols1: Seq[String], cols2: Seq[String], distinct: Boolean): String = {
      val union = if (distinct) {
        "UNION"
      } else {
        "UNION ALL"
      }
      s"""
         |SELECT ${cols1.mkString(",")} FROM VALUES (1, 1), (1, 2) AS t1(a, b)
         |$union SELECT ${cols2.mkString(",")} FROM VALUES (2, 2), (2, 3) AS t2(c, d)
         |""".stripMargin
    }

    Seq(
      (Seq("a", "a"), Seq("c", "d"), Seq(Row(1, 1), Row(1, 1), Row(2, 2), Row(2, 3))),
      (Seq("a", "b"), Seq("c", "d"), Seq(Row(1, 1), Row(1, 2), Row(2, 2), Row(2, 3))),
      (Seq("a", "b"), Seq("c", "c"), Seq(Row(1, 1), Row(1, 2), Row(2, 2), Row(2, 2)))
    ).foreach { case (cols1, cols2, rows) =>
      // UNION ALL (non-distinct)
      val df3 = df1.selectExpr(cols1: _*).union(df2.selectExpr(cols2: _*))
      checkAnswer(df3, rows)

      val t3 = sqlQuery(cols1, cols2, false)
      checkAnswer(sql(t3), rows)

      // Avoid breaking change
      var correctAnswer = rows.map(r => Row(r(0)))
      checkAnswer(df3.select(df1.col("a")), correctAnswer)
      checkAnswer(sql(s"select a from ($t3) t3"), correctAnswer)

      // This has always been broken
      intercept[AnalysisException] {
        df3.select(df2.col("d")).collect()
      }
      intercept[AnalysisException] {
        sql(s"select d from ($t3) t3")
      }

      // UNION (distinct)
      val df4 = df3.distinct
      checkAnswer(df4, rows.distinct)

      val t4 = sqlQuery(cols1, cols2, true)
      checkAnswer(sql(t4), rows.distinct)

      // Avoid breaking change
      correctAnswer = rows.distinct.map(r => Row(r(0)))
      checkAnswer(df4.select(df1.col("a")), correctAnswer)
      checkAnswer(sql(s"select a from ($t4) t4"), correctAnswer)

      // This has always been broken
      intercept[AnalysisException] {
        df4.select(df2.col("d")).collect()
      }
      intercept[AnalysisException] {
        sql(s"select d from ($t4) t4")
      }
    }
  }

  test("SPARK-39612: exceptAll with following count should work") {
    val d1 = Seq("a").toDF
    assert(d1.exceptAll(d1).count() === 0)
  }
}

case class GroupByKey(a: Int, b: Int)

case class Bar2(s: String)

/**
 * This class is used for unit-testing. It's a logical plan whose output and stats are passed in.
 */
case class OutputListAwareStatsTestPlan(
    outputList: Seq[Attribute],
    rowCount: BigInt,
    size: Option[BigInt] = None) extends LeafNode with MultiInstanceRelation {
  override def output: Seq[Attribute] = outputList
  override def computeStats(): Statistics = {
    val columnInfo = outputList.map { attr =>
      attr.dataType match {
        case BooleanType =>
          attr -> ColumnStat(
            distinctCount = Some(2),
            min = Some(false),
            max = Some(true),
            nullCount = Some(0),
            avgLen = Some(1),
            maxLen = Some(1))

        case ByteType =>
          attr -> ColumnStat(
            distinctCount = Some(2),
            min = Some(1),
            max = Some(2),
            nullCount = Some(0),
            avgLen = Some(1),
            maxLen = Some(1))

        case _ =>
          attr -> ColumnStat()
      }
    }
    val attrStats = AttributeMap(columnInfo)

    Statistics(
      // If sizeInBytes is useless in testing, we just use a fake value
      sizeInBytes = size.getOrElse(Int.MaxValue),
      rowCount = Some(rowCount),
      attributeStats = attrStats)
  }
  override def newInstance(): LogicalPlan = copy(outputList = outputList.map(_.newInstance()))
}
