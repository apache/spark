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
import java.util.Locale

import scala.collection.immutable.ListMap
import scala.util.Random

import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkException
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, EqualTo, ExpressionSet, GreaterThan, Literal, PythonUDF, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LocalRelation, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.execution.{FilterExec, LogicalRDD, QueryExecution, SortExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.test.SQLTestData.{ArrayStringWrapper, ContainerStringWrapper, StringWrapper, TestData2}
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.ArrayImplicits._

@SlowSQLTest
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

  test("rename nested groupby") {
    val df = Seq((1, (1, 1))).toDF()

    checkAnswer(
      df.groupBy("_1").agg(sum("_2._1")).toDF("key", "total"),
      Row(1, 1) :: Nil)
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
    checkError(
      exception = intercept[AnalysisException] {
        df.select(explode($"*"))
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"explode(csv)\"",
        "paramIndex" -> "first",
        "inputSql"-> "\"csv\"",
        "inputType" -> "\"STRING\"",
        "requiredType" -> "(\"ARRAY\" or \"MAP\")"),
      context = ExpectedContext(fragment = "explode", getCurrentClassCallSitePattern)
    )

    val df2 = Seq(Array("1", "2"), Array("4"), Array("7", "8", "9")).toDF("csv")
    checkAnswer(
      df2.select(explode($"*")),
      Row("1") :: Row("2") :: Row("4") :: Row("7") :: Row("8") :: Row("9") :: Nil)
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

  test("SPARK-50503 - cannot partition by variant columns") {
    val df = sql("select parse_json(case when id = 0 then 'null' else '1' end)" +
      " as v, id % 5 as id, named_struct('v', parse_json(id::string)) s from range(0, 100, 1, 5)")
    // variant column
    checkError(
      exception = intercept[AnalysisException](df.repartition(5, col("v"))),
      condition = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
      parameters = Map(
        "expr" -> "\"v\"",
        "dataType" -> "\"VARIANT\"")
    )
    // nested variant column
    checkError(
      exception = intercept[AnalysisException](df.repartition(5, col("s"))),
      condition = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
      parameters = Map(
        "expr" -> "\"s\"",
        "dataType" -> "\"STRUCT<v: VARIANT NOT NULL>\"")
    )
    // variant producing expression
    checkError(
      exception =
        intercept[AnalysisException](df.repartition(5, parse_json(col("id").cast("string")))),
      condition = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
      parameters = Map(
        "expr" -> "\"parse_json(CAST(id AS STRING))\"",
        "dataType" -> "\"VARIANT\"")
    )
    // Partitioning by non-variant column works
    try {
      df.repartition(5, col("id")).collect()
    } catch {
      case e: Exception =>
        fail(s"Expected no exception to be thrown but an exception was thrown: ${e.getMessage}")
    }
    // SQL
    withTempView("tv") {
      df.createOrReplaceTempView("tv")
      checkError(
        exception = intercept[AnalysisException](sql("SELECT * FROM tv DISTRIBUTE BY v")),
        condition = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
        parameters = Map(
          "expr" -> "\"v\"",
          "dataType" -> "\"VARIANT\""),
        context = ExpectedContext(
          fragment = "DISTRIBUTE BY v",
          start = 17,
          stop = 31)
      )
      checkError(
        exception = intercept[AnalysisException](sql("SELECT * FROM tv DISTRIBUTE BY s")),
        condition = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
        parameters = Map(
          "expr" -> "\"s\"",
          "dataType" -> "\"STRUCT<v: VARIANT NOT NULL>\""),
        context = ExpectedContext(
          fragment = "DISTRIBUTE BY s",
          start = 17,
          stop = 31)
      )
    }
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

  test("repartition by MapType") {
    Seq("int", "long", "float", "double", "decimal(10, 2)", "string", "varchar(6)").foreach { dt =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df = spark.range(20)
          .withColumn("c1",
            when(col("id") % 3 === 1, typedLit(Map(1 -> 1)))
              .when(col("id") % 3 === 2, typedLit(Map(1 -> 1, 2 -> 2)))
              .otherwise(typedLit(Map(2 -> 2, 1 -> 1))).cast(s"map<$dt, $dt>"))
          .withColumn("c2", typedLit(Map(1 -> null)).cast(s"map<$dt, $dt>"))
          .withColumn("c3", lit(null).cast(s"map<$dt, $dt>"))

        assertPartitionNumber(df.repartition(4, col("c1")), 2)
        assertPartitionNumber(df.repartition(4, col("c2")), 1)
        assertPartitionNumber(df.repartition(4, col("c3")), 1)
        assertPartitionNumber(df.repartition(4, col("c1"), col("c2")), 2)
        assertPartitionNumber(df.repartition(4, col("c1"), col("c3")), 2)
        assertPartitionNumber(df.repartition(4, col("c1"), col("c2"), col("c3")), 2)
        assertPartitionNumber(df.repartition(4, col("c2"), col("c3")), 2)
      }
    }
  }

  private def assertPartitionNumber(df: => DataFrame, max: Int): Unit = {
    val dfGrouped = df.groupBy(spark_partition_id()).count()
    // Result number of partition can be lower or equal to max,
    // but no more than that.
    assert(dfGrouped.count() <= max, dfGrouped.queryExecution.simpleString)
  }

  test("coalesce") {
    intercept[IllegalArgumentException] {
      testData.select("key").coalesce(0)
    }

    assert(testData.select("key").coalesce(1).rdd.partitions.length === 1)

    checkAnswer(
      testData.select("key").coalesce(1).select("key"),
      testData.select("key").collect().toSeq)

    assert(spark.emptyDataFrame.coalesce(1).rdd.partitions.length === 1)
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

    checkError(
      exception = intercept[AnalysisException] {
        testData.toDF().withColumns(Seq("newCol1", "newCOL1"),
          Seq(col("key") + 1, col("key") + 2))
      },
      condition = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`newcol1`"))
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

      checkError(
        exception = intercept[AnalysisException] {
          testData.toDF().withColumns(Seq("newCol1", "newCol1"),
            Seq(col("key") + 1, col("key") + 2))
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`newCol1`"))
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
    val df1 = sparkContext.parallelize(Array(1, 2, 3).toImmutableArraySeq).toDF("x")
    val df2 = df1.withMetadata("x", metadata)
    assert(df2.schema(0).metadata === metadata)

    checkError(
      exception = intercept[AnalysisException] {
        df1.withMetadata("x1", metadata)
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`x1`", "proposal" -> "`x`")
    )
  }

  test("replace column using withColumn") {
    val df2 = sparkContext.parallelize(Array(1, 2, 3).toImmutableArraySeq).toDF("x")
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

  test("SPARK-39895: drop two column references") {
    val col = Column("key")
    val randomCol = Column("random")
    val df = testData.drop(col, randomCol)
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
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
    assert(df2.drop("`a.b`").columns.length == 2)
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

  test("SPARK-40311: withColumnsRenamed") {
      val df = testData.toDF().withColumns(Seq("newCol1", "newCOL2"),
        Seq(col("key") + 1, col("key") + 2))
        .withColumnsRenamed(Map("newCol1" -> "renamed1", "newCol2" -> "renamed2"))
      checkAnswer(
        df,
        testData.collect().map { case Row(key: Int, value: String) =>
          Row(key, value, key + 1, key + 2)
        }.toSeq)
      assert(df.columns === Array("key", "value", "renamed1", "renamed2"))
  }

  test("SPARK-46260: withColumnsRenamed should respect the Map ordering") {
    val df = spark.range(10).toDF()
    assert(df.withColumnsRenamed(ListMap("id" -> "a", "a" -> "b")).columns === Array("b"))
    assert(df.withColumnsRenamed(ListMap("a" -> "b", "id" -> "a")).columns === Array("a"))
  }

  test("SPARK-20384: Value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(StringWrapper("a"), StringWrapper("b"), StringWrapper("c")))
      .toDF()
    val filtered = df.where("s = \"a\"")
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(StringWrapper("a"))).toDF())
  }

  test("SPARK-20384: Tuple2 of value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(
        (StringWrapper("a1"), StringWrapper("a2")),
        (StringWrapper("b1"), StringWrapper("b2"))))
      .toDF()
    val filtered = df.where("_2.s = \"a2\"")
    checkAnswer(filtered,
      spark.sparkContext.parallelize(Seq((StringWrapper("a1"), StringWrapper("a2")))).toDF())
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
        Seq((StringWrapper("a1"), StringWrapper("a2"), StringWrapper("a3")))).toDF())
  }

  test("SPARK-20384: Array value class filter") {
    val ab = ArrayStringWrapper(Seq(StringWrapper("a"), StringWrapper("b")))
    val cd = ArrayStringWrapper(Seq(StringWrapper("c"), StringWrapper("d")))

    val df = spark.sparkContext.parallelize(Seq(ab, cd)).toDF()
    val filtered = df.where(array_contains(col("wrappers.s"), "b"))
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(ab)).toDF())
  }

  test("SPARK-20384: Nested value class filter") {
    val a = ContainerStringWrapper(StringWrapper("a"))
    val b = ContainerStringWrapper(StringWrapper("b"))

    val df = spark.sparkContext.parallelize(Seq(a, b)).toDF()
    // flat value class, `s` field is not in schema
    val filtered = df.where("wrapper = \"a\"")
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(a)).toDF())
  }

  private lazy val person2: DataFrame = Seq(
    ("Bob", 16, 176),
    ("Alice", 32, 164),
    ("David", 60, 192),
    ("Amy", 24, 180)).toDF("name", "age", "height")

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

  test("SPARK-41391: Correct the output column name of groupBy.agg(count_distinct)") {
    withTempView("person") {
      person.createOrReplaceTempView("person")
      val df1 = person.groupBy("id").agg(count_distinct(col("name")))
      val df2 = spark.sql("SELECT id, COUNT(DISTINCT name) FROM person GROUP BY id")
      assert(df1.columns === df2.columns)
      val df3 = person.groupBy("id").agg(count_distinct(col("*")))
      val df4 = spark.sql("SELECT id, COUNT(DISTINCT *) FROM person GROUP BY id")
      assert(df3.columns === df4.columns)
    }
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

    withSQLConf(SQLConf.BINARY_OUTPUT_STYLE.key -> "HEX_DISCRETE") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("[31 32]", "[41 42 43 2E]"),
        Seq("[33 34]", "[31 32 33 34 36]"))
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SQLConf.BINARY_OUTPUT_STYLE.key -> "HEX") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("3132", "4142432E"),
        Seq("3334", "3132333436")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SQLConf.BINARY_OUTPUT_STYLE.key -> "BASE64") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("MTI", "QUJDLg"),
        Seq("MzQ", "MTIzNDY")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SQLConf.BINARY_OUTPUT_STYLE.key -> "UTF-8") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("12", "ABC."),
        Seq("34", "12346")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SQLConf.BINARY_OUTPUT_STYLE.key -> "BASIC") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("[49, 50]", "[65, 66, 67, 46]"),
        Seq("[51, 52]", "[49, 50, 51, 52, 54]")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
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

      def checkSyntaxError(name: String): Unit = {
        checkError(
          exception = intercept[org.apache.spark.sql.AnalysisException] {
            df(name)
          },
          condition = "INVALID_ATTRIBUTE_NAME_SYNTAX",
          parameters = Map("name" -> name))
      }

      checkSyntaxError("`abc.`c`")
      checkSyntaxError("`abc`..d")
      checkSyntaxError("`a`.b.")
      checkSyntaxError("`a.b`.c.`d")
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
    val e = intercept[AnalysisException] {
      Seq((1, 2, 3), (2, 3, 4), (3, 4, 5)).toDF("column1", "column2", "column1")
        .write.format("parquet").save("temp")
    }
    checkError(
      exception = e,
      condition = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`column1`"))

    // multiple duplicate columns present
    val f = intercept[AnalysisException] {
      Seq((1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7))
        .toDF("column1", "column2", "column3", "column1", "column3")
        .write.format("json").save("temp")
    }
    checkError(
      exception = f,
      condition = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`column1`"))
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
        checkError(
          exception = intercept[AnalysisException] {
            insertion.write.insertInto("rdd_base")
          },
          condition = "UNSUPPORTED_INSERT.RDD_BASED",
          parameters = Map.empty
        )

        // error case: insert into a logical plan that is not a LeafNode
        val indirectDS = pdf.select("_1").filter($"_1" > 5)
        indirectDS.createOrReplaceTempView("indirect_ds")
        checkError(
          exception = intercept[AnalysisException] {
            insertion.write.insertInto("indirect_ds")
          },
          condition = "UNSUPPORTED_INSERT.RDD_BASED",
          parameters = Map.empty
        )

        // error case: insert into an OneRowRelation
        Dataset.ofRows(spark, OneRowRelation()).createOrReplaceTempView("one_row")
        checkError(
          exception = intercept[AnalysisException] {
            insertion.write.insertInto("one_row")
          },
          condition = "UNSUPPORTED_INSERT.RDD_BASED",
          parameters = Map.empty
        )
      }
    }
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
    val expected = df.select($"i", rand(seed)).as[(Long, Double)].collect().sortBy(_._2).map(_._1)
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

  test("SPARK-39834: build the constraints for LogicalRDD based on origin constraints") {
    def buildExpectedConstraints(attrs: Seq[Attribute]): ExpressionSet = {
      val exprs = attrs.flatMap { attr =>
        attr.dataType match {
          case BooleanType => Some(EqualTo(attr, Literal(true, BooleanType)))
          case IntegerType => Some(GreaterThan(attr, Literal(5, IntegerType)))
          case _ => None
        }
      }
      ExpressionSet(exprs)
    }

    val outputList = Seq(
      AttributeReference("cbool", BooleanType)(),
      AttributeReference("cbyte", ByteType)(),
      AttributeReference("cint", IntegerType)()
    )

    val statsPlan = OutputListAwareConstraintsTestPlan(outputList = outputList)

    val df = Dataset.ofRows(spark, statsPlan)
      // add some map-like operations which optimizer will optimize away, and make a divergence
      // for output between logical plan and optimized plan
      // logical plan
      // Project [cb#6 AS cbool#12, cby#7 AS cbyte#13, ci#8 AS cint#14]
      // +- Project [cbool#0 AS cb#6, cbyte#1 AS cby#7, cint#2 AS ci#8]
      //    +- OutputListAwareConstraintsTestPlan [cbool#0, cbyte#1, cint#2]
      // optimized plan
      // OutputListAwareConstraintsTestPlan [cbool#0, cbyte#1, cint#2]
      .selectExpr("cbool AS cb", "cbyte AS cby", "cint AS ci")
      .selectExpr("cb AS cbool", "cby AS cbyte", "ci AS cint")

    // We can't leverage LogicalRDD.fromDataset here, since it triggers physical planning and
    // there is no matching physical node for OutputListAwareConstraintsTestPlan.
    val optimizedPlan = df.queryExecution.optimizedPlan
    val rewrite = LogicalRDD.buildOutputAssocForRewrite(optimizedPlan.output, df.logicalPlan.output)
    val logicalRDD = LogicalRDD(
      df.logicalPlan.output, spark.sparkContext.emptyRDD[InternalRow], isStreaming = true)(
      spark, None, Some(LogicalRDD.rewriteConstraints(optimizedPlan.constraints, rewrite.get)))

    val constraints = logicalRDD.constraints
    val expectedConstraints = buildExpectedConstraints(logicalRDD.output)
    assert(constraints === expectedConstraints)

    // This method re-issues expression IDs for all outputs. We expect constraints to be
    // reflected as well.
    val newLogicalRDD = logicalRDD.newInstance()
    val newConstraints = newLogicalRDD.constraints
    val newExpectedConstraints = buildExpectedConstraints(newLogicalRDD.output)
    assert(newConstraints === newExpectedConstraints)
  }

  test("SPARK-46794: exclude subqueries from LogicalRDD constraints") {
    withTempDir { checkpointDir =>
      val subquery =
        Column(ScalarSubquery(spark.range(10).selectExpr("max(id)").logicalPlan))
      val df = spark.range(1000).filter($"id" === subquery)
      assert(df.logicalPlan.constraints.exists(_.exists(_.isInstanceOf[ScalarSubquery])))

      spark.sparkContext.setCheckpointDir(checkpointDir.getAbsolutePath)
      val checkpointedDf = df.checkpoint()
      assert(!checkpointedDf.logicalPlan.constraints
        .exists(_.exists(_.isInstanceOf[ScalarSubquery])))
    }
  }

  test("SPARK-10656: completely support special chars") {
    val df = Seq(1 -> "a").toDF("i_$.a", "d^'a.")
    checkAnswer(df.select(df("*")), Row(1, "a"))
    checkAnswer(df.withColumnRenamed("d^'a.", "a"), Row(1, "a"))
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

  test("SPARK-15230: distinct() does not handle column name with dot properly") {
    val df = Seq(1, 1, 2).toDF("column.with.dot")
    checkAnswer(df.distinct(), Row(1) :: Row(2) :: Nil)
  }

  test("SPARK-16664: persist with more than 200 columns") {
    val size = 201L
    val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(Seq.range(0, size))))
    val schemas = List.range(0, size).map(a => StructField("name" + a, LongType, true))
    val df = spark.createDataFrame(rdd, StructType(schemas))
    assert(df.persist().take(1).apply(0).toSeq(100).asInstanceOf[Long] == 100)
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
    val d = sampleDf.withColumn("c", monotonically_increasing_id()).select($"c").collect()
    assert(d.length == d.distinct.length)
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

  test("Uuid expressions should produce same results at retries in the same DataFrame") {
    val df = spark.range(1).select($"id", uuid())
    checkAnswer(df, df.collect())
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

  test("SPARK-42655 Fix ambiguous column reference error") {
    val df1 = sparkContext.parallelize(List((1, 2, 3, 4, 5))).toDF("id", "col2", "col3",
      "col4", "col5")
    val op_cols_mixed_case = List("id", "col2", "col3", "col4", "col5", "ID")
    val df2 = df1.select(op_cols_mixed_case.head, op_cols_mixed_case.tail: _*)
    // should not throw any error.
    checkAnswer(df2.select("id"), Row(1))
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
      checkAnswer(df, spark.range(10).toDF())
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
    val modeField = classOf[DataFrameWriter[_]].getDeclaredField("mode")
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

    implicit val valueEncoder = ExpressionEncoder(df1.schema)

    val df3 = df1.groupBy("a", "b").as[GroupByKey, Row]
      .cogroup(df2.groupBy("a", "b").as[GroupByKey, Row]) { case (_, data1, data2) =>
        data1.zip(data2).map { p =>
          p._1.getInt(2) + p._2.getInt(2)
        }
      }.toDF()

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

    implicit val valueEncoder = ExpressionEncoder(df1.schema)

    val groupedDataset1 = df1.groupBy(($"a1" + 1).as("a"), $"b").as[GroupByKey, Row]
    val groupedDataset2 = df2.groupBy(($"a1" + 1).as("a"), $"b").as[GroupByKey, Row]

    val df3 = groupedDataset1
      .cogroup(groupedDataset2) { case (_, data1, data2) =>
        data1.zip(data2).map { p =>
          p._1.getInt(2) + p._2.getInt(2)
        }
      }.toDF()

    checkAnswer(df3.sort("value"), Row(7) :: Row(9) :: Nil)
  }

  test("groupBy.as: throw AnalysisException for unresolved grouping expr") {
    val df = Seq((1, 2, 3), (2, 3, 4)).toDF("a", "b", "c")

    implicit val valueEncoder = ExpressionEncoder(df.schema)

    checkError(
      exception = intercept[AnalysisException] {
        df.groupBy($"d", $"b").as[GroupByKey, Row]
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`d`", "proposal" -> "`a`, `b`, `c`"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("SPARK-40601: flatMapCoGroupsInPandas should fail with different number of keys") {
    val df1 = Seq((1, 2, "A1"), (2, 1, "A2")).toDF("key1", "key2", "value")
    val df2 = df1.filter($"value" === "A2")

    val flatMapCoGroupsInPandasUDF = PythonUDF("flagMapCoGroupsInPandasUDF", null,
      StructType(Seq(StructField("x", LongType), StructField("y", LongType))),
      Seq.empty,
      PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      true)

    // the number of keys must match
    val exception1 = intercept[IllegalArgumentException] {
      df1.groupBy($"key1", $"key2").flatMapCoGroupsInPandas(
        df2.groupBy($"key2"), Column(flatMapCoGroupsInPandasUDF))
    }
    assert(exception1.getMessage.contains("Cogroup keys must have same size: 2 != 1"))
    val exception2 = intercept[IllegalArgumentException] {
      df1.groupBy($"key1").flatMapCoGroupsInPandas(
        df2.groupBy($"key1", $"key2"), Column(flatMapCoGroupsInPandasUDF))
    }
    assert(exception2.getMessage.contains("Cogroup keys must have same size: 1 != 2"))

    // but different keys are allowed
    val actual = df1.groupBy($"key1").flatMapCoGroupsInPandas(
      df2.groupBy($"key2"), Column(flatMapCoGroupsInPandasUDF))
    // can't evaluate the DataFrame as there is no PythonFunction given
    assert(actual != null)
  }

  test("emptyDataFrame should be foldable") {
    val emptyDf = spark.emptyDataFrame.withColumn("id", lit(1L))
    val joined = spark.range(10).join(emptyDf, "id")
    joined.queryExecution.optimizedPlan match {
      case LocalRelation(Seq(id), Nil, _, _) =>
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
    checkErrorTableNotFound(e, "`nonexist`.`t`",
      ExpectedContext("nonexist.t", 25, 34))
  }

  test("SPARK-32680: Don't analyze CTAS with unresolved query") {
    val v2Source = classOf[FakeV2Provider].getName
    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE t USING $v2Source AS SELECT * from nonexist")
    }
    checkErrorTableNotFound(e, "`nonexist`",
      ExpectedContext("nonexist", s"CREATE TABLE t USING $v2Source AS SELECT * from ".length,
        s"CREATE TABLE t USING $v2Source AS SELECT * from nonexist".length - 1))
  }

  test("CalendarInterval reflection support") {
    val df = Seq((1, new CalendarInterval(1, 2, 3))).toDF("a", "b")
    checkAnswer(df.selectExpr("b"), Row(new CalendarInterval(1, 2, 3)))
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
    val ids = spark.range(10).repartition(5).select(
      Column.internalFn("distributed_sequence_id").alias("default_index"), col("id"))
    assert(ids.collect().map(_.getLong(0)).toSet === Range(0, 10).toSet)
    assert(ids.take(5).map(_.getLong(0)).toSet === Range(0, 5).toSet)
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
      val df4 = df3.distinct()
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
    val d1 = Seq("a").toDF()
    assert(d1.exceptAll(d1).count() === 0)
  }

  test("SPARK-39887: RemoveRedundantAliases should keep attributes of a Union's first child") {
    val df = sql(
      """
        |SELECT a, b AS a FROM (
        |  SELECT a, a AS b FROM (SELECT a FROM VALUES (1) AS t(a))
        |  UNION ALL
        |  SELECT a, b FROM (SELECT a, b FROM VALUES (1, 2) AS t(a, b))
        |)
        |""".stripMargin)
    val stringCols = df.logicalPlan.output.map(Column(_).cast(StringType))
    val castedDf = df.select(stringCols: _*)
    checkAnswer(castedDf, Row("1", "1") :: Row("1", "2") :: Nil)
  }

  test("SPARK-39887: RemoveRedundantAliases should keep attributes of a Union's first child 2") {
    val df = sql(
      """
        |SELECT
        |  to_date(a) a,
        |  to_date(b) b
        |FROM
        |  (
        |    SELECT
        |      a,
        |      a AS b
        |    FROM
        |      (
        |        SELECT
        |          to_date(a) a
        |        FROM
        |        VALUES
        |          ('2020-02-01') AS t1(a)
        |        GROUP BY
        |          to_date(a)
        |      ) t3
        |    UNION ALL
        |    SELECT
        |      a,
        |      b
        |    FROM
        |      (
        |        SELECT
        |          to_date(a) a,
        |          to_date(b) b
        |        FROM
        |        VALUES
        |          ('2020-01-01', '2020-01-02') AS t1(a, b)
        |        GROUP BY
        |          to_date(a),
        |          to_date(b)
        |      ) t4
        |  ) t5
        |GROUP BY
        |  to_date(a),
        |  to_date(b);
        |""".stripMargin)
    checkAnswer(df,
      Row(java.sql.Date.valueOf("2020-02-01"), java.sql.Date.valueOf("2020-02-01")) ::
        Row(java.sql.Date.valueOf("2020-01-01"), java.sql.Date.valueOf("2020-01-02")) :: Nil)
  }

  test("SPARK-39915: Dataset.repartition(N) may not create N partitions") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.sql("select * from values(1) where 1 < rand()").repartition(2)
      assert(df.queryExecution.executedPlan.execute().getNumPartitions == 2)
    }
  }

  test("SPARK-41048: Improve output partitioning and ordering with AQE cache") {
    withSQLConf(
        SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(10).selectExpr("cast(id as string) c1")
      val df2 = spark.range(10).selectExpr("cast(id as string) c2")
      val cached = df1.join(df2, $"c1" === $"c2").cache()
      cached.count()
      val executedPlan = cached.groupBy("c1").agg(max($"c2")).queryExecution.executedPlan
      // before is 2 sort and 1 shuffle
      assert(collect(executedPlan) {
        case s: ShuffleExchangeLike => s
      }.isEmpty)
      assert(collect(executedPlan) {
        case s: SortExec => s
      }.isEmpty)
    }
  }

  test("SPARK-41049: stateful expression should be copied correctly") {
    val df = spark.sparkContext.parallelize(1 to 5).toDF("x")
    val v1 = (rand() * 10000).cast(IntegerType)
    val v2 = to_csv(struct(v1.as("a"))) // to_csv is CodegenFallback
    df.select(v1, v1, v2, v2).collect().foreach { row =>
      assert(row.getInt(0) == row.getInt(1))
      assert(row.getInt(0).toString == row.getString(2))
      assert(row.getInt(0).toString == row.getString(3))
    }

    val v3 = map(lit("key"), lit("value"))
    val v4 = to_csv(struct(v3.as("a"))) // to_csv is CodegenFallback
    df.select(v3, v3, v4, v4).collect().foreach { row =>
      assert(row.getMap(0).toString() == row.getMap(1).toString())
      assert(row.getString(2) == s"{key -> ${row.getMap(0).get("key").get}}")
      assert(row.getString(3) == s"{key -> ${row.getMap(0).get("key").get}}")
    }
  }

  test("SPARK-45216: Non-deterministic functions with seed") {
    val df = Seq(Array.range(0, 10)).toDF("a")

    val r = rand()
    val r2 = randn()
    val r3 = random()
    val r4 = uuid()
    val r5 = shuffle(col("a"))
    df.select(r, r, r2, r2, r3, r3, r4, r4, r5, r5).collect().foreach { row =>
      (0 until 5).foreach(i => assert(row.get(i * 2) === row.get(i * 2 + 1)))
    }
  }

  test("SPARK-41219: IntegralDivide use decimal(1, 0) to represent 0") {
    val df = Seq("0.5944910").toDF("a")
    checkAnswer(df.selectExpr("cast(a as decimal(7,7)) div 100"), Row(0))
  }

  test("SPARK-44206: Dataset.selectExpr scope Session.active") {
    val _spark = spark.newSession()
    _spark.conf.set("spark.sql.legacy.interval.enabled", "true")
    val df1 = _spark.sql("select '2023-01-01'+ INTERVAL 1 YEAR as b")
    val df2 = _spark.sql("select '2023-01-01' as a").selectExpr("a + INTERVAL 1 YEAR as b")
    checkAnswer(df1, df2)
  }

  test("SPARK-44373: filter respects active session and it's params respects parser") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      checkError(
        exception = intercept[ParseException] {
          spark.range(1).toDF("CASE").filter("CASE").collect()
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'CASE'", "hint" -> ""))
    }
  }

  test("SPARK-44373: createTempView respects active session and it's params respects parser") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).createTempView("AUTHORIZATION")
        },
        condition = "_LEGACY_ERROR_TEMP_1321",
        parameters = Map("viewName" -> "AUTHORIZATION"))
    }
  }

  test("SPARK-46502: Unwrap timestamp cast on timestamp_ntz column") {
    def getQueryResult(ruleEnabled: Boolean): Seq[Row] = {
      val ruleName = if (ruleEnabled) {
        ""
      } else {
        "org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison"
      }

      var result: Seq[Row] = Seq.empty

      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
        withTable("table_timestamp") {
          sql(
            """
              |CREATE TABLE table_timestamp (
              |  batch TIMESTAMP_NTZ)
              |USING parquet;
              |""".stripMargin)
          sql("INSERT INTO table_timestamp SELECT CAST('2023-12-21 09:00:00' AS TIMESTAMP)")
          sql("INSERT INTO table_timestamp SELECT CAST('2023-12-21 10:00:00' AS TIMESTAMP)")
          sql("INSERT INTO table_timestamp SELECT CAST('2023-12-21 12:00:00' AS TIMESTAMP)")

          sql("CREATE OR REPLACE VIEW timestamp_view AS " +
            "SELECT CAST(batch AS TIMESTAMP) FROM table_timestamp")
          val df = sql("SELECT * from timestamp_view where batch >= '2023-12-21 10:00:00'")

          val filter = df.queryExecution.optimizedPlan.collect {
            case f: Filter => f
          }
          assert(filter.size == 1)

          val filterCondition = filter.head.condition
          val castExpr = filterCondition.collect {
            case c: Cast => c
          }

          if (ruleEnabled) {
            assert(castExpr.isEmpty)
          } else {
            assert(castExpr.size == 1)
          }

          result = df.collect().toSeq

          sql("DROP VIEW timestamp_view")
        }
      }
      result
    }

    val actual = getQueryResult(true).map(_.getTimestamp(0).toString).sorted
    val expected = getQueryResult(false).map(_.getTimestamp(0).toString).sorted
    assert(actual == expected)
  }
}

case class GroupByKey(a: Int, b: Int)

case class Bar2(s: String)

/**
 * This class is used for unit-testing. It's a logical plan whose output is passed in.
 */
case class OutputListAwareConstraintsTestPlan(
    outputList: Seq[Attribute]) extends LeafNode with MultiInstanceRelation {
  override def output: Seq[Attribute] = outputList

  override lazy val constraints: ExpressionSet = {
    val exprs = outputList.flatMap { attr =>
      attr.dataType match {
        case BooleanType => Some(EqualTo(attr, Literal(true, BooleanType)))
        case IntegerType => Some(GreaterThan(attr, Literal(5, IntegerType)))
        case _ => None
      }
    }
    ExpressionSet(exprs)
  }

  override def newInstance(): LogicalPlan = copy(outputList = outputList.map(_.newInstance()))
}


