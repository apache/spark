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
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.UUID

import scala.util.Random

import org.scalatest.Matchers._

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Uuid
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Union}
import org.apache.spark.sql.execution.{FilterExec, QueryExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSQLContext}
import org.apache.spark.sql.test.SQLTestData.{NullStrings, TestData2}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

class DataFrameSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("analysis error should be eagerly reported") {
    intercept[Exception] { testData.select('nonExistentName) }
    intercept[Exception] {
      testData.groupBy('key).agg(Map("nonExistentName" -> "sum"))
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
    assert(complexData.filter(complexData("m").getItem("1") === 1).count() == 1)
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

  test("head and take") {
    assert(testData.take(2) === testData.collect().take(2))
    assert(testData.head(2) === testData.collect().take(2))
    assert(testData.head(2).head.schema === testData.schema)
  }

  test("dataframe alias") {
    val df = Seq(Tuple1(1)).toDF("c").as("t")
    val dfAlias = df.alias("t2")
    df.col("t.c")
    dfAlias.col("t2.c")
  }

  test("Star Expansion - CreateStruct and CreateArray") {
    val structDf = testData2.select("a", "b").as("record")
    // CreateStruct and CreateArray in aggregateExpressions
    assert(structDf.groupBy($"a").agg(min(struct($"record.*"))).first() == Row(3, Row(3, 1)))
    assert(structDf.groupBy($"a").agg(min(array($"record.*"))).first() == Row(3, Seq(3, 1)))

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
      df.select(explode(split($"csv", ","))),
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
      testData.select('key).repartition(0)
    }

    checkAnswer(
      testData.select('key).repartition(10).select('key),
      testData.select('key).collect().toSeq)
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
      testData.select('key).coalesce(0)
    }

    assert(testData.select('key).coalesce(1).rdd.partitions.size === 1)

    checkAnswer(
      testData.select('key).coalesce(1).select('key),
      testData.select('key).collect().toSeq)

    assert(spark.emptyDataFrame.coalesce(1).rdd.partitions.size === 1)
  }

  test("convert $\"attribute name\" into unresolved attribute") {
    checkAnswer(
      testData.where($"key" === lit(1)).select($"value"),
      Row("1"))
  }

  test("convert Scala Symbol 'attrname into unresolved attribute") {
    checkAnswer(
      testData.where('key === lit(1)).select('value),
      Row("1"))
  }

  test("select *") {
    checkAnswer(
      testData.select($"*"),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      testData.where('key === lit(1)).select('value),
      Row("1"))
  }

  test("select with functions") {
    checkAnswer(
      testData.select(sum('value), avg('value), count(lit(1))),
      Row(5050.0, 50.5, 100))

    checkAnswer(
      testData2.select('a + 'b, 'a < 'b),
      Seq(
        Row(2, false),
        Row(3, true),
        Row(3, false),
        Row(4, false),
        Row(4, false),
        Row(5, false)))

    checkAnswer(
      testData2.select(sumDistinct('a)),
      Row(6))
  }

  test("sorting with null ordering") {
    val data = Seq[java.lang.Integer](2, 1, null).toDF("key")

    checkAnswer(data.orderBy('key.asc), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy(asc("key")), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy('key.asc_nulls_first), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy(asc_nulls_first("key")), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy('key.asc_nulls_last), Row(1) :: Row(2) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(asc_nulls_last("key")), Row(1) :: Row(2) :: Row(null) :: Nil)

    checkAnswer(data.orderBy('key.desc), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(desc("key")), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy('key.desc_nulls_first), Row(null) :: Row(2) :: Row(1) :: Nil)
    checkAnswer(data.orderBy(desc_nulls_first("key")), Row(null) :: Row(2) :: Row(1) :: Nil)
    checkAnswer(data.orderBy('key.desc_nulls_last), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(desc_nulls_last("key")), Row(2) :: Row(1) :: Row(null) :: Nil)
  }

  test("global sorting") {
    checkAnswer(
      testData2.orderBy('a.asc, 'b.asc),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    checkAnswer(
      testData2.orderBy(asc("a"), desc("b")),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy('a.asc, 'b.desc),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.desc),
      Seq(Row(3, 2), Row(3, 1), Row(2, 2), Row(2, 1), Row(1, 2), Row(1, 1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.asc),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2)))

    checkAnswer(
      arrayData.toDF().orderBy('data.getItem(0).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy('data.getItem(0).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).reverse.toSeq)

    checkAnswer(
      arrayData.toDF().orderBy('data.getItem(1).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy('data.getItem(1).desc),
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

  test("udf") {
    val foo = udf((a: Int, b: String) => a.toString + b)

    checkAnswer(
      // SELECT *, foo(key, value) FROM testData
      testData.select($"*", foo('key, 'value)).limit(3),
      Row(1, "1", "11") :: Row(2, "2", "22") :: Row(3, "3", "33") :: Nil
    )
  }

  test("callUDF without Hive Support") {
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    df.sparkSession.udf.register("simpleUDF", (v: Int) => v * v)
    checkAnswer(
      df.select($"id", callUDF("simpleUDF", $"value")),
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

  test("withColumns") {
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

  test("withColumns: case sensitive") {
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

  test("withColumns: given metadata") {
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

  test("replace column using withColumn") {
    val df2 = sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
    val df3 = df2.withColumn("x", df2("x") + 1)
    checkAnswer(
      df3.select("x"),
      Row(2) :: Row(3) :: Row(4) :: Nil)
  }

  test("replace column using withColumns") {
    val df2 = sparkContext.parallelize(Array((1, 2), (2, 3), (3, 4))).toDF("x", "y")
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

  test("drop(name: String) search and drop all top level columns that matchs the name") {
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
    checkAnswer(describeOneCol, describeResult.map { case Row(s, _, d, _) => Row(s, d)} )

    val describeNoCol = person2.select().describe()
    assert(getSchemaAsSeq(describeNoCol) === Seq("summary"))
    checkAnswer(describeNoCol, describeResult.map { case Row(s, _, _, _) => Row(s)} )

    val emptyDescription = person2.limit(0).describe()
    assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
    checkAnswer(emptyDescription, emptyDescribeResult)
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
    checkAnswer(summaryOneCol, summaryResult.map { case Row(s, _, d, _) => Row(s, d)} )

    val summaryNoCol = person2.select().summary()
    assert(getSchemaAsSeq(summaryNoCol) === Seq("summary"))
    checkAnswer(summaryNoCol, summaryResult.map { case Row(s, _, _, _) => Row(s)} )

    val emptyDescription = person2.limit(0).summary()
    assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
    checkAnswer(emptyDescription, emptySummaryResult)
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
    checkAnswer(df.select(df("key")), testData.select('key).collect().toSeq)
  }

  test("inputFiles") {
    Seq("csv", "").foreach { useV1List =>
      withSQLConf(SQLConf.USE_V1_SOURCE_READER_LIST.key -> useV1List) {
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
         ||[1 -> a, 2 -> b]|
         |+----------------+
         |""".stripMargin)
    val df3 = Seq(((1, "a"), 0), ((2, "b"), 0)).toDF("a", "b")
    assert(df3.showString(10) ===
      s"""+------+---+
         ||     a|  b|
         |+------+---+
         ||[1, a]|  0|
         ||[2, b]|  0|
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

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "GMT") {

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

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "GMT") {

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
    checkAnswer(decimalData.agg(avg('a)), Row(new java.math.BigDecimal(2)))
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
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
      upperCaseData.filter('N > 1).select('N).filter('N < 6).orderBy('L.asc),
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
    val query1 = df.groupBy('i)
      .agg(max('j).as("aggOrder"))
      .orderBy(sum('j))
    checkAnswer(query1, Row(1, 2))

    // In the plan, there are two attributes having the same name 'havingCondition'
    // One is a user-provided alias name; another is an internally generated one.
    val query2 = df.groupBy('i)
      .agg(max('j).as("havingCondition"))
      .where(sum('j) > 0)
      .orderBy('havingCondition.asc)
    checkAnswer(query2, Row(1, 2))
  }

  test("SPARK-10316: respect non-deterministic expressions in PhysicalOperation") {
    withTempDir { dir =>
      (1 to 10).toDF("id").write.mode(SaveMode.Overwrite).json(dir.getCanonicalPath)
      val input = spark.read.json(dir.getCanonicalPath)

      val df = input.select($"id", rand(0).as('r))
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
    val df4 = data.repartition($"a").sortWithinPartitions($"b".desc)
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
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(
        join.queryExecution.executedPlan.collect { case e: ShuffleExchangeExec => true }.size === 1)
      assert(
        join.queryExecution.executedPlan.collect { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        join2.queryExecution.executedPlan.collect { case e: ShuffleExchangeExec => true }.size == 1)
      assert(
        join2.queryExecution.executedPlan
          .collect { case e: BroadcastExchangeExec => true }.size === 1)
      assert(
        join2.queryExecution.executedPlan.collect { case e: ReusedExchangeExec => true }.size == 4)
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
      val m = intercept[AnalysisException](df.createOrReplaceTempView(name)).getMessage
      assert(m.contains(s"Invalid view name: $name"))
    }

    // valid table names
    Seq("table1", "`11111`", "`t~`", "`#$@sum`", "`table!#`").foreach { name =>
      df.createOrReplaceTempView(name)
    }
  }

  test("assertAnalyzed shouldn't replace original stack trace") {
    val e = intercept[AnalysisException] {
      spark.range(1).select('id as 'a, 'id as 'b).groupBy('a).agg('b)
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
      testData2.select(lit(7), 'a, 'b).orderBy(lit(1), lit(2), lit(3)),
      Seq(Row(7, 1, 1), Row(7, 1, 2), Row(7, 2, 1), Row(7, 2, 2), Row(7, 3, 1), Row(7, 3, 2)))
  }

  test("SPARK-22271: mean overflows and returns null for some decimal variables") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df.select('DecimalCol cast DecimalType(38, 33))
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
      .select(when('cond, struct(lit("a").as("val1"), lit(10).as("val2"))).otherwise('s) as "res")
      .select('res.getField("val1"))
    def arrayWhenDF: DataFrame = sourceDF
      .select(when('cond, array(lit("a"), lit("b"))).otherwise('a) as "res")
      .select('res.getItem(0))
    def mapWhenDF: DataFrame = sourceDF
      .select(when('cond, map(lit(0), lit("a"))).otherwise('m) as "res")
      .select('res.getItem(0))

    def structIfDF: DataFrame = sourceDF
      .select(expr("if(cond, struct('a' as val1, 10 as val2), s)") as "res")
      .select('res.getField("val1"))
    def arrayIfDF: DataFrame = sourceDF
      .select(expr("if(cond, array('a', 'b'), a)") as "res")
      .select('res.getItem(0))
    def mapIfDF: DataFrame = sourceDF
      .select(expr("if(cond, map(0, 'a'), m)") as "res")
      .select('res.getItem(0))

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

      val aggPlusFilter1 = df.groupBy(df("name")).agg(count(df("name"))).filter(df("name") === 0)
      val aggPlusFilter2 = df.groupBy(col("name")).agg(count(col("name"))).filter(col("name") === 0)
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

      var numJobs = 0
      sparkContext.addSparkListener(new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          numJobs += 1
        }
      })

      val df = spark.read.json(path.getCanonicalPath)
      assert(df.columns === Array("i", "p"))
      spark.sparkContext.listenerBus.waitUntilEmpty(10000)
      assert(numJobs == 1)
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
          |+- 'UnresolvedRelation `tmp`""".stripMargin))
      assert(output.contains(
        """== Physical Plan ==
          |*(1) Range (0, 10, step=1, splits=2)""".stripMargin))
    }
  }
}
