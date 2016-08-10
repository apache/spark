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

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.util.Random

import org.scalatest.Matchers._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Union}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchange}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSQLContext}
import org.apache.spark.sql.test.SQLTestData.TestData2
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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

  test("union all") {
    val unionDF = testData.union(testData).union(testData)
      .union(testData).union(testData)

    // Before optimizer, Union should be combined.
    assert(unionDF.queryExecution.analyzed.collect {
      case j: Union if j.children.size == 5 => j }.size === 1)

    checkAnswer(
      unionDF.agg(avg('key), max('key), min('key), sum('key)),
      Row(50.5, 100, 1, 25250) :: Nil
    )
  }

  test("union should union DataFrames with UDTs (SPARK-13410)") {
    val rowRDD1 = sparkContext.parallelize(Seq(Row(1, new ExamplePoint(1.0, 2.0))))
    val schema1 = StructType(Array(StructField("label", IntegerType, false),
                    StructField("point", new ExamplePointUDT(), false)))
    val rowRDD2 = sparkContext.parallelize(Seq(Row(2, new ExamplePoint(3.0, 4.0))))
    val schema2 = StructType(Array(StructField("label", IntegerType, false),
                    StructField("point", new ExamplePointUDT(), false)))
    val df1 = spark.createDataFrame(rowRDD1, schema1)
    val df2 = spark.createDataFrame(rowRDD2, schema2)

    checkAnswer(
      df1.union(df2).orderBy("label"),
      Seq(Row(1, new ExamplePoint(1.0, 2.0)), Row(2, new ExamplePoint(3.0, 4.0)))
    )
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

  test("simple explode") {
    val df = Seq(Tuple1("a b c"), Tuple1("d e")).toDF("words")

    checkAnswer(
      df.explode("words", "word") { word: String => word.split(" ").toSeq }.select('word),
      Row("a") :: Row("b") :: Row("c") :: Row("d") ::Row("e") :: Nil
    )
  }

  test("explode") {
    val df = Seq((1, "a b c"), (2, "a b"), (3, "a")).toDF("number", "letters")
    val df2 =
      df.explode('letters) {
        case Row(letters: String) => letters.split(" ").map(Tuple1(_)).toSeq
      }

    checkAnswer(
      df2
        .select('_1 as 'letter, 'number)
        .groupBy('letter)
        .agg(countDistinct('number)),
      Row("a", 3) :: Row("b", 2) :: Row("c", 1) :: Nil
    )
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

  test("Star Expansion - explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1", "1,2"), ("2", "4"), ("3", "7,8,9")).toDF("prefix", "csv")
    val e = intercept[AnalysisException] {
      df.explode($"*") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      }.queryExecution.assertAnalyzed()
    }
    assert(e.getMessage.contains("Invalid usage of '*' in explode/json_tuple/UDTF"))

    checkAnswer(
      df.explode('prefix, 'csv) { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      },
      Row("1", "1,2", "1:1") ::
        Row("1", "1,2", "1:2") ::
        Row("2", "4", "2:4") ::
        Row("3", "7,8,9", "3:7") ::
        Row("3", "7,8,9", "3:8") ::
        Row("3", "7,8,9", "3:9") :: Nil)
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

  test("coalesce") {
    intercept[IllegalArgumentException] {
      testData.select('key).coalesce(0)
    }

    assert(testData.select('key).coalesce(1).rdd.partitions.size === 1)

    checkAnswer(
      testData.select('key).coalesce(1).select('key),
      testData.select('key).collect().toSeq)
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

  test("except") {
    checkAnswer(
      lowerCaseData.except(upperCaseData),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.except(lowerCaseData), Nil)
    checkAnswer(upperCaseData.except(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.except(nullInts.filter("0 = 1")),
      nullInts)
    checkAnswer(
      nullInts.except(nullInts),
      Nil)

    // check if values are de-duplicated
    checkAnswer(
      allNulls.except(allNulls.filter("0 = 1")),
      Row(null) :: Nil)
    checkAnswer(
      allNulls.except(allNulls),
      Nil)

    // check if values are de-duplicated
    val df = Seq(("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)).toDF("id", "value")
    checkAnswer(
      df.except(df.filter("0 = 1")),
      Row("id1", 1) ::
      Row("id", 1) ::
      Row("id1", 2) :: Nil)

    // check if the empty set on the left side works
    checkAnswer(
      allNulls.filter("0 = 1").except(allNulls),
      Nil)
  }

  test("except distinct - SQL compliance") {
    val df_left = Seq(1, 2, 2, 3, 3, 4).toDF("id")
    val df_right = Seq(1, 3).toDF("id")

    checkAnswer(
      df_left.except(df_right),
      Row(2) :: Row(4) :: Nil
    )
  }

  test("except - nullability") {
    val nonNullableInts = Seq(Tuple1(11), Tuple1(3)).toDF()
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.except(nullInts)
    checkAnswer(df1, Row(11) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.except(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(2) :: Row(null) :: Nil)
    assert(df2.schema.forall(_.nullable))

    val df3 = nullInts.except(nullInts)
    checkAnswer(df3, Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.except(nonNullableInts)
    checkAnswer(df4, Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("intersect") {
    checkAnswer(
      lowerCaseData.intersect(lowerCaseData),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersect(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.intersect(nullInts),
      Row(1) ::
      Row(2) ::
      Row(3) ::
      Row(null) :: Nil)

    // check if values are de-duplicated
    checkAnswer(
      allNulls.intersect(allNulls),
      Row(null) :: Nil)

    // check if values are de-duplicated
    val df = Seq(("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)).toDF("id", "value")
    checkAnswer(
      df.intersect(df),
      Row("id1", 1) ::
      Row("id", 1) ::
      Row("id1", 2) :: Nil)
  }

  test("intersect - nullability") {
    val nonNullableInts = Seq(Tuple1(1), Tuple1(3)).toDF()
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.intersect(nullInts)
    checkAnswer(df1, Row(1) :: Row(3) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.intersect(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(3) :: Nil)
    assert(df2.schema.forall(!_.nullable))

    val df3 = nullInts.intersect(nullInts)
    checkAnswer(df3, Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.intersect(nonNullableInts)
    checkAnswer(df4, Row(1) :: Row(3) :: Nil)
    assert(df4.schema.forall(!_.nullable))
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

  test("replace column using withColumn") {
    val df2 = sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
    val df3 = df2.withColumn("x", df2("x") + 1)
    checkAnswer(
      df3.select("x"),
      Row(2) :: Row(3) :: Row(4) :: Nil)
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

  test("describe") {
    val describeTestData = Seq(
      ("Bob", 16, 176),
      ("Alice", 32, 164),
      ("David", 60, 192),
      ("Amy", 24, 180)).toDF("name", "age", "height")

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

    val describeTwoCols = describeTestData.describe("name", "age", "height")
    assert(getSchemaAsSeq(describeTwoCols) === Seq("summary", "name", "age", "height"))
    checkAnswer(describeTwoCols, describeResult)
    // All aggregate value should have been cast to string
    describeTwoCols.collect().foreach { row =>
      assert(row.get(2).isInstanceOf[String], "expected string but found " + row.get(2).getClass)
      assert(row.get(3).isInstanceOf[String], "expected string but found " + row.get(3).getClass)
    }

    val describeAllCols = describeTestData.describe()
    assert(getSchemaAsSeq(describeAllCols) === Seq("summary", "name", "age", "height"))
    checkAnswer(describeAllCols, describeResult)

    val describeOneCol = describeTestData.describe("age")
    assert(getSchemaAsSeq(describeOneCol) === Seq("summary", "age"))
    checkAnswer(describeOneCol, describeResult.map { case Row(s, _, d, _) => Row(s, d)} )

    val describeNoCol = describeTestData.select("name").describe()
    assert(getSchemaAsSeq(describeNoCol) === Seq("summary", "name"))
    checkAnswer(describeNoCol, describeResult.map { case Row(s, n, _, _) => Row(s, n)} )

    val emptyDescription = describeTestData.limit(0).describe()
    assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
    checkAnswer(emptyDescription, emptyDescribeResult)
  }

  test("apply on query results (SPARK-5462)") {
    val df = testData.sparkSession.sql("select key from testData")
    checkAnswer(df.select(df("key")), testData.select('key).collect().toSeq)
  }

  test("inputFiles") {
    withTempDir { dir =>
      val df = Seq((1, 22)).toDF("a", "b")

      val parquetDir = new File(dir, "parquet").getCanonicalPath
      df.write.parquet(parquetDir)
      val parquetDF = spark.read.parquet(parquetDir)
      assert(parquetDF.inputFiles.nonEmpty)

      val jsonDir = new File(dir, "json").getCanonicalPath
      df.write.json(jsonDir)
      val jsonDF = spark.read.json(jsonDir)
      assert(parquetDF.inputFiles.nonEmpty)

      val unioned = jsonDF.union(parquetDF).inputFiles.sorted
      val allFiles = (jsonDF.inputFiles ++ parquetDF.inputFiles).distinct.sorted
      assert(unioned === allFiles)
    }
  }

  ignore("show") {
    // This test case is intended ignored, but to make sure it compiles correctly
    testData.select($"*").show()
    testData.select($"*").show(1000)
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

  test("showString(negative)") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
    assert(testData.select($"*").showString(-1) === expectedAnswer)
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

  test("SPARK-7327 show with empty dataFrame") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |""".stripMargin
    assert(testData.select($"*").filter($"key" < 0).showString(1) === expectedAnswer)
  }

  test("createDataFrame(RDD[Row], StructType) should convert UDTs (SPARK-6672)") {
    val rowRDD = sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0))))
    val schema = StructType(Array(StructField("point", new ExamplePointUDT(), false)))
    val df = spark.createDataFrame(rowRDD, schema)
    df.rdd.collect()
  }

  test("SPARK-6899: type should match when using codegen") {
    checkAnswer(decimalData.agg(avg('a)), Row(new java.math.BigDecimal(2.0)))
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
    assert(complexData.filter(complexData("a")(complexData("s")("key")) === 1).count() == 1)
  }

  test("SPARK-7551: support backticks for DataFrame attribute resolution") {
    val df = spark.read.json(sparkContext.makeRDD(
      """{"a.b": {"c": {"d..e": {"f": 1}}}}""" :: Nil))
    checkAnswer(
      df.select(df("`a.b`.c.`d..e`.`f`")),
      Row(1)
    )

    val df2 = spark.read.json(sparkContext.makeRDD(
      """{"a  b": {"c": {"d  e": {"f": 1}}}}""" :: Nil))
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

  test("SPARK-7150 range api") {
    // numSlice is greater than length
    val res1 = spark.range(0, 10, 1, 15).select("id")
    assert(res1.count == 10)
    assert(res1.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res2 = spark.range(3, 15, 3, 2).select("id")
    assert(res2.count == 4)
    assert(res2.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))

    val res3 = spark.range(1, -2).select("id")
    assert(res3.count == 0)

    // start is positive, end is negative, step is negative
    val res4 = spark.range(1, -2, -2, 6).select("id")
    assert(res4.count == 2)
    assert(res4.agg(sum("id")).as("sumid").collect() === Seq(Row(0)))

    // start, end, step are negative
    val res5 = spark.range(-3, -8, -2, 1).select("id")
    assert(res5.count == 3)
    assert(res5.agg(sum("id")).as("sumid").collect() === Seq(Row(-15)))

    // start, end are negative, step is positive
    val res6 = spark.range(-8, -4, 2, 1).select("id")
    assert(res6.count == 2)
    assert(res6.agg(sum("id")).as("sumid").collect() === Seq(Row(-14)))

    val res7 = spark.range(-10, -9, -20, 1).select("id")
    assert(res7.count == 0)

    val res8 = spark.range(Long.MinValue, Long.MaxValue, Long.MaxValue, 100).select("id")
    assert(res8.count == 3)
    assert(res8.agg(sum("id")).as("sumid").collect() === Seq(Row(-3)))

    val res9 = spark.range(Long.MaxValue, Long.MinValue, Long.MinValue, 100).select("id")
    assert(res9.count == 2)
    assert(res9.agg(sum("id")).as("sumid").collect() === Seq(Row(Long.MaxValue - 1)))

    // only end provided as argument
    val res10 = spark.range(10).select("id")
    assert(res10.count == 10)
    assert(res10.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res11 = spark.range(-1).select("id")
    assert(res11.count == 0)

    // using the default slice number
    val res12 = spark.range(3, 15, 3).select("id")
    assert(res12.count == 4)
    assert(res12.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))
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
    assert(e.getMessage.contains("Duplicate column(s)"))
    assert(e.getMessage.contains("column1"))
    assert(!e.getMessage.contains("column2"))

    // multiple duplicate columns present
    val f = intercept[org.apache.spark.sql.AnalysisException] {
      Seq((1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7))
        .toDF("column1", "column2", "column3", "column1", "column3")
        .write.format("json").save("temp")
    }
    assert(f.getMessage.contains("Duplicate column(s)"))
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
      Dataset.ofRows(spark, OneRowRelation).createOrReplaceTempView("one_row")
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
    import org.apache.spark.util.random.XORShiftRandom

    val seed = 33
    val df = (1 to 100).map(Tuple1.apply).toDF("i")
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
    val df = spark.read.json(sparkContext.makeRDD(
      """{"a": {"b": 1}}""" :: Nil))
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
    val input = spark.read.json(spark.sparkContext.makeRDD(
      (1 to 10).map(i => s"""{"id": $i}""")))

    val df = input.select($"id", rand(0).as('r))
    df.as("a").join(df.filter($"r" < 0.5).as("b"), $"a.id" === $"b.id").collect().foreach { row =>
      assert(row.getDouble(1) - row.getDouble(3) === 0.0 +- 0.001)
    }
  }

  test("SPARK-10539: Project should not be pushed down through Intersect or Except") {
    val df1 = (1 to 100).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 30).map(Tuple1.apply).toDF("i")
    val intersect = df1.intersect(df2)
    val except = df1.except(df2)
    assert(intersect.count() === 30)
    assert(except.count() === 70)
  }

  test("SPARK-10740: handle nondeterministic expressions correctly for set operations") {
    val df1 = (1 to 20).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 10).map(Tuple1.apply).toDF("i")

    // When generating expected results at here, we need to follow the implementation of
    // Rand expression.
    def expected(df: DataFrame): Seq[Row] = {
      df.rdd.collectPartitions().zipWithIndex.flatMap {
        case (data, index) =>
          val rng = new org.apache.spark.util.random.XORShiftRandom(7 + index)
          data.filter(_.getInt(0) < rng.nextDouble() * 10)
      }
    }

    val union = df1.union(df2)
    checkAnswer(
      union.filter('i < rand(7) * 10),
      expected(union)
    )
    checkAnswer(
      union.select(rand(7)),
      union.rdd.collectPartitions().zipWithIndex.flatMap {
        case (data, index) =>
          val rng = new org.apache.spark.util.random.XORShiftRandom(7 + index)
          data.map(_ => rng.nextDouble()).map(i => Row(i))
      }
    )

    val intersect = df1.intersect(df2)
    checkAnswer(
      intersect.filter('i < rand(7) * 10),
      expected(intersect)
    )

    val except = df1.except(df2)
    checkAnswer(
      except.filter('i < rand(7) * 10),
      expected(except)
    )
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
      case e: ShuffleExchange => atFirstAgg = false
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
        new StructType().add("f1", IntegerType).add("f2", IntegerType),
        needsConversion = false).select($"F1", $"f2".as("f2"))
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
      new java.lang.Integer(22) -> "John",
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
        join.queryExecution.executedPlan.collect { case e: ShuffleExchange => true }.size === 1)
      assert(
        join.queryExecution.executedPlan.collect { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        join2.queryExecution.executedPlan.collect { case e: ShuffleExchange => true }.size === 1)
      assert(
        join2.queryExecution.executedPlan
          .collect { case e: BroadcastExchangeExec => true }.size === 1)
      assert(
        join2.queryExecution.executedPlan.collect { case e: ReusedExchangeExec => true }.size === 4)
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
    // invalid table name test as below
    intercept[AnalysisException](df.createOrReplaceTempView("t~"))
    // valid table name test as below
    df.createOrReplaceTempView("table1")
    // another invalid table name test as below
    intercept[AnalysisException](df.createOrReplaceTempView("#$@sum"))
    // another invalid table name test as below
    intercept[AnalysisException](df.createOrReplaceTempView("table!#"))
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

  test("SPARK-16938: `drop/dropDuplicate` should handle qualified column names") {
    val dfa = Seq((1, 2), (2, 3)).toDF("id", "a").alias("dfa")
    val dfb = Seq((1, 0), (1, 1)).toDF("id", "b").alias("dfb")

    checkAnswer(dfa.drop("dfa.id"), Row(2) :: Row(3) :: Nil)
    checkAnswer(dfa.drop("id"), Row(2) :: Row(3) :: Nil)
    checkAnswer(dfa.drop($"dfa.id"), Row(2) :: Row(3) :: Nil)
    checkAnswer(dfa.drop($"id"), Row(2) :: Row(3) :: Nil)

    checkAnswer(dfb.dropDuplicates(Array("dfb.id")), Row(1, 0) :: Nil)
    checkAnswer(dfb.dropDuplicates(Array("dfb.b")), Row(1, 0) :: Row(1, 1) :: Nil)
    checkAnswer(dfb.dropDuplicates(Array("id")), Row(1, 0) :: Nil)
    checkAnswer(dfb.dropDuplicates(Array("b")), Row(1, 0) :: Row(1, 1) :: Nil)

    checkAnswer(dfa.join(dfb, dfa("id") === dfb("id")).dropDuplicates(Array("dfa.id", "dfb.id")),
      Row(1, 2, 1, 1) :: Nil)
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
    val df = spark.createDataFrame(rdd, StructType(schemas), false)
    assert(df.persist.take(1).apply(0).toSeq(100).asInstanceOf[Long] == 100)
  }

  test("copy results for sampling with replacement") {
    val df = Seq((1, 0), (2, 0), (3, 0)).toDF("a", "b")
    val sampleDf = df.sample(true, 2.00)
    val d = sampleDf.withColumn("c", monotonically_increasing_id).select($"c").collect
    assert(d.size == d.distinct.size)
  }
}
