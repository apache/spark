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

import scala.language.postfixOps

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.{ExamplePointUDT, ExamplePoint, TestSQLContext}
import org.apache.spark.sql.test.TestSQLContext.implicits._


class DataFrameSuite extends QueryTest {
  import org.apache.spark.sql.TestData._

  test("analysis error should be eagerly reported") {
    val oldSetting = TestSQLContext.conf.dataFrameEagerAnalysis
    // Eager analysis.
    TestSQLContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, "true")

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

    // No more eager analysis once the flag is turned off
    TestSQLContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, "false")
    testData.select('nonExistentName)

    // Set the flag back to original value before this test.
    TestSQLContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, oldSetting.toString)
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

  test("invalid plan toString, debug mode") {
    val oldSetting = TestSQLContext.conf.dataFrameEagerAnalysis
    TestSQLContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, "true")

    // Turn on debug mode so we can see invalid query plans.
    import org.apache.spark.sql.execution.debug._
    TestSQLContext.debug()

    val badPlan = testData.select('badColumn)

    assert(badPlan.toString contains badPlan.queryExecution.toString,
      "toString on bad query plans should include the query execution but was:\n" +
        badPlan.toString)

    // Set the flag back to original value before this test.
    TestSQLContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, oldSetting.toString)
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
    assert(TestSQLContext.emptyDataFrame.columns.toSeq === Seq.empty[String])
    assert(TestSQLContext.emptyDataFrame.count() === 0)
  }

  test("head and take") {
    assert(testData.take(2) === testData.collect().take(2))
    assert(testData.head(2) === testData.collect().take(2))
    assert(testData.head(2).head.schema === testData.schema)
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

  test("explode alias and star") {
    val df = Seq((Array("a"), 1)).toDF("a", "b")

    checkAnswer(
      df.select(explode($"a").as("a"), $"*"),
      Row("a", Seq("a"), 1) :: Nil)
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

  test("filterExpr") {
    checkAnswer(
      testData.filter("key > 90"),
      testData.collect().filter(_.getInt(0) > 90).toSeq)
  }

  test("repartition") {
    checkAnswer(
      testData.select('key).repartition(10).select('key),
      testData.select('key).collect().toSeq)
  }

  test("coalesce") {
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
  }

  test("intersect") {
    checkAnswer(
      lowerCaseData.intersect(lowerCaseData),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersect(upperCaseData), Nil)
  }

  test("udf") {
    val foo = udf((a: Int, b: String) => a.toString + b)

    checkAnswer(
      // SELECT *, foo(key, value) FROM testData
      testData.select($"*", foo('key, 'value)).limit(3),
      Row(1, "1", "11") :: Row(2, "2", "22") :: Row(3, "3", "33") :: Nil
    )
  }

  test("call udf in SQLContext") {
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    val sqlctx = df.sqlContext
    sqlctx.udf.register("simpleUdf", (v: Int) => v * v)
    checkAnswer(
      df.select($"id", callUdf("simpleUdf", $"value")),
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
    val df2 = TestSQLContext.sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
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

  test("drop unknown column with same name (no-op) with column reference") {
    val col = Column("key")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
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

  test("randomSplit") {
    val n = 600
    val data = TestSQLContext.sparkContext.parallelize(1 to n, 2).toDF("id")
    for (seed <- 1 to 5) {
      val splits = data.randomSplit(Array[Double](1, 2, 3), seed)
      assert(splits.length == 3, "wrong number of splits")

      assert(splits.reduce((a, b) => a.unionAll(b)).sort("id").collect().toList ==
        data.collect().toList, "incomplete or wrong split")

      val s = splits.map(_.count())
      assert(math.abs(s(0) - 100) < 50) // std =  9.13
      assert(math.abs(s(1) - 200) < 50) // std = 11.55
      assert(math.abs(s(2) - 300) < 50) // std = 12.25
    }
  }

  test("describe") {
    val describeTestData = Seq(
      ("Bob", 16, 176),
      ("Alice", 32, 164),
      ("David", 60, 192),
      ("Amy", 24, 180)).toDF("name", "age", "height")

    val describeResult = Seq(
      Row("count", "4", "4"),
      Row("mean", "33.0", "178.0"),
      Row("stddev", "16.583123951777", "10.0"),
      Row("min", "16", "164"),
      Row("max", "60", "192"))

    val emptyDescribeResult = Seq(
      Row("count", "0", "0"),
      Row("mean", null, null),
      Row("stddev", null, null),
      Row("min", null, null),
      Row("max", null, null))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    val describeTwoCols = describeTestData.describe("age", "height")
    assert(getSchemaAsSeq(describeTwoCols) === Seq("summary", "age", "height"))
    checkAnswer(describeTwoCols, describeResult)
    // All aggregate value should have been cast to string
    describeTwoCols.collect().foreach { row =>
      assert(row.get(1).isInstanceOf[String], "expected string but found " + row.get(1).getClass)
      assert(row.get(2).isInstanceOf[String], "expected string but found " + row.get(2).getClass)
    }

    val describeAllCols = describeTestData.describe()
    assert(getSchemaAsSeq(describeAllCols) === Seq("summary", "age", "height"))
    checkAnswer(describeAllCols, describeResult)

    val describeOneCol = describeTestData.describe("age")
    assert(getSchemaAsSeq(describeOneCol) === Seq("summary", "age"))
    checkAnswer(describeOneCol, describeResult.map { case Row(s, d, _) => Row(s, d)} )

    val describeNoCol = describeTestData.select("name").describe()
    assert(getSchemaAsSeq(describeNoCol) === Seq("summary"))
    checkAnswer(describeNoCol, describeResult.map { case Row(s, _, _) => Row(s)} )

    val emptyDescription = describeTestData.limit(0).describe()
    assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "age", "height"))
    checkAnswer(emptyDescription, emptyDescribeResult)
  }

  test("apply on query results (SPARK-5462)") {
    val df = testData.sqlContext.sql("select key from testData")
    checkAnswer(df.select(df("key")), testData.select('key).collect().toSeq)
  }

  ignore("show") {
    // This test case is intended ignored, but to make sure it compiles correctly
    testData.select($"*").show()
    testData.select($"*").show(1000)
  }

  test("SPARK-7319 showString") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           ||  1|    1|
                           |+---+-----+
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
    val rowRDD = TestSQLContext.sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0))))
    val schema = StructType(Array(StructField("point", new ExamplePointUDT(), false)))
    val df = TestSQLContext.createDataFrame(rowRDD, schema)
    df.rdd.collect()
  }

  test("SPARK-6899") {
    val originalValue = TestSQLContext.conf.codegenEnabled
    TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, "true")
    checkAnswer(
      decimalData.agg(avg('a)),
      Row(new java.math.BigDecimal(2.0)))
    TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
  }

  test("SPARK-7551: support backticks for DataFrame attribute resolution") {
    val df = TestSQLContext.read.json(TestSQLContext.sparkContext.makeRDD(
      """{"a.b": {"c": {"d..e": {"f": 1}}}}""" :: Nil))
    checkAnswer(
      df.select(df("`a.b`.c.`d..e`.`f`")),
      Row(1)
    )

    val df2 = TestSQLContext.read.json(TestSQLContext.sparkContext.makeRDD(
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
    val testData = TestSQLContext.sparkContext.parallelize(
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
  }

  test("SPARK-7276: Project collapse for continuous select") {
    var df = testData
    for (i <- 1 to 5) {
      df = df.select($"*")
    }

    import org.apache.spark.sql.catalyst.plans.logical.Project
    // make sure df have at most two Projects
    val p = df.logicalPlan.asInstanceOf[Project].child.asInstanceOf[Project]
    assert(!p.child.isInstanceOf[Project])
  }

  test("SPARK-7150 range api") {
    // numSlice is greater than length
    val res1 = TestSQLContext.range(0, 10, 1, 15).select("id")
    assert(res1.count == 10)
    assert(res1.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res2 = TestSQLContext.range(3, 15, 3, 2).select("id")
    assert(res2.count == 4)
    assert(res2.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))

    val res3 = TestSQLContext.range(1, -2).select("id")
    assert(res3.count == 0)

    // start is positive, end is negative, step is negative
    val res4 = TestSQLContext.range(1, -2, -2, 6).select("id")
    assert(res4.count == 2)
    assert(res4.agg(sum("id")).as("sumid").collect() === Seq(Row(0)))

    // start, end, step are negative
    val res5 = TestSQLContext.range(-3, -8, -2, 1).select("id")
    assert(res5.count == 3)
    assert(res5.agg(sum("id")).as("sumid").collect() === Seq(Row(-15)))

    // start, end are negative, step is positive
    val res6 = TestSQLContext.range(-8, -4, 2, 1).select("id")
    assert(res6.count == 2)
    assert(res6.agg(sum("id")).as("sumid").collect() === Seq(Row(-14)))

    val res7 = TestSQLContext.range(-10, -9, -20, 1).select("id")
    assert(res7.count == 0)

    val res8 = TestSQLContext.range(Long.MinValue, Long.MaxValue, Long.MaxValue, 100).select("id")
    assert(res8.count == 3)
    assert(res8.agg(sum("id")).as("sumid").collect() === Seq(Row(-3)))

    val res9 = TestSQLContext.range(Long.MaxValue, Long.MinValue, Long.MinValue, 100).select("id")
    assert(res9.count == 2)
    assert(res9.agg(sum("id")).as("sumid").collect() === Seq(Row(Long.MaxValue - 1)))

    // only end provided as argument
    val res10 = TestSQLContext.range(10).select("id")
    assert(res10.count == 10)
    assert(res10.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res11 = TestSQLContext.range(-1).select("id")
    assert(res11.count == 0)
  }

  test("SPARK-8621: support empty string column name") {
    val df = Seq(Tuple1(1)).toDF("").as("t")
    // We should allow empty string as column name
    df.col("")
    df.col("t.``")
  }
}
