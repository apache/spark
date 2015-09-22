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

import scala.language.postfixOps
import scala.util.Random

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.{ExamplePointUDT, ExamplePoint, SharedSQLContext}

class DataFrameSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("analysis error should be eagerly reported") {
    // Eager analysis.
    withSQLConf(SQLConf.DATAFRAME_EAGER_ANALYSIS.key -> "true") {
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

    // No more eager analysis once the flag is turned off
    withSQLConf(SQLConf.DATAFRAME_EAGER_ANALYSIS.key -> "false") {
      testData.select('nonExistentName)
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

  test("invalid plan toString, debug mode") {
    // Turn on debug mode so we can see invalid query plans.
    import org.apache.spark.sql.execution.debug._

    withSQLConf(SQLConf.DATAFRAME_EAGER_ANALYSIS.key -> "true") {
      sqlContext.debug()

      val badPlan = testData.select('badColumn)

      assert(badPlan.toString contains badPlan.queryExecution.toString,
        "toString on bad query plans should include the query execution but was:\n" +
          badPlan.toString)
    }
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
    assert(sqlContext.emptyDataFrame.columns.toSeq === Seq.empty[String])
    assert(sqlContext.emptyDataFrame.count() === 0)
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

  test("SPARK-8930: explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1", "1,2"), ("2", "4"), ("3", "7,8,9")).toDF("prefix", "csv")
    val e = intercept[AnalysisException] {
      df.explode($"*") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      }.queryExecution.assertAnalyzed()
    }
    assert(e.getMessage.contains(
      "Cannot explode *, explode can only be applied on a specific column."))

    df.explode('prefix, 'csv) { case Row(prefix: String, csv: String) =>
      csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
    }.queryExecution.assertAnalyzed()
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

  test("filterExpr using where") {
    checkAnswer(
      testData.where("key > 50"),
      testData.collect().filter(_.getInt(0) > 50).toSeq)
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

  test("deprecated callUdf in SQLContext") {
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    val sqlctx = df.sqlContext
    sqlctx.udf.register("simpleUdf", (v: Int) => v * v)
    checkAnswer(
      df.select($"id", callUdf("simpleUdf", $"value")),
      Row("id1", 1) :: Row("id2", 16) :: Row("id3", 25) :: Nil)
  }

  test("callUDF in SQLContext") {
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    val sqlctx = df.sqlContext
    sqlctx.udf.register("simpleUDF", (v: Int) => v * v)
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
    val df2 = sqlContext.sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
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

  test("inputFiles") {
    withTempDir { dir =>
      val df = Seq((1, 22)).toDF("a", "b")

      val parquetDir = new File(dir, "parquet").getCanonicalPath
      df.write.parquet(parquetDir)
      val parquetDF = sqlContext.read.parquet(parquetDir)
      assert(parquetDF.inputFiles.nonEmpty)

      val jsonDir = new File(dir, "json").getCanonicalPath
      df.write.json(jsonDir)
      val jsonDF = sqlContext.read.json(jsonDir)
      assert(parquetDF.inputFiles.nonEmpty)

      val unioned = jsonDF.unionAll(parquetDF).inputFiles.sorted
      val allFiles = (jsonDF.inputFiles ++ parquetDF.inputFiles).toSet.toArray.sorted
      assert(unioned === allFiles)
    }
  }

  ignore("show") {
    // This test case is intended ignored, but to make sure it compiles correctly
    testData.select($"*").show()
    testData.select($"*").show(1000)
  }

  test("showString: truncate = [true, false]") {
    val longString = Array.fill(21)("1").mkString
    val df = sqlContext.sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = """+---------------------+
                                   ||_1                   |
                                   |+---------------------+
                                   ||1                    |
                                   ||111111111111111111111|
                                   |+---------------------+
                                   |""".stripMargin
    assert(df.showString(10, false) === expectedAnswerForFalse)
    val expectedAnswerForTrue = """+--------------------+
                                  ||                  _1|
                                  |+--------------------+
                                  ||                   1|
                                  ||11111111111111111...|
                                  |+--------------------+
                                  |""".stripMargin
    assert(df.showString(10, true) === expectedAnswerForTrue)
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
    val rowRDD = sqlContext.sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0))))
    val schema = StructType(Array(StructField("point", new ExamplePointUDT(), false)))
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.rdd.collect()
  }

  test("SPARK-6899: type should match when using codegen") {
    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
      checkAnswer(
        decimalData.agg(avg('a)),
        Row(new java.math.BigDecimal(2.0)))
    }
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
    assert(complexData.filter(complexData("a")(complexData("s")("key")) === 1).count() == 1)
  }

  test("SPARK-7551: support backticks for DataFrame attribute resolution") {
    val df = sqlContext.read.json(sqlContext.sparkContext.makeRDD(
      """{"a.b": {"c": {"d..e": {"f": 1}}}}""" :: Nil))
    checkAnswer(
      df.select(df("`a.b`.c.`d..e`.`f`")),
      Row(1)
    )

    val df2 = sqlContext.read.json(sqlContext.sparkContext.makeRDD(
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
    val testData = sqlContext.sparkContext.parallelize(
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

  test("SPARK-7150 range api") {
    // numSlice is greater than length
    val res1 = sqlContext.range(0, 10, 1, 15).select("id")
    assert(res1.count == 10)
    assert(res1.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res2 = sqlContext.range(3, 15, 3, 2).select("id")
    assert(res2.count == 4)
    assert(res2.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))

    val res3 = sqlContext.range(1, -2).select("id")
    assert(res3.count == 0)

    // start is positive, end is negative, step is negative
    val res4 = sqlContext.range(1, -2, -2, 6).select("id")
    assert(res4.count == 2)
    assert(res4.agg(sum("id")).as("sumid").collect() === Seq(Row(0)))

    // start, end, step are negative
    val res5 = sqlContext.range(-3, -8, -2, 1).select("id")
    assert(res5.count == 3)
    assert(res5.agg(sum("id")).as("sumid").collect() === Seq(Row(-15)))

    // start, end are negative, step is positive
    val res6 = sqlContext.range(-8, -4, 2, 1).select("id")
    assert(res6.count == 2)
    assert(res6.agg(sum("id")).as("sumid").collect() === Seq(Row(-14)))

    val res7 = sqlContext.range(-10, -9, -20, 1).select("id")
    assert(res7.count == 0)

    val res8 = sqlContext.range(Long.MinValue, Long.MaxValue, Long.MaxValue, 100).select("id")
    assert(res8.count == 3)
    assert(res8.agg(sum("id")).as("sumid").collect() === Seq(Row(-3)))

    val res9 = sqlContext.range(Long.MaxValue, Long.MinValue, Long.MinValue, 100).select("id")
    assert(res9.count == 2)
    assert(res9.agg(sum("id")).as("sumid").collect() === Seq(Row(Long.MaxValue - 1)))

    // only end provided as argument
    val res10 = sqlContext.range(10).select("id")
    assert(res10.count == 10)
    assert(res10.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res11 = sqlContext.range(-1).select("id")
    assert(res11.count == 0)
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
    assert(e.getMessage.contains("parquet"))
    assert(e.getMessage.contains("column1"))
    assert(!e.getMessage.contains("column2"))

    // multiple duplicate columns present
    val f = intercept[org.apache.spark.sql.AnalysisException] {
      Seq((1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7))
        .toDF("column1", "column2", "column3", "column1", "column3")
        .write.format("json").save("temp")
    }
    assert(f.getMessage.contains("Duplicate column(s)"))
    assert(f.getMessage.contains("JSON"))
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
      val pdf = sqlContext.read.parquet(tempParquetFile.getCanonicalPath)
      pdf.registerTempTable("parquet_base")
      insertion.write.insertInto("parquet_base")

      // pass case: json table (InsertableRelation)
      df.write.mode(SaveMode.Overwrite).json(tempJsonFile.getCanonicalPath)
      val jdf = sqlContext.read.json(tempJsonFile.getCanonicalPath)
      jdf.registerTempTable("json_base")
      insertion.write.mode(SaveMode.Overwrite).insertInto("json_base")

      // error cases: insert into an RDD
      df.registerTempTable("rdd_base")
      val e1 = intercept[AnalysisException] {
        insertion.write.insertInto("rdd_base")
      }
      assert(e1.getMessage.contains("Inserting into an RDD-based table is not allowed."))

      // error case: insert into a logical plan that is not a LeafNode
      val indirectDS = pdf.select("_1").filter($"_1" > 5)
      indirectDS.registerTempTable("indirect_ds")
      val e2 = intercept[AnalysisException] {
        insertion.write.insertInto("indirect_ds")
      }
      assert(e2.getMessage.contains("Inserting into an RDD-based table is not allowed."))

      // error case: insert into an OneRowRelation
      new DataFrame(sqlContext, OneRowRelation).registerTempTable("one_row")
      val e3 = intercept[AnalysisException] {
        insertion.write.insertInto("one_row")
      }
      assert(e3.getMessage.contains("Inserting into an RDD-based table is not allowed."))
    }
  }

  test("SPARK-8608: call `show` on local DataFrame with random columns should return same value") {
    // Make sure we can pass this test for both codegen mode and interpreted mode.
    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
      val df = testData.select(rand(33))
      assert(df.showString(5) == df.showString(5))
    }

    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "false") {
      val df = testData.select(rand(33))
      assert(df.showString(5) == df.showString(5))
    }

    // We will reuse the same Expression object for LocalRelation.
    val df = (1 to 10).map(Tuple1.apply).toDF().select(rand(33))
    assert(df.showString(5) == df.showString(5))
  }

  test("SPARK-8609: local DataFrame with random columns should return same value after sort") {
    // Make sure we can pass this test for both codegen mode and interpreted mode.
    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
      checkAnswer(testData.sort(rand(33)), testData.sort(rand(33)))
    }

    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "false") {
      checkAnswer(testData.sort(rand(33)), testData.sort(rand(33)))
    }

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

  test("SPARK-9323: DataFrame.orderBy should support nested column name") {
    val df = sqlContext.read.json(sqlContext.sparkContext.makeRDD(
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

    val union = df1.unionAll(df2)
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
}
