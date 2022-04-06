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

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

import scala.collection.mutable.{ArrayBuffer, WrappedArray}

import org.apache.spark.SparkException
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, OuterScopes}
import org.apache.spark.sql.catalyst.expressions.{Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{QueryExecution, SimpleMode}
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, SparkUserDefinedFunction, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.{lit, struct, udaf, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.sql.util.QueryExecutionListener

private case class FunctionResult(f1: String, f2: String)
private case class LocalDateInstantType(date: LocalDate, instant: Instant)
private case class TimestampInstantType(t: Timestamp, instant: Instant)

class UDFSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("built-in fixed arity expressions") {
    val df = spark.emptyDataFrame
    df.selectExpr("rand()", "randn()", "rand(5)", "randn(50)")
  }

  test("built-in vararg expressions") {
    val df = Seq((1, 2)).toDF("a", "b")
    df.selectExpr("array(a, b)")
    df.selectExpr("struct(a, b)")
  }

  test("built-in expressions with multiple constructors") {
    val df = Seq(("abcd", 2)).toDF("a", "b")
    df.selectExpr("substr(a, 2)", "substr(a, 2, 3)").collect()
  }

  test("count") {
    val df = Seq(("abcd", 2)).toDF("a", "b")
    df.selectExpr("count(a)")
  }

  test("count distinct") {
    val df = Seq(("abcd", 2)).toDF("a", "b")
    df.selectExpr("count(distinct a)")
  }

  test("SPARK-8003 spark_partition_id") {
    val df = Seq((1, "Tearing down the walls that divide us")).toDF("id", "saying")
    df.createOrReplaceTempView("tmp_table")
    checkAnswer(sql("select spark_partition_id() from tmp_table").toDF(), Row(0))
    spark.catalog.dropTempView("tmp_table")
  }

  test("SPARK-8005 input_file_name") {
    withTempPath { dir =>
      val data = sparkContext.parallelize(0 to 10, 2).toDF("id")
      data.write.parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("test_table")
      val answer = sql("select input_file_name() from test_table").head().getString(0)
      assert(answer.contains(dir.toURI.getPath))
      assert(sql("select input_file_name() from test_table").distinct().collect().length >= 2)
      spark.catalog.dropTempView("test_table")
    }
  }

  test("error reporting for incorrect number of arguments - builtin function") {
    val df = spark.emptyDataFrame
    val e = intercept[AnalysisException] {
      df.selectExpr("substr('abcd', 2, 3, 4)")
    }
    assert(e.getMessage.contains("Invalid number of arguments for function substr. Expected:"))
  }

  test("error reporting for incorrect number of arguments - udf") {
    val df = spark.emptyDataFrame
    val e = intercept[AnalysisException] {
      spark.udf.register("foo", (_: String).length)
      df.selectExpr("foo(2, 3, 4)")
    }
    assert(e.getMessage.contains("Invalid number of arguments for function foo. Expected:"))
  }

  test("error reporting for undefined functions") {
    val df = spark.emptyDataFrame
    val e = intercept[AnalysisException] {
      df.selectExpr("a_function_that_does_not_exist()")
    }
    assert(e.getMessage.contains("Undefined function"))
    assert(e.getMessage.contains("a_function_that_does_not_exist"))
  }

  test("Simple UDF") {
    spark.udf.register("strLenScala", (_: String).length)
    assert(sql("SELECT strLenScala('test')").head().getInt(0) === 4)
  }

  test("UDF defined using UserDefinedFunction") {
    import functions.udf
    val foo = udf((x: Int) => x + 1)
    spark.udf.register("foo", foo)
    assert(sql("select foo(5)").head().getInt(0) == 6)
  }

  test("ZeroArgument non-deterministic UDF") {
    val foo = udf(() => Math.random())
    spark.udf.register("random0", foo.asNondeterministic())
    val df = sql("SELECT random0()")
    assert(df.logicalPlan.asInstanceOf[Project].projectList.forall(!_.deterministic))
    assert(df.head().getDouble(0) >= 0.0)

    val foo1 = foo.asNondeterministic()
    val df1 = testData.select(foo1())
    assert(df1.logicalPlan.asInstanceOf[Project].projectList.forall(!_.deterministic))
    assert(df1.head().getDouble(0) >= 0.0)

    withSQLConf(SQLConf.LEGACY_ALLOW_UNTYPED_SCALA_UDF.key -> "true") {
      val bar = udf(() => Math.random(), DataTypes.DoubleType).asNondeterministic()
      val df2 = testData.select(bar())
      assert(df2.logicalPlan.asInstanceOf[Project].projectList.forall(!_.deterministic))
      assert(df2.head().getDouble(0) >= 0.0)
    }

    val javaUdf = udf(new UDF0[Double] {
      override def call(): Double = Math.random()
    }, DoubleType).asNondeterministic()
    val df3 = testData.select(javaUdf())
    assert(df3.logicalPlan.asInstanceOf[Project].projectList.forall(!_.deterministic))
    assert(df3.head().getDouble(0) >= 0.0)
  }

  test("TwoArgument UDF") {
    spark.udf.register("strLenScala", (_: String).length + (_: Int))
    assert(sql("SELECT strLenScala('test', 1)").head().getInt(0) === 5)
  }

  test("UDF in a WHERE") {
    withTempView("integerData") {
      spark.udf.register("oneArgFilter", (n: Int) => { n > 80 })

      val df = sparkContext.parallelize(
        (1 to 100).map(i => TestData(i, i.toString))).toDF()
      df.createOrReplaceTempView("integerData")

      val result =
        sql("SELECT * FROM integerData WHERE oneArgFilter(key)")
      assert(result.count() === 20)
    }
  }

  test("UDF in a HAVING") {
    withTempView("groupData") {
      spark.udf.register("havingFilter", (n: Long) => { n > 5 })

      val df = Seq(("red", 1), ("red", 2), ("blue", 10),
        ("green", 100), ("green", 200)).toDF("g", "v")
      df.createOrReplaceTempView("groupData")

      val result =
        sql(
          """
           | SELECT g, SUM(v) as s
           | FROM groupData
           | GROUP BY g
           | HAVING havingFilter(s)
          """.stripMargin)

      assert(result.count() === 2)
    }
  }

  test("UDF in a GROUP BY") {
    withTempView("groupData") {
      spark.udf.register("groupFunction", (n: Int) => { n > 10 })

      val df = Seq(("red", 1), ("red", 2), ("blue", 10),
        ("green", 100), ("green", 200)).toDF("g", "v")
      df.createOrReplaceTempView("groupData")

      val result =
        sql(
          """
           | SELECT SUM(v)
           | FROM groupData
           | GROUP BY groupFunction(v)
          """.stripMargin)
      assert(result.count() === 2)
    }
  }

  test("UDFs everywhere") {
    withTempView("groupData") {
      spark.udf.register("groupFunction", (n: Int) => { n > 10 })
      spark.udf.register("havingFilter", (n: Long) => { n > 2000 })
      spark.udf.register("whereFilter", (n: Int) => { n < 150 })
      spark.udf.register("timesHundred", (n: Long) => { n * 100 })

      val df = Seq(("red", 1), ("red", 2), ("blue", 10),
        ("green", 100), ("green", 200)).toDF("g", "v")
      df.createOrReplaceTempView("groupData")

      val result =
        sql(
          """
           | SELECT timesHundred(SUM(v)) as v100
           | FROM groupData
           | WHERE whereFilter(v)
           | GROUP BY groupFunction(v)
           | HAVING havingFilter(v100)
          """.stripMargin)
      assert(result.count() === 1)
    }
  }

  test("struct UDF") {
    spark.udf.register("returnStruct", (f1: String, f2: String) => FunctionResult(f1, f2))

    val result =
      sql("SELECT returnStruct('test', 'test2') as ret")
        .select($"ret.f1").head().getString(0)
    assert(result === "test")
  }

  test("udf that is transformed") {
    spark.udf.register("makeStruct", (x: Int, y: Int) => (x, y))
    // 1 + 1 is constant folded causing a transformation.
    assert(sql("SELECT makeStruct(1 + 1, 2)").first().getAs[Row](0) === Row(2, 2))
  }

  test("type coercion for udf inputs") {
    spark.udf.register("intExpected", (x: Int) => x)
    // pass a decimal to intExpected.
    assert(sql("SELECT intExpected(1.0)").head().getInt(0) === 1)
  }

  test("udf in different types") {
    spark.udf.register("testDataFunc", (n: Int, s: String) => { (n, s) })
    spark.udf.register("decimalDataFunc",
      (a: java.math.BigDecimal, b: java.math.BigDecimal) => { (a, b) })
    spark.udf.register("binaryDataFunc", (a: Array[Byte], b: Int) => { (a, b) })
    spark.udf.register("arrayDataFunc",
      (data: Seq[Int], nestedData: Seq[Seq[Int]]) => { (data, nestedData) })
    spark.udf.register("mapDataFunc",
      (data: scala.collection.Map[Int, String]) => { data })
    spark.udf.register("complexDataFunc",
      (m: Map[String, Int], a: Seq[Int], b: Boolean) => { (m, a, b) } )

    checkAnswer(
      sql("SELECT tmp.t.* FROM (SELECT testDataFunc(key, value) AS t from testData) tmp").toDF(),
      testData)
    checkAnswer(
      sql("""
           | SELECT tmp.t.* FROM
           | (SELECT decimalDataFunc(a, b) AS t FROM decimalData) tmp
          """.stripMargin).toDF(), decimalData)
    checkAnswer(
      sql("""
           | SELECT tmp.t.* FROM
           | (SELECT binaryDataFunc(a, b) AS t FROM binaryData) tmp
          """.stripMargin).toDF(), binaryData)
    checkAnswer(
      sql("""
           | SELECT tmp.t.* FROM
           | (SELECT arrayDataFunc(data, nestedData) AS t FROM arrayData) tmp
          """.stripMargin).toDF(), arrayData.toDF())
    checkAnswer(
      sql("""
           | SELECT mapDataFunc(data) AS t FROM mapData
          """.stripMargin).toDF(), mapData.toDF())
    checkAnswer(
      sql("""
           | SELECT tmp.t.* FROM
           | (SELECT complexDataFunc(m, a, b) AS t FROM complexData) tmp
          """.stripMargin).toDF(), complexData.select("m", "a", "b"))
  }

  test("SPARK-11716 UDFRegistration does not include the input data type in returned UDF") {
    val myUDF = spark.udf.register("testDataFunc", (n: Int, s: String) => { (n, s.toInt) })

    // Without the fix, this will fail because we fail to cast data type of b to string
    // because myUDF does not know its input data type. With the fix, this query should not
    // fail.
    checkAnswer(
      testData2.select(myUDF($"a", $"b").as("t")),
      testData2.selectExpr("struct(a, b)"))

    checkAnswer(
      sql("SELECT tmp.t.* FROM (SELECT testDataFunc(a, b) AS t from testData2) tmp").toDF(),
      testData2)
  }

  test("SPARK-19338 Provide identical names for UDFs in the EXPLAIN output") {
    def explainStr(df: DataFrame): String = {
      val explain = ExplainCommand(df.queryExecution.logical, SimpleMode)
      val sparkPlan = spark.sessionState.executePlan(explain).executedPlan
      sparkPlan.executeCollect().map(_.getString(0).trim).headOption.getOrElse("")
    }
    val udf1Name = "myUdf1"
    val udf2Name = "myUdf2"
    val udf1 = spark.udf.register(udf1Name, (n: Int) => n + 1)
    val udf2 = spark.udf.register(udf2Name, (n: Int) => n * 1)
    assert(explainStr(sql("SELECT myUdf1(myUdf2(1))")).contains(s"$udf1Name($udf2Name(1))"))
    assert(explainStr(spark.range(1).select(udf1(udf2(functions.lit(1)))))
      .contains(s"$udf1Name($udf2Name(1))"))
  }

  test("SPARK-23666 Do not display exprId in argument names") {
    withTempView("x") {
      Seq(((1, 2), 3)).toDF("a", "b").createOrReplaceTempView("x")
      spark.udf.register("f", (a: Int) => a)
      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        spark.sql("SELECT f(a._1) FROM x").show
      }
      assert(outputStream.toString.contains("f(a._1)"))
    }
  }

  test("cached Data should be used in the write path") {
    withTable("t") {
      withTempPath { path =>
        var numTotalCachedHit = 0
        val listener = new QueryExecutionListener {
          override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}

          override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
            qe.withCachedData match {
              case c: CreateDataSourceTableAsSelectCommand
                  if c.query.isInstanceOf[InMemoryRelation] =>
                numTotalCachedHit += 1
              case i: InsertIntoHadoopFsRelationCommand
                  if i.query.isInstanceOf[InMemoryRelation] =>
                numTotalCachedHit += 1
              case _ =>
            }
          }
        }
        spark.listenerManager.register(listener)

        val udf1 = udf({ (x: Int, y: Int) => x + y })
        val df = spark.range(0, 3).toDF("a")
          .withColumn("b", udf1($"a", lit(10)))
        df.cache()
        df.write.saveAsTable("t")
        sparkContext.listenerBus.waitUntilEmpty()
        assert(numTotalCachedHit == 1, "expected to be cached in saveAsTable")
        df.write.insertInto("t")
        sparkContext.listenerBus.waitUntilEmpty()
        assert(numTotalCachedHit == 2, "expected to be cached in insertInto")
        df.write.save(path.getCanonicalPath)
        sparkContext.listenerBus.waitUntilEmpty()
        assert(numTotalCachedHit == 3, "expected to be cached in save for native")
      }
    }
  }

  test("SPARK-24891 Fix HandleNullInputsForUDF rule") {
    val udf1 = udf({(x: Int, y: Int) => x + y})
    val df = spark.range(0, 3).toDF("a")
      .withColumn("b", udf1($"a", udf1($"a", lit(10))))
      .withColumn("c", udf1($"a", lit(null)))
    val plan = spark.sessionState.executePlan(df.logicalPlan).analyzed

    comparePlans(df.logicalPlan, plan)
    checkAnswer(
      df,
      Seq(
        Row(0, 10, null),
        Row(1, 12, null),
        Row(2, 14, null)))
  }

  test("SPARK-24891 Fix HandleNullInputsForUDF rule - with table") {
    withTable("x") {
      Seq((1, "2"), (2, "4")).toDF("a", "b").write.format("json").saveAsTable("x")
      sql("insert into table x values(3, null)")
      sql("insert into table x values(null, '4')")
      spark.udf.register("f", (a: Int, b: String) => a + b)
      val df = spark.sql("SELECT f(a, b) FROM x")
      val plan = spark.sessionState.executePlan(df.logicalPlan).analyzed
      comparePlans(df.logicalPlan, plan)
      checkAnswer(df, Seq(Row("12"), Row("24"), Row("3null"), Row(null)))
    }
  }

  test("SPARK-25044 Verify null input handling for primitive types - with udf()") {
    val input = Seq(
      (null, Integer.valueOf(1), "x"),
      ("M", null, "y"),
      ("N", Integer.valueOf(3), null)).toDF("a", "b", "c")

    val udf1 = udf((a: String, b: Int, c: Any) => a + b + c)
    val df = input.select(udf1($"a", $"b", $"c"))
    checkAnswer(df, Seq(Row("null1x"), Row(null), Row("N3null")))

    // test Java UDF. Java UDF can't have primitive inputs, as it's generic typed.
    val udf2 = udf(new UDF3[String, Integer, Object, String] {
      override def call(t1: String, t2: Integer, t3: Object): String = {
        t1 + t2 + t3
      }
    }, StringType)
    val df2 = input.select(udf2($"a", $"b", $"c"))
    checkAnswer(df2, Seq(Row("null1x"), Row("Mnully"), Row("N3null")))
  }

  test("SPARK-25044 Verify null input handling for primitive types - with udf.register") {
    withTable("t") {
      Seq((null, Integer.valueOf(1), "x"), ("M", null, "y"), ("N", Integer.valueOf(3), null))
        .toDF("a", "b", "c").write.format("json").saveAsTable("t")
      spark.udf.register("f", (a: String, b: Int, c: Any) => a + b + c)
      val df = spark.sql("SELECT f(a, b, c) FROM t")
      checkAnswer(df, Seq(Row("null1x"), Row(null), Row("N3null")))

      // test Java UDF. Java UDF can't have primitive inputs, as it's generic typed.
      spark.udf.register("f2", new UDF3[String, Integer, Object, String] {
        override def call(t1: String, t2: Integer, t3: Object): String = {
          t1 + t2 + t3
        }
      }, StringType)
      val df2 = spark.sql("SELECT f2(a, b, c) FROM t")
      checkAnswer(df2, Seq(Row("null1x"), Row("Mnully"), Row("N3null")))
    }
  }

  test("SPARK-25044 Verify null input handling for primitive types - with udf(Any, DataType)") {
    withSQLConf(SQLConf.LEGACY_ALLOW_UNTYPED_SCALA_UDF.key -> "true") {
      val f = udf((x: Int) => x, IntegerType)
      checkAnswer(
        Seq(Integer.valueOf(1), null).toDF("x").select(f($"x")),
        Row(1) :: Row(0) :: Nil)

      val f2 = udf((x: Double) => x, DoubleType)
      checkAnswer(
        Seq(java.lang.Double.valueOf(1.1), null).toDF("x").select(f2($"x")),
        Row(1.1) :: Row(0.0) :: Nil)
    }

  }

  test("use untyped Scala UDF should fail by default") {
    val e = intercept[AnalysisException](udf((x: Int) => x, IntegerType))
    assert(e.getMessage.contains("You're using untyped Scala UDF"))
  }

  test("SPARK-26308: udf with decimal") {
    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(new BigDecimal("2011000000000002456556")))),
      StructType(Seq(StructField("col1", DecimalType(30, 0)))))
    val udf1 = org.apache.spark.sql.functions.udf((value: BigDecimal) => {
      if (value == null) null else value.toBigInteger.toString
    })
    checkAnswer(df1.select(udf1(df1.col("col1"))), Seq(Row("2011000000000002456556")))
  }

  test("SPARK-26308: udf with complex types of decimal") {
    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(Array(new BigDecimal("2011000000000002456556"))))),
      StructType(Seq(StructField("col1", ArrayType(DecimalType(30, 0))))))
    val udf1 = org.apache.spark.sql.functions.udf((arr: Seq[BigDecimal]) => {
      arr.map(value => if (value == null) null else value.toBigInteger.toString)
    })
    checkAnswer(df1.select(udf1($"col1")), Seq(Row(Array("2011000000000002456556"))))

    val df2 = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(Map("a" -> new BigDecimal("2011000000000002456556"))))),
      StructType(Seq(StructField("col1", MapType(StringType, DecimalType(30, 0))))))
    val udf2 = org.apache.spark.sql.functions.udf((map: Map[String, BigDecimal]) => {
      map.mapValues(value => if (value == null) null else value.toBigInteger.toString).toMap
    })
    checkAnswer(df2.select(udf2($"col1")), Seq(Row(Map("a" -> "2011000000000002456556"))))
  }

  test("SPARK-26323 Verify input type check - with udf()") {
    val f = udf((x: Long, y: Any) => x)
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j").select(f($"i", $"j"))
    checkAnswer(df, Seq(Row(1L), Row(2L)))
  }

  test("SPARK-26323 Verify input type check - with udf.register") {
    withTable("t") {
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.format("json").saveAsTable("t")
      spark.udf.register("f", (x: Long, y: Any) => x)
      val df = spark.sql("SELECT f(i, j) FROM t")
      checkAnswer(df, Seq(Row(1L), Row(2L)))
    }
  }

  test("Using java.time.Instant in UDF") {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val expected = java.time.Instant.parse("2019-02-27T00:00:00Z")
      .atZone(DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
      .toLocalDateTime
      .format(dtf)
    val plusSec = udf((i: java.time.Instant) => i.plusSeconds(1))
    val df = spark.sql("SELECT TIMESTAMP '2019-02-26 23:59:59Z' as t")
      .select(plusSec($"t").cast(StringType))
    checkAnswer(df, Row(expected) :: Nil)
  }

  test("Using java.time.LocalDate in UDF") {
    val expected = java.time.LocalDate.parse("2019-02-27").toString
    val plusDay = udf((i: java.time.LocalDate) => i.plusDays(1))
    val df = spark.sql("SELECT DATE '2019-02-26' as d")
      .select(plusDay($"d").cast(StringType))
    checkAnswer(df, Row(expected) :: Nil)
  }

  test("Using combined types of Instant/LocalDate in UDF") {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val date = LocalDate.parse("2019-02-26")
    val instant = Instant.parse("2019-02-26T23:59:59Z")
    val expectedDate = date.toString
    val expectedInstant =
      instant.atZone(DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
        .toLocalDateTime
        .format(dtf)
    val df = Seq((date, instant)).toDF("d", "i")

    // test normal case
    spark.udf.register("buildLocalDateInstantType",
      udf((d: LocalDate, i: Instant) => LocalDateInstantType(d, i)))
    checkAnswer(df.selectExpr(s"buildLocalDateInstantType(d, i) as di")
      .select($"di".cast(StringType)),
      Row(s"{$expectedDate, $expectedInstant}") :: Nil)

    // test null cases
    spark.udf.register("buildLocalDateInstantType",
      udf((d: LocalDate, i: Instant) => LocalDateInstantType(null, null)))
    checkAnswer(df.selectExpr("buildLocalDateInstantType(d, i) as di"),
      Row(Row(null, null)))

    spark.udf.register("buildLocalDateInstantType",
      udf((d: LocalDate, i: Instant) => null.asInstanceOf[LocalDateInstantType]))
    checkAnswer(df.selectExpr("buildLocalDateInstantType(d, i) as di"),
      Row(null))
  }

  test("Using combined types of Instant/Timestamp in UDF") {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val timestamp = Timestamp.valueOf("2019-02-26 23:59:59")
    val instant = Instant.parse("2019-02-26T23:59:59Z")
    val expectedTimestamp = timestamp.toLocalDateTime.format(dtf)
    val expectedInstant =
      instant.atZone(DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
      .toLocalDateTime
      .format(dtf)
    val df = Seq((timestamp, instant)).toDF("t", "i")

    // test normal case
    spark.udf.register("buildTimestampInstantType",
      udf((t: Timestamp, i: Instant) => TimestampInstantType(t, i)))
    checkAnswer(df.selectExpr("buildTimestampInstantType(t, i) as ti")
      .select($"ti".cast(StringType)),
      Row(s"{$expectedTimestamp, $expectedInstant}"))

    // test null cases
    spark.udf.register("buildTimestampInstantType",
      udf((t: Timestamp, i: Instant) => TimestampInstantType(null, null)))
    checkAnswer(df.selectExpr("buildTimestampInstantType(t, i) as ti"),
      Row(Row(null, null)))

    spark.udf.register("buildTimestampInstantType",
      udf((t: Timestamp, i: Instant) => null.asInstanceOf[TimestampInstantType]))
    checkAnswer(df.selectExpr("buildTimestampInstantType(t, i) as ti"),
      Row(null))
  }

  test("SPARK-32154: return null with or without explicit type") {
    // without explicit type
    val udf1 = udf((i: String) => null)
    assert(udf1.asInstanceOf[SparkUserDefinedFunction] .dataType === NullType)
    checkAnswer(Seq("1").toDF("a").select(udf1($"a")), Row(null) :: Nil)
    // with explicit type
    val udf2 = udf((i: String) => null.asInstanceOf[String])
    assert(udf2.asInstanceOf[SparkUserDefinedFunction].dataType === StringType)
    checkAnswer(Seq("1").toDF("a").select(udf1($"a")), Row(null) :: Nil)
  }

  test("SPARK-28321 0-args Java UDF should not be called only once") {
    val nonDeterministicJavaUDF = udf(
      new UDF0[Int] {
        override def call(): Int = scala.util.Random.nextInt()
      }, IntegerType).asNondeterministic()

    assert(spark.range(2).select(nonDeterministicJavaUDF()).distinct().count() == 2)
  }

  test("SPARK-28521 error message for CAST(parameter types contains DataType)") {
    val e = intercept[AnalysisException] {
      spark.sql("SELECT CAST(1)")
    }
    assert(e.getMessage.contains("Invalid arguments for function cast"))
  }

  test("only one case class parameter") {
    val f = (d: TestData) => d.key * d.value.toInt
    val myUdf = udf(f)
    val df = Seq(("data", TestData(50, "2"))).toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col2"))), Row(100) :: Nil)
  }

  test("one case class with primitive parameter") {
    val f = (i: Int, p: TestData) => p.key * i
    val myUdf = udf(f)
    val df = Seq((2, TestData(50, "data"))).toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col1"), Column("col2"))), Row(100) :: Nil)
  }

  test("multiple case class parameters") {
    val f = (d1: TestData, d2: TestData) => d1.key * d2.key
    val myUdf = udf(f)
    val df = Seq((TestData(10, "d1"), TestData(50, "d2"))).toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col1"), Column("col2"))), Row(500) :: Nil)
  }

  test("input case class parameter and return case class") {
    val f = (d: TestData) => TestData(d.key * 2, "copy")
    val myUdf = udf(f)
    val df = Seq(("data", TestData(50, "d2"))).toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col2"))), Row(Row(100, "copy")) :: Nil)
  }

  test("any and case class parameter") {
    val f = (any: Any, d: TestData) => s"${any.toString}, ${d.value}"
    val myUdf = udf(f)
    val df = Seq(("Hello", TestData(50, "World"))).toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col1"), Column("col2"))), Row("Hello, World") :: Nil)
  }

  test("nested case class parameter") {
    val f = (y: Int, training: TrainingSales) => training.sales.year + y
    val myUdf = udf(f)
    val df = Seq((20, TrainingSales("training", CourseSales("course", 2000, 3.14))))
      .toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col1"), Column("col2"))), Row(2020) :: Nil)
  }

  test("case class as element type of Seq/Array") {
    val f1 = (s: Seq[TestData]) => s.map(d => d.key * d.value.toInt).sum
    val myUdf1 = udf(f1)
    val df1 = Seq(("data", Seq(TestData(50, "2")))).toDF("col1", "col2")
    checkAnswer(df1.select(myUdf1(Column("col2"))), Row(100) :: Nil)

    val f2 = (s: Array[TestData]) => s.map(d => d.key * d.value.toInt).sum
    val myUdf2 = udf(f2)
    val df2 = Seq(("data", Array(TestData(50, "2")))).toDF("col1", "col2")
    checkAnswer(df2.select(myUdf2(Column("col2"))), Row(100) :: Nil)
  }

  test("case class as key/value type of Map") {
    val f1 = (s: Map[TestData, Int]) => s.keys.head.key * s.keys.head.value.toInt
    val myUdf1 = udf(f1)
    val df1 = Seq(("data", Map(TestData(50, "2") -> 502))).toDF("col1", "col2")
    checkAnswer(df1.select(myUdf1(Column("col2"))), Row(100) :: Nil)

    val f2 = (s: Map[Int, TestData]) => s.values.head.key * s.values.head.value.toInt
    val myUdf2 = udf(f2)
    val df2 = Seq(("data", Map(502 -> TestData(50, "2")))).toDF("col1", "col2")
    checkAnswer(df2.select(myUdf2(Column("col2"))), Row(100) :: Nil)

    val f3 = (s: Map[TestData, TestData]) => s.keys.head.key * s.values.head.value.toInt
    val myUdf3 = udf(f3)
    val df3 = Seq(("data", Map(TestData(50, "2") -> TestData(50, "2")))).toDF("col1", "col2")
    checkAnswer(df3.select(myUdf3(Column("col2"))), Row(100) :: Nil)
  }

  test("case class as element of tuple") {
    val f = (s: (TestData, Int)) => s._1.key * s._2
    val myUdf = udf(f)
    val df = Seq(("data", (TestData(50, "2"), 2))).toDF("col1", "col2")
    checkAnswer(df.select(myUdf(Column("col2"))), Row(100) :: Nil)
  }

  test("case class as generic type of Option") {
    val f = (o: Option[TestData]) => o.map(t => t.key * t.value.toInt)
    val myUdf = udf(f)
    val df1 = Seq(("data", Some(TestData(50, "2")))).toDF("col1", "col2")
    checkAnswer(df1.select(myUdf(Column("col2"))), Row(100) :: Nil)
    val df2 = Seq(("data", None: Option[TestData])).toDF("col1", "col2")
    checkAnswer(df2.select(myUdf(Column("col2"))), Row(null) :: Nil)
  }

  test("more input fields than expect for case class") {
    val f = (t: TestData2) => t.a * t.b
    val myUdf = udf(f)
    val df = spark.range(1)
      .select(lit(50).as("a"), lit(2).as("b"), lit(2).as("c"))
      .select(struct("a", "b", "c").as("col"))
    checkAnswer(df.select(myUdf(Column("col"))), Row(100) :: Nil)
  }

  test("less input fields than expect for case class") {
    val f = (t: TestData2) => t.a * t.b
    val myUdf = udf(f)
    val df = spark.range(1)
      .select(lit(50).as("a"))
      .select(struct("a").as("col"))
    val error = intercept[AnalysisException](df.select(myUdf(Column("col"))))
    assert(error.getErrorClass == "MISSING_COLUMN")
    assert(error.messageParameters.sameElements(Array("b", "a")))
  }

  test("wrong order of input fields for case class") {
    val f = (t: TestData) => t.key * t.value.toInt
    val myUdf = udf(f)
    val df = spark.range(1)
      .select(lit("2").as("value"), lit(50).as("key"))
      .select(struct("value", "key").as("col"))
    checkAnswer(df.select(myUdf(Column("col"))), Row(100) :: Nil)
  }

  test("top level Option primitive type") {
    val f = (i: Option[Int]) => i.map(_ * 10)
    val myUdf = udf(f)
    val df = Seq(Some(10), None).toDF("col")
    checkAnswer(df.select(myUdf(Column("col"))), Row(100) :: Row(null) :: Nil)
  }

  test("array Option") {
    val f = (i: Array[Option[TestData]]) =>
      i.map(_.map(t => t.key * t.value.toInt).getOrElse(0)).sum
    val myUdf = udf(f)
    val df = Seq(Array(Some(TestData(50, "2")), None)).toDF("col")
    checkAnswer(df.select(myUdf(Column("col"))), Row(100) :: Nil)
  }

  object MalformedClassObject extends Serializable {
    class MalformedNonPrimitiveFunction extends (String => Int) with Serializable {
      override def apply(v1: String): Int = v1.toInt / 0
    }

    class MalformedPrimitiveFunction extends (Int => Int) with Serializable {
      override def apply(v1: Int): Int = v1 / 0
    }
  }

  test("SPARK-32238: Use Utils.getSimpleName to avoid hitting Malformed class name") {
    OuterScopes.addOuterScope(MalformedClassObject)
    val f1 = new MalformedClassObject.MalformedNonPrimitiveFunction()
    val f2 = new MalformedClassObject.MalformedPrimitiveFunction()

    val e1 = intercept[SparkException] {
      Seq("20").toDF("col").select(udf(f1).apply(Column("col"))).collect()
    }
    assert(e1.getMessage.contains("UDFSuite$MalformedClassObject$MalformedNonPrimitiveFunction"))

    val e2 = intercept[SparkException] {
      Seq(20).toDF("col").select(udf(f2).apply(Column("col"))).collect()
    }
    assert(e2.getMessage.contains("UDFSuite$MalformedClassObject$MalformedPrimitiveFunction"))
  }

  test("SPARK-32307: Aggregation that use map type input UDF as group expression") {
    spark.udf.register("key", udf((m: Map[String, String]) => m.keys.head.toInt))
    Seq(Map("1" -> "one", "2" -> "two")).toDF("a").createOrReplaceTempView("t")
    checkAnswer(sql("SELECT key(a) AS k FROM t GROUP BY key(a)"), Row(1) :: Nil)
  }

  test("SPARK-32307: Aggregation that use array type input UDF as group expression") {
    spark.udf.register("key", udf((m: Array[Int]) => m.head))
    Seq(Array(1)).toDF("a").createOrReplaceTempView("t")
    checkAnswer(sql("SELECT key(a) AS k FROM t GROUP BY key(a)"), Row(1) :: Nil)
  }

  test("SPARK-32459: UDF should not fail on WrappedArray") {
    val myUdf = udf((a: WrappedArray[Int]) =>
      WrappedArray.make[Int](Array(a.head + 99)))
    checkAnswer(Seq(Array(1))
      .toDF("col")
      .select(myUdf(Column("col"))),
      Row(ArrayBuffer(100)))
  }

  test("SPARK-34388: UDF name is propagated with registration for ScalaUDF") {
    spark.udf.register("udf34388", udf((value: Int) => value > 2))
    spark.sessionState.catalog.lookupFunction(
      FunctionIdentifier("udf34388"), Seq(Literal(1))) match {
      case udf: ScalaUDF => assert(udf.name === "udf34388")
    }
  }

  test("SPARK-34388: UDF name is propagated with registration for ScalaAggregator") {
    val agg = new Aggregator[Long, Long, Long] {
      override def zero: Long = 0L
      override def reduce(b: Long, a: Long): Long = a + b
      override def merge(b1: Long, b2: Long): Long = b1 + b2
      override def finish(reduction: Long): Long = reduction
      override def bufferEncoder: Encoder[Long] = ExpressionEncoder[Long]()
      override def outputEncoder: Encoder[Long] = ExpressionEncoder[Long]()
    }

    spark.udf.register("agg34388", udaf(agg))
    spark.sessionState.catalog.lookupFunction(
      FunctionIdentifier("agg34388"), Seq(Literal(1))) match {
      case agg: ScalaAggregator[_, _, _] => assert(agg.name === "agg34388")
    }
  }

  test("SPARK-34388: UDF name is propagated with registration for ScalaUDAF") {
    val udaf = new UserDefinedAggregateFunction {
      def inputSchema: StructType = new StructType().add("a", LongType)
      def bufferSchema: StructType = new StructType().add("product", LongType)
      def dataType: DataType = LongType
      def deterministic: Boolean = true
      def initialize(buffer: MutableAggregationBuffer): Unit = {}
      def update(buffer: MutableAggregationBuffer, input: Row): Unit = {}
      def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {}
      def evaluate(buffer: Row): Any = buffer.getLong(0)
    }
    spark.udf.register("udaf34388", udaf)
    spark.sessionState.catalog.lookupFunction(
      FunctionIdentifier("udaf34388"), Seq(Literal(1))) match {
      case udaf: ScalaUDAF => assert(udaf.name === "udaf34388")
    }
  }

  test("SPARK-35674: using java.time.LocalDateTime in UDF") {
    // Regular case
    val input = Seq(java.time.LocalDateTime.parse("2021-01-01T00:00:00")).toDF("dateTime")
    val plusYear = udf((l: java.time.LocalDateTime) => l.plusYears(1))
    val result = input.select(plusYear($"dateTime").as("newDateTime"))
    checkAnswer(result, Row(java.time.LocalDateTime.parse("2022-01-01T00:00:00")) :: Nil)
    assert(result.schema === new StructType().add("newDateTime", TimestampNTZType))
    // UDF produces `null`
    val nullFunc = udf((_: java.time.LocalDateTime) => null.asInstanceOf[java.time.LocalDateTime])
    val nullResult = input.select(nullFunc($"dateTime").as("nullDateTime"))
    checkAnswer(nullResult, Row(null) :: Nil)
    assert(nullResult.schema === new StructType().add("nullDateTime", TimestampNTZType))
    // Input parameter of UDF is null
    val nullInput = Seq(null.asInstanceOf[java.time.LocalDateTime]).toDF("nullDateTime")
    val constDuration = udf((_: java.time.LocalDateTime) =>
      java.time.LocalDateTime.parse("2021-01-01T00:00:00"))
    val constResult = nullInput.select(constDuration($"nullDateTime").as("firstDayOf2021"))
    checkAnswer(constResult, Row(java.time.LocalDateTime.parse("2021-01-01T00:00:00")) :: Nil)
    assert(constResult.schema === new StructType().add("firstDayOf2021", TimestampNTZType))
    // Error in the conversion of UDF result to the internal representation of timestamp without
    // time zone
    val overflowFunc = udf((l: java.time.LocalDateTime) => l.plusDays(Long.MaxValue))
    val e = intercept[SparkException] {
      input.select(overflowFunc($"dateTime")).collect()
    }.getCause.getCause
    assert(e.isInstanceOf[java.lang.ArithmeticException])
  }

  test("SPARK-34663, SPARK-35730: using java.time.Duration in UDF") {
    // Regular case
    val input = Seq(java.time.Duration.ofHours(23)).toDF("d")
      .select($"d",
      $"d".cast(DayTimeIntervalType(DAY)).as("d_dd"),
        $"d".cast(DayTimeIntervalType(DAY, HOUR)).as("d_dh"),
        $"d".cast(DayTimeIntervalType(DAY, MINUTE)).as("d_dm"),
        $"d".cast(DayTimeIntervalType(HOUR)).as("d_hh"),
        $"d".cast(DayTimeIntervalType(HOUR, MINUTE)).as("d_hm"),
        $"d".cast(DayTimeIntervalType(HOUR, SECOND)).as("d_hs"),
        $"d".cast(DayTimeIntervalType(MINUTE)).as("d_mm"),
        $"d".cast(DayTimeIntervalType(MINUTE, SECOND)).as("d_ms"),
        $"d".cast(DayTimeIntervalType(SECOND)).as("d_ss"))
    val plusHour = udf((d: java.time.Duration) => d.plusHours(1))
    val result = input.select(
      plusHour($"d").as("new_d"),
      plusHour($"d_dd").as("new_d_dd"),
      plusHour($"d_dh").as("new_d_dh"),
      plusHour($"d_dm").as("new_d_dm"),
      plusHour($"d_hh").as("new_d_hh"),
      plusHour($"d_hm").as("new_d_hm"),
      plusHour($"d_hs").as("new_d_hs"),
      plusHour($"d_mm").as("new_d_mm"),
      plusHour($"d_ms").as("new_d_ms"),
      plusHour($"d_ss").as("new_d_ss"))
    checkAnswer(result, Row(java.time.Duration.ofDays(1), java.time.Duration.ofHours(1),
      java.time.Duration.ofDays(1), java.time.Duration.ofDays(1),
      java.time.Duration.ofDays(1), java.time.Duration.ofDays(1),
      java.time.Duration.ofDays(1), java.time.Duration.ofDays(1),
      java.time.Duration.ofDays(1), java.time.Duration.ofDays(1)) :: Nil)
    assert(result.schema === new StructType()
      .add("new_d", DayTimeIntervalType())
      .add("new_d_dd", DayTimeIntervalType())
      .add("new_d_dh", DayTimeIntervalType())
      .add("new_d_dm", DayTimeIntervalType())
      .add("new_d_hh", DayTimeIntervalType())
      .add("new_d_hm", DayTimeIntervalType())
      .add("new_d_hs", DayTimeIntervalType())
      .add("new_d_mm", DayTimeIntervalType())
      .add("new_d_ms", DayTimeIntervalType())
      .add("new_d_ss", DayTimeIntervalType()))
    // UDF produces `null`
    val nullFunc = udf((_: java.time.Duration) => null.asInstanceOf[java.time.Duration])
    val nullResult = input.select(nullFunc($"d").as("null_d"),
      nullFunc($"d_dd").as("null_d_dd"),
      nullFunc($"d_dh").as("null_d_dh"),
      nullFunc($"d_dm").as("null_d_dm"),
      nullFunc($"d_hh").as("null_d_hh"),
      nullFunc($"d_hm").as("null_d_hm"),
      nullFunc($"d_hs").as("null_d_hs"),
      nullFunc($"d_mm").as("null_d_mm"),
      nullFunc($"d_ms").as("null_d_ms"),
      nullFunc($"d_ss").as("null_d_ss"))
    checkAnswer(nullResult, Row(null, null, null, null, null, null, null, null, null, null) :: Nil)
    assert(nullResult.schema === new StructType()
      .add("null_d", DayTimeIntervalType())
      .add("null_d_dd", DayTimeIntervalType())
      .add("null_d_dh", DayTimeIntervalType())
      .add("null_d_dm", DayTimeIntervalType())
      .add("null_d_hh", DayTimeIntervalType())
      .add("null_d_hm", DayTimeIntervalType())
      .add("null_d_hs", DayTimeIntervalType())
      .add("null_d_mm", DayTimeIntervalType())
      .add("null_d_ms", DayTimeIntervalType())
      .add("null_d_ss", DayTimeIntervalType()))
    // Input parameter of UDF is null
    val nullInput = Seq(null.asInstanceOf[java.time.Duration]).toDF("null_d")
      .select($"null_d",
        $"null_d".cast(DayTimeIntervalType(DAY)).as("null_d_dd"),
        $"null_d".cast(DayTimeIntervalType(DAY, HOUR)).as("null_d_dh"),
        $"null_d".cast(DayTimeIntervalType(DAY, MINUTE)).as("null_d_dm"),
        $"null_d".cast(DayTimeIntervalType(HOUR)).as("null_d_hh"),
        $"null_d".cast(DayTimeIntervalType(HOUR, MINUTE)).as("null_d_hm"),
        $"null_d".cast(DayTimeIntervalType(HOUR, SECOND)).as("null_d_hs"),
        $"null_d".cast(DayTimeIntervalType(MINUTE)).as("null_d_mm"),
        $"null_d".cast(DayTimeIntervalType(MINUTE, SECOND)).as("null_d_ms"),
        $"null_d".cast(DayTimeIntervalType(SECOND)).as("null_d_ss"))
    val constDuration = udf((_: java.time.Duration) => java.time.Duration.ofMinutes(10))
    val constResult = nullInput.select(
      constDuration($"null_d").as("10_min"),
      constDuration($"null_d_dd").as("10_min_dd"),
      constDuration($"null_d_dh").as("10_min_dh"),
      constDuration($"null_d_dm").as("10_min_dm"),
      constDuration($"null_d_hh").as("10_min_hh"),
      constDuration($"null_d_hm").as("10_min_hm"),
      constDuration($"null_d_hs").as("10_min_hs"),
      constDuration($"null_d_mm").as("10_min_mm"),
      constDuration($"null_d_ms").as("10_min_ms"),
      constDuration($"null_d_ss").as("10_min_ss"))
    checkAnswer(constResult, Row(
      java.time.Duration.ofMinutes(10), java.time.Duration.ofMinutes(10),
      java.time.Duration.ofMinutes(10), java.time.Duration.ofMinutes(10),
      java.time.Duration.ofMinutes(10), java.time.Duration.ofMinutes(10),
      java.time.Duration.ofMinutes(10), java.time.Duration.ofMinutes(10),
      java.time.Duration.ofMinutes(10), java.time.Duration.ofMinutes(10)) :: Nil)
    assert(constResult.schema === new StructType()
      .add("10_min", DayTimeIntervalType())
      .add("10_min_dd", DayTimeIntervalType())
      .add("10_min_dh", DayTimeIntervalType())
      .add("10_min_dm", DayTimeIntervalType())
      .add("10_min_hh", DayTimeIntervalType())
      .add("10_min_hm", DayTimeIntervalType())
      .add("10_min_hs", DayTimeIntervalType())
      .add("10_min_mm", DayTimeIntervalType())
      .add("10_min_ms", DayTimeIntervalType())
      .add("10_min_ss", DayTimeIntervalType()))
    // Error in the conversion of UDF result to the internal representation of day-time interval
    val overflowFunc = udf((d: java.time.Duration) => d.plusDays(Long.MaxValue))
    val e = intercept[SparkException] {
      input.select(overflowFunc($"d")).collect()
    }.getCause.getCause
    assert(e.isInstanceOf[java.lang.ArithmeticException])
  }

  test("SPARK-34663, SPARK-35777: using java.time.Period in UDF") {
    // Regular case
    val input = Seq(java.time.Period.ofMonths(13)).toDF("p")
      .select($"p", $"p".cast(YearMonthIntervalType(YEAR)).as("p_y"),
        $"p".cast(YearMonthIntervalType(MONTH)).as("p_m"))
    val incMonth = udf((p: java.time.Period) => p.plusMonths(1))
    val result = input.select(incMonth($"p").as("new_p"),
      incMonth($"p_y").as("new_p_y"),
      incMonth($"p_m").as("new_p_m"))
    checkAnswer(result, Row(java.time.Period.ofMonths(14).normalized(),
      java.time.Period.ofMonths(13).normalized(),
      java.time.Period.ofMonths(14).normalized()) :: Nil)
    assert(result.schema === new StructType()
      .add("new_p", YearMonthIntervalType())
      .add("new_p_y", YearMonthIntervalType())
      .add("new_p_m", YearMonthIntervalType()))
    // UDF produces `null`
    val nullFunc = udf((_: java.time.Period) => null.asInstanceOf[java.time.Period])
    val nullResult = input.select(nullFunc($"p").as("null_p"),
      nullFunc($"p_y").as("null_p_y"), nullFunc($"p_m").as("null_p_m"))
    checkAnswer(nullResult, Row(null, null, null) :: Nil)
    assert(nullResult.schema === new StructType()
      .add("null_p", YearMonthIntervalType())
      .add("null_p_y", YearMonthIntervalType())
      .add("null_p_m", YearMonthIntervalType()))
    // Input parameter of UDF is null
    val nullInput = Seq(null.asInstanceOf[java.time.Period]).toDF("null_p")
      .select($"null_p",
        $"null_p".cast(YearMonthIntervalType(YEAR)).as("null_p_y"),
        $"null_p".cast(YearMonthIntervalType(MONTH)).as("null_p_m"))
    val constPeriod = udf((_: java.time.Period) => java.time.Period.ofYears(10))
    val constResult = nullInput.select(constPeriod($"null_p").as("10_years"),
      constPeriod($"null_p_y").as("p_y_10_years"), constPeriod($"null_p_m").as("pm_10_years"))
    checkAnswer(constResult, Row(java.time.Period.ofYears(10),
      java.time.Period.ofYears(10), java.time.Period.ofYears(10)) :: Nil)
    assert(constResult.schema === new StructType()
      .add("10_years", YearMonthIntervalType())
      .add("p_y_10_years", YearMonthIntervalType())
      .add("pm_10_years", YearMonthIntervalType()))
    // Error in the conversion of UDF result to the internal representation of year-month interval
    val overflowFunc = udf((p: java.time.Period) => p.plusYears(Long.MaxValue))
    val e = intercept[SparkException] {
      input.select(overflowFunc($"p")).collect()
    }.getCause.getCause
    assert(e.isInstanceOf[java.lang.ArithmeticException])
  }
}
