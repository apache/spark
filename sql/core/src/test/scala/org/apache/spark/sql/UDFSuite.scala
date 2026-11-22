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
import java.time.{Instant, LocalDate, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SPARK_DOC_ROOT, SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, OuterScopes}
import org.apache.spark.sql.catalyst.expressions.{Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, Project}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SimpleMode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, SparkUserDefinedFunction, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
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

private case class KryoEncodedBuf(value: Long)
private case class KryoBufAggregator() extends Aggregator[Long, KryoEncodedBuf, Long] {
  override def zero: KryoEncodedBuf = KryoEncodedBuf(0)
  override def reduce(b: KryoEncodedBuf, a: Long): KryoEncodedBuf = KryoEncodedBuf(b.value + a)
  override def merge(b1: KryoEncodedBuf, b2: KryoEncodedBuf): KryoEncodedBuf =
    KryoEncodedBuf(b1.value + b2.value)
  override def finish(reduction: KryoEncodedBuf): Long = reduction.value
  override def bufferEncoder: Encoder[KryoEncodedBuf] = Encoders.kryo[KryoEncodedBuf]
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

private object RowOrderIndepUDFTestCounters {
  val rowOrderIndepCalls = new AtomicInteger(0)
  val legacyNondetCalls = new AtomicInteger(0)
  val parquetViewCalls = new AtomicInteger(0)
  val chainingCalls = new AtomicInteger(0)
  val wholeOptimizerCalls = new AtomicInteger(0)
}

class UDFSuite extends SharedSparkSession {
  import testImplicits._

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
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("substr('abcd', 2, 3, 4)")
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> toSQLId("substr"),
        "expectedNum" -> "[2, 3]",
        "actualNum" -> "4",
        "docroot" -> SPARK_DOC_ROOT
      ),
      context = ExpectedContext(
        fragment = "substr('abcd', 2, 3, 4)",
        start = 0,
        stop = 22)
    )
  }

  test("error reporting for incorrect number of arguments - udf") {
    val df = spark.emptyDataFrame
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("foo", (_: String).length)
        df.selectExpr("foo(2, 3, 4)")
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> toSQLId("foo"),
        "expectedNum" -> "1",
        "actualNum" -> "3",
        "docroot" -> SPARK_DOC_ROOT
      ),
      context = ExpectedContext(
        fragment = "foo(2, 3, 4)",
        start = 0,
        stop = 11)
    )
  }

  test("error reporting for undefined functions") {
    val sqlText = "a_function_that_does_not_exist()"
    checkError(
      exception = intercept[AnalysisException] {
        spark.emptyDataFrame.selectExpr(sqlText)
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`a_function_that_does_not_exist`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 31))
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
          """.stripMargin).toDF(), arrayData)
    checkAnswer(
      sql("""
           | SELECT mapDataFunc(data) AS t FROM mapData
          """.stripMargin).toDF(), mapData)
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
        spark.sql("SELECT f(a._1) FROM x").show()
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
      spark.udf.register("f", (a: Int, b: String) => s"$a$b")
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
      map.transform((_, value) => if (value == null) null else value.toBigInteger.toString)
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
    checkError(
      exception =
        intercept[AnalysisException](df.select(myUdf(Column("col")))),
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`b`",
        "proposal" -> "`a`"),
      context = ExpectedContext("apply", ".*"))
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
    assert(e1.getCondition == "FAILED_EXECUTE_UDF")
    assert(e1.getCause.getStackTrace.head.toString.contains(
      "UDFSuite$MalformedClassObject$MalformedNonPrimitiveFunction"))

    val e2 = intercept[SparkException] {
      Seq(20).toDF("col").select(udf(f2).apply(Column("col"))).collect()
    }
    assert(e2.getCondition == "FAILED_EXECUTE_UDF")
    assert(e2.getCause.getStackTrace.head.toString.contains(
      "UDFSuite$MalformedClassObject$MalformedPrimitiveFunction"))
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

  test("SPARK-32459: UDF should not fail on mutable.ArraySeq") {
    val myUdf = udf((a: mutable.ArraySeq[Int]) =>
      mutable.ArraySeq.make[Int](Array(a.head + 99)))
    checkAnswer(Seq(Array(1))
      .toDF("col")
      .select(myUdf(Column("col"))),
      Row(ArrayBuffer(100)))
  }

  test("SPARK-46586: UDF should not fail on immutable.ArraySeq") {
    val myUdf1 = udf((a: immutable.ArraySeq[Int]) =>
      immutable.ArraySeq.unsafeWrapArray[Int](Array(a.head + 99)))
    checkAnswer(Seq(Array(1))
      .toDF("col")
      .select(myUdf1(Column("col"))),
    Row(ArrayBuffer(100)))

    val myUdf2 = udf((a: immutable.ArraySeq[Int]) =>
      immutable.ArraySeq.unsafeWrapArray[Int]((a :+ 5 :+ 6).toArray))
    checkAnswer(Seq(Array(1, 2, 3))
      .toDF("col")
      .select(myUdf2(Column("col"))),
    Row(ArrayBuffer(1, 2, 3, 5, 6)))
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
    }
    assert(e.getCondition == "FAILED_EXECUTE_UDF")
    assert(e.getCause.isInstanceOf[java.lang.ArithmeticException])
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
    }
    assert(e.getCondition == "FAILED_EXECUTE_UDF")
    assert(e.getCause.isInstanceOf[java.lang.ArithmeticException])
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
    }
    assert(e.getCondition == "FAILED_EXECUTE_UDF")
    assert(e.getCause.isInstanceOf[java.lang.ArithmeticException])
  }

  test("SPARK-43099: UDF className is correctly populated") {
    spark.udf.register("dummyUDF", (x: Int) => x + 1)
    val expressionInfo = spark.sessionState.catalog
      .lookupFunctionInfo(FunctionIdentifier("dummyUDF"))
    assert(expressionInfo.getClassName.contains(
      "org.apache.spark.sql.classic.UDFRegistration$$Lambda"))
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

  test("SPARK-47927: Correctly pass null values derived from join to UDF") {
    val f = udf[Tuple1[Option[Int]], Tuple1[Option[Int]]](identity)
    val ds1 = Seq(1).toDS()
    val ds2 = Seq[Int]().toDS()

    checkAnswer(
      ds1.join(ds2, ds1("value") === ds2("value"), "left_outer")
        .select(f(struct(ds2("value").as("_1")))),
      Row(Row(null)))
  }

  test("SPARK-51402: Test TimeType in UDF") {
    // Mocks
    val mockTimeStr = "00:00:00.000000"
    val input = Seq(LocalTime.parse(mockTimeStr)).toDF("currentTime")
    // Regular case
    val plusHour = udf((l: LocalTime) => l.plusHours(1))
    val result = input.select(plusHour($"currentTime").as("newTime"))
    checkAnswer(result, Row(LocalTime.parse("01:00:00.000000")) :: Nil)
    assert(result.schema === new StructType().add("newTime", TimeType()))
    // UDF produces `null`
    val nullFunc = udf((_: LocalTime) => null.asInstanceOf[LocalTime])
    val nullResult = input.select(nullFunc($"currentTime").as("nullTime"))
    checkAnswer(nullResult, Row(null) :: Nil)
    assert(nullResult.schema === new StructType().add("nullTime", TimeType()))
    // Input parameter of UDF is null
    val nullInput = Seq(null.asInstanceOf[LocalTime]).toDF("nullTime")
    val constTime = udf((_: LocalTime) =>
      LocalTime.parse(mockTimeStr))
    val constResult = nullInput.select(constTime($"nullTime").as("zeroHour"))
    checkAnswer(constResult, Row(LocalTime.parse(mockTimeStr)) :: Nil)
    assert(constResult.schema === new StructType().add("zeroHour", TimeType()))
    // Error in the conversion of UDF result to the internal representation of time
    val invalidFunc = udf((l: LocalTime) => "Zero".toLong)
    val e = intercept[SparkException] {
      input.select(invalidFunc($"currentTime")).collect()
    }
    assert(e.getCause.isInstanceOf[java.lang.NumberFormatException])
  }

  test("char/varchar as UDF return type") {
    Seq(CharType(5), VarcharType(5)).foreach { dt =>
      val f = udf(
        new UDF0[String] {
          override def call(): String = "a"
        },
        dt
      )
      checkError(
        intercept[AnalysisException](spark.range(1).select(f()).encoder),
        condition = "UNSUPPORTED_DATA_TYPE_FOR_ENCODER",
        sqlState = "0A000",
        parameters = Map("dataType" -> s"\"${dt.sql}\"")
      )
    }
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      Seq(CharType(5), VarcharType(5)).foreach { dt =>
        val f = udf(
          new UDF0[String] {
            override def call(): String = "a"
          },
          dt
        )
        val df = spark.range(1).select(f().as("c"))
        assert(df.schema.head.dataType === dt)
        assert(df.encoder.schema.head.dataType === dt)
        val expected = if (dt.isInstanceOf[CharType]) "a    " else "a"
        checkAnswer(df, Row(expected))
      }
    }
  }

  test("SPARK-47927: ScalaUDF null handling") {
    val f = udf[Int, Int](_ + 1)
    val df = Seq(Some(1), None).toDF("c")
      .select(f($"c").as("f"), f($"f"))
    checkAnswer(df, Seq(Row(2, 3), Row(null, null)))
  }

  test("SPARK-61935: ScalaUDF null handling of a column made nullable by an outer join") {
    val f = udf[Int, Int](_ + 1)
    val ds1 = Seq(1, 2).toDS()
    val ds2 = Seq(2).toDS()
    // `ds2("value")` is resolved as non-nullable, and becomes nullable after resolution
    checkAnswer(
      ds1.join(ds2, ds1("value") === ds2("value"), "left_outer").select(f(ds2("value"))),
      Row(null) :: Row(3) :: Nil)
  }

  test("SPARK-61935: grouping sets, CTE and generator over a ScalaUDF with a primitive input") {
    withTempView("gs") {
      withUserDefinedFunction("plus1" -> true) {
        spark.udf.register("plus1", (x: Int) => x + 1)
        Seq((1, None), (2, Some(201)), (3, None)).toDF("pk", "salary")
          .createOrReplaceTempView("gs")

        checkAnswer(
          sql("SELECT plus1(salary) AS k, count(*) FROM gs " +
            "GROUP BY GROUPING SETS ((plus1(salary)))"),
          Row(null, 2) :: Row(202, 1) :: Nil)

        // a CTE that is not inlined
        checkAnswer(
          sql(
            """WITH c AS (SELECT pk, plus1(salary) AS k, rand() AS r FROM gs)
              |SELECT a.pk, a.k, a.k IS NULL FROM c a JOIN c b ON a.pk = b.pk
              |""".stripMargin),
          Row(1, null, true) :: Row(2, 202, false) :: Row(3, null, true) :: Nil)

        val df = sql("SELECT explode(array(plus1(a), plus1(b))) AS v " +
          "FROM VALUES (1, 2), (NULL, 3) AS t(a, b)")
        assert(df.schema("v").nullable)
        checkAnswer(df, Row(2) :: Row(3) :: Row(null) :: Row(4) :: Nil)
      }
    }
  }

  test("SPARK-52819: Support using Kryo to encode BUF in Aggregator") {
    val kryoBufUDAF = udaf(KryoBufAggregator())
    val input = Seq(1L, 2L, 3L).toDF("value")
    val result = input.select(kryoBufUDAF($"value").as("sum"))
    checkAnswer(result, Row(6L) :: Nil)
  }

  test("SPARK-60762: The types of a UDF are not in the generated code") {
    spark.udf.register("rowSize", (r: Row) => r.size)
    val df = sql("""SELECT rowSize(named_struct('fieldName"b', id)) AS r FROM range(2)""")
    val code = df.queryExecution.executedPlan.collect {
      case w: WholeStageCodegenExec => w.doCodeGen()._2.body
    }
    assert(code.nonEmpty && code.forall(!_.contains("fieldName")))
    checkAnswer(df, Row(1) :: Row(1) :: Nil)
  }

  test("rowOrderIndependent UDF filter pushdown and call counts") {
    val rowOrderIndepFn = udf((v: Int) => {
      RowOrderIndepUDFTestCounters.rowOrderIndepCalls.incrementAndGet()
      v * 2
    }).asRowOrderIndependentNondeterministic()

    val legacyNondetFn = udf((v: Int) => {
      RowOrderIndepUDFTestCounters.legacyNondetCalls.incrementAndGet()
      v * 2
    }).asNondeterministic()

    val input = (1 to 20).map(i => (i, i - 10)).toDF("a", "b")

    // Disjoint deterministic filter pushes below Project; UDF runs only on surviving rows.
    RowOrderIndepUDFTestCounters.rowOrderIndepCalls.set(0)
    val dfRoiPush = input.select($"a", rowOrderIndepFn($"b").as("c")).filter($"a" > 15)
    assert(dfRoiPush.queryExecution.optimizedPlan.collect {
      case p: Project if p.child.isInstanceOf[LogicalFilter] => p
    }.nonEmpty)
    assert(dfRoiPush.collect().length === 5)
    assert(RowOrderIndepUDFTestCounters.rowOrderIndepCalls.get() === 5)

    // Mixed conjuncts split cleanly: a > 10 pushes below Project, c > 0 stays above.
    RowOrderIndepUDFTestCounters.rowOrderIndepCalls.set(0)
    val dfMixed = input.select($"a", rowOrderIndepFn($"b").as("c")).filter($"c" > 0 && $"a" > 10)
    val mixedResult = dfMixed.collect()
    assert(mixedResult.length === 10)
    assert(mixedResult.forall(_.getInt(1) > 0))
    assert(RowOrderIndepUDFTestCounters.rowOrderIndepCalls.get() === 10)

    // Legacy .asNondeterministic() blocks filter pushdown; UDF runs on all 20 rows.
    RowOrderIndepUDFTestCounters.legacyNondetCalls.set(0)
    val dfLegacy = input.select($"a", legacyNondetFn($"b").as("c")).filter($"a" > 15)
    assert(dfLegacy.queryExecution.optimizedPlan.collect {
      case f: LogicalFilter if f.child.isInstanceOf[Project] => f
    }.nonEmpty)
    assert(dfLegacy.collect().length === 5)
    assert(RowOrderIndepUDFTestCounters.legacyNondetCalls.get() === 20)

    // Built-in rand(0) remains blocked from pushdown and preserves baseline output bit-for-bit.
    val randBaseline = input.select($"a", rand(0).as("r")).collect()
      .map(r => (r.getInt(0), r.getDouble(1))).toMap
    val randFiltered = input.select($"a", rand(0).as("r")).filter($"a" > 15).collect()
    assert(randFiltered.forall(r => r.getDouble(1) == randBaseline(r.getInt(0))))
  }

  test("whole SparkOptimizer preserves single ScalaUDF instance and 1x invocation count") {
    val roiStrFn = udf((s: String) => {
      RowOrderIndepUDFTestCounters.wholeOptimizerCalls.incrementAndGet()
      s"${s}_x"
    }).asRowOrderIndependentNondeterministic()

    withTempPath { dir =>
      (1 to 20).map(i => (i, s"val_$i")).toDF("a", "s")
        .write.parquet(dir.getCanonicalPath)
      val base = spark.read.parquet(dir.getCanonicalPath)

      def countScalaUdfs(df: DataFrame): Int = {
        df.queryExecution.optimizedPlan.collect {
          case p => p.expressions.flatMap(_.collect { case u: ScalaUDF => u }).size
        }.sum
      }

      // (1) Single-reference projection collapse + filter pushdown across full SparkOptimizer.
      RowOrderIndepUDFTestCounters.wholeOptimizerCalls.set(0)
      val dfSingleCollapse = base
        .select($"a", roiStrFn($"s").as("c"))
        .select($"a", coalesce(upper($"c"), lit("")).as("u"))
        .filter($"a" > 15)
      assert(countScalaUdfs(dfSingleCollapse) === 1)
      assert(dfSingleCollapse.queryExecution.optimizedPlan.collect {
        case p: Project => p
      }.size === 1)
      val singleCollapseRes = dfSingleCollapse.collect().map(_.getInt(0)).sorted
      assert(singleCollapseRes.toSeq === (16 to 20))
      assert(RowOrderIndepUDFTestCounters.wholeOptimizerCalls.get() === 5)

      // (2) Single-reference projection + post-UDF LIKE predicate across full SparkOptimizer.
      RowOrderIndepUDFTestCounters.wholeOptimizerCalls.set(0)
      val dfSingleLike = base
        .select($"a", roiStrFn($"s").as("c"))
        .select($"a", upper($"c").as("u"))
        .filter($"a" > 15 && $"u".like("%X%"))
      assert(countScalaUdfs(dfSingleLike) === 1)
      val singleLikeRes = dfSingleLike.collect().map(_.getInt(0)).sorted
      assert(singleLikeRes.toSeq === (16 to 20))
      assert(RowOrderIndepUDFTestCounters.wholeOptimizerCalls.get() === 5)

      // (3) Multi-reference consumer across full SparkOptimizer keeps AST count == 1 and 1x calls.
      RowOrderIndepUDFTestCounters.wholeOptimizerCalls.set(0)
      val dfMultiRef = base
        .select($"a", roiStrFn($"s").as("c"))
        .select($"a", concat($"c", lit("_1")).as("u1"), concat($"c", lit("_2")).as("u2"))
        .filter($"a" > 15)
      assert(countScalaUdfs(dfMultiRef) === 1)
      val multiRes = dfMultiRef.collect().map(_.getInt(0)).sorted
      assert(multiRes.toSeq === (16 to 20))
      assert(RowOrderIndepUDFTestCounters.wholeOptimizerCalls.get() === 5)
    }
  }

  test("opaque temp view pushes partition/data filters into Parquet scan") {
    withTempPath { dir =>
      val rows = for {
        dt <- Seq("2026-01-14", "2026-01-15")
        country <- Seq("US", "CA")
        i <- 1 to 10
      } yield (i, country, dt, s"$country-$i")
      rows.toDF("id", "country", "event_date", "payload")
        .repartition(1)
        .write
        .partitionBy("event_date")
        .parquet(dir.getCanonicalPath)

      RowOrderIndepUDFTestCounters.parquetViewCalls.set(0)
      val scoreUdf = udf((payload: String) => {
        RowOrderIndepUDFTestCounters.parquetViewCalls.incrementAndGet()
        payload.length
      }).asRowOrderIndependentNondeterministic()

      withTempView("events", "scored_events") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("events")
        spark.udf.register("score_udf", scoreUdf)
        sql(
          """
            |CREATE TEMP VIEW scored_events AS
            |SELECT id, country, event_date, score_udf(payload) AS score
            |FROM events
          """.stripMargin)

        val df = sql(
          """
            |SELECT id, score
            |FROM scored_events
            |WHERE event_date = '2026-01-15' AND country = 'US'
          """.stripMargin)
        assert(df.collect().length === 10)
        assert(RowOrderIndepUDFTestCounters.parquetViewCalls.get() === 10)

        val scan = df.queryExecution.executedPlan.collectFirst {
          case s: FileSourceScanExec => s
        }.get
        assert(scan.partitionFilters.nonEmpty)
        assert(scan.dataFilters.nonEmpty)
      }
    }
  }

  test("ANSI throwable and Divide predicate ordering and error suppression") {
    val input = Seq((1, 0, 10), (2, 1, 20)).toDF("id", "step", "b")
    val rowOrderIndepFn = udf((b: Int) => if (b == 10) -1 else 1)
      .asRowOrderIndependentNondeterministic()
    val throwableFn = udf((b: Int) => {
      if (b <= 10) throw new IllegalArgumentException("bad row")
      b
    }).asRowOrderIndependentNondeterministic()
    val detThrowingUdf = udf((step: Int) => {
      if (step == 0) throw new IllegalArgumentException("det boom")
      step > 0
    })

    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        // When c <= 0 for step == 0, sequence(1, 3, step) (throwable = true) must stay above
        // Project so c > 0 short-circuits without throwing INVALID_PARAMETER_VALUE.ZERO.
        val dfSafe = input.select($"id", $"step", rowOrderIndepFn($"b").as("c"))
          .filter($"c" > 0 && size(sequence(lit(1), lit(3), $"step")) > 0)
        checkAnswer(dfSafe, Row(2, 1, 1) :: Nil)

        // Likewise, ANSI 10 / step > 1 (throwable == false, !canEvaluateUnconditionally) written
        // AFTER stayed-up c > 0 must stay above Project and never throw DIVIDE_BY_ZERO on step == 0
        // while trailing NoThrow (id > 0) still pushes cleanly.
        val dfAnsiDivSafe = input.select($"id", $"step", rowOrderIndepFn($"b").as("c"))
          .filter($"c" > 0 && (lit(10) / $"step" > 1) && $"id" > 0)
        checkAnswer(dfAnsiDivSafe, Row(2, 1, 1) :: Nil)

        // Deterministic throwing ScalaUDF (!ExprUtils.canEvaluateUnconditionally) after stayed-up
        // c > 0 stays above Project without throwing on step == 0, while id > 0 pushes below.
        val dfDetUdfSafe = input.select($"id", $"step", rowOrderIndepFn($"b").as("c"))
          .filter($"c" > 0 && detThrowingUdf($"step") && $"id" > 0)
        assert(dfDetUdfSafe.queryExecution.optimizedPlan.collect {
          case p: Project if p.child.isInstanceOf[LogicalFilter] => p
        }.nonEmpty)
        checkAnswer(dfDetUdfSafe, Row(2, 1, 1) :: Nil)

        // When step =!= 0 precedes 10 / step > 1 and sequence, the guarded prefix pushes below
        // Project in exact written order without throwing.
        val dfPushBoth = input.select($"id", $"step", rowOrderIndepFn($"b").as("c"))
          .filter(
            $"step" =!= 0 &&
              (lit(10) / $"step" > 1) &&
              size(sequence(lit(1), lit(3), $"step")) > 0 &&
              $"c" > 0)
        checkAnswer(dfPushBoth, Row(2, 1, 1) :: Nil)

        // Pushing id > 1 below Project suppresses throwableUDF error on id == 1 row.
        val dfSuppress = input.select($"id", throwableFn($"b").as("v")).filter($"id" > 1)
        checkAnswer(dfSuppress, Row(2, 20) :: Nil)
      }
    }
  }

  test("chaining reset, registration, and UDAF rejection") {
    val base = udf((x: Int) => {
      RowOrderIndepUDFTestCounters.chainingCalls.incrementAndGet()
      x + 1
    })
    assert(base.deterministic)
    assert(base.rowOrderIndependent)

    val rowIndep = base.asRowOrderIndependentNondeterministic()
    assert(!rowIndep.deterministic)
    assert(rowIndep.rowOrderIndependent)

    val resetNondet = rowIndep.asNondeterministic()
    assert(!resetNondet.deterministic)
    assert(!resetNondet.rowOrderIndependent)

    val reEnabled = resetNondet.asRowOrderIndependentNondeterministic()
    assert(!reEnabled.deterministic)
    assert(reEnabled.rowOrderIndependent)

    // Verify resetNondet blocks pushdown while reEnabled allows it.
    val dfInput = (1 to 10).toDF("a")
    RowOrderIndepUDFTestCounters.chainingCalls.set(0)
    assert(dfInput.select($"a", resetNondet($"a").as("c")).filter($"a" > 8).collect().length === 2)
    assert(RowOrderIndepUDFTestCounters.chainingCalls.get() === 10)

    RowOrderIndepUDFTestCounters.chainingCalls.set(0)
    assert(dfInput.select($"a", reEnabled($"a").as("c")).filter($"a" > 8).collect().length === 2)
    assert(RowOrderIndepUDFTestCounters.chainingCalls.get() === 2)

    // Verify SQL registration preserves rowOrderIndependent.
    spark.udf.register("row_indep_inc", reEnabled)
    withTempView("src_n12") {
      dfInput.createOrReplaceTempView("src_n12")
      RowOrderIndepUDFTestCounters.chainingCalls.set(0)
      val sqlDf = sql("SELECT a, c FROM (SELECT a, row_indep_inc(a) AS c FROM src_n12) WHERE a > 8")
      assert(sqlDf.collect().length === 2)
      assert(RowOrderIndepUDFTestCounters.chainingCalls.get() === 2)
    }

    // Verify UserDefinedAggregator rejects asRowOrderIndependentNondeterministic.
    val uda = udaf(KryoBufAggregator())
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        uda.asRowOrderIndependentNondeterministic()
      },
      condition = "UNSUPPORTED_FEATURE.ROW_ORDER_INDEPENDENT_AGGREGATOR",
      parameters = Map("functionName" -> uda.name))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        uda.withName("bad_uda").asRowOrderIndependentNondeterministic()
      },
      condition = "UNSUPPORTED_FEATURE.ROW_ORDER_INDEPENDENT_AGGREGATOR",
      parameters = Map("functionName" -> "bad_uda"))
  }
}
