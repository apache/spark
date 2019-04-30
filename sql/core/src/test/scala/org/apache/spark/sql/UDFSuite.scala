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

import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener


private case class FunctionResult(f1: String, f2: String)

class UDFSuite extends QueryTest with SharedSQLContext {
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

    val bar = udf(() => Math.random(), DataTypes.DoubleType).asNondeterministic()
    val df2 = testData.select(bar())
    assert(df2.logicalPlan.asInstanceOf[Project].projectList.forall(!_.deterministic))
    assert(df2.head().getDouble(0) >= 0.0)

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
      val explain = ExplainCommand(df.queryExecution.logical, extended = false)
      val sparkPlan = spark.sessionState.executePlan(explain).executedPlan
      sparkPlan.executeCollect().map(_.getString(0).trim).headOption.getOrElse("")
    }
    val udf1Name = "myUdf1"
    val udf2Name = "myUdf2"
    val udf1 = spark.udf.register(udf1Name, (n: Int) => n + 1)
    val udf2 = spark.udf.register(udf2Name, (n: Int) => n * 1)
    assert(explainStr(sql("SELECT myUdf1(myUdf2(1))")).contains(s"UDF:$udf1Name(UDF:$udf2Name(1))"))
    assert(explainStr(spark.range(1).select(udf1(udf2(functions.lit(1)))))
      .contains(s"UDF:$udf1Name(UDF:$udf2Name(1))"))
  }

  test("SPARK-23666 Do not display exprId in argument names") {
    withTempView("x") {
      Seq(((1, 2), 3)).toDF("a", "b").createOrReplaceTempView("x")
      spark.udf.register("f", (a: Int) => a)
      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        spark.sql("SELECT f(a._1) FROM x").show
      }
      assert(outputStream.toString.contains("UDF:f(a._1 AS `_1`)"))
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
        sparkContext.listenerBus.waitUntilEmpty(1000)
        assert(numTotalCachedHit == 1, "expected to be cached in saveAsTable")
        df.write.insertInto("t")
        sparkContext.listenerBus.waitUntilEmpty(1000)
        assert(numTotalCachedHit == 2, "expected to be cached in insertInto")
        df.write.save(path.getCanonicalPath)
        sparkContext.listenerBus.waitUntilEmpty(1000)
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
    val df = input.select(udf1('a, 'b, 'c))
    checkAnswer(df, Seq(Row("null1x"), Row(null), Row("N3null")))

    // test Java UDF. Java UDF can't have primitive inputs, as it's generic typed.
    val udf2 = udf(new UDF3[String, Integer, Object, String] {
      override def call(t1: String, t2: Integer, t3: Object): String = {
        t1 + t2 + t3
      }
    }, StringType)
    val df2 = input.select(udf2('a, 'b, 'c))
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
    val f = udf((x: Int) => x, IntegerType)
    checkAnswer(
      Seq(new Integer(1), null).toDF("x").select(f($"x")),
      Row(1) :: Row(0) :: Nil)

    val f2 = udf((x: Double) => x, DoubleType)
    checkAnswer(
      Seq(new java.lang.Double(1.1), null).toDF("x").select(f2($"x")),
      Row(1.1) :: Row(0.0) :: Nil)

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
      map.mapValues(value => if (value == null) null else value.toBigInteger.toString)
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
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val expected = java.time.Instant.parse("2019-02-27T00:00:00Z")
      val plusSec = udf((i: java.time.Instant) => i.plusSeconds(1))
      val df = spark.sql("SELECT TIMESTAMP '2019-02-26 23:59:59Z' as t")
        .select(plusSec('t))
      assert(df.collect().toSeq === Seq(Row(expected)))
    }
  }

  test("Using java.time.LocalDate in UDF") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val expected = java.time.LocalDate.parse("2019-02-27")
      val plusDay = udf((i: java.time.LocalDate) => i.plusDays(1))
      val df = spark.sql("SELECT DATE '2019-02-26' as d")
        .select(plusDay('d))
      assert(df.collect().toSeq === Seq(Row(expected)))
    }
  }
}
