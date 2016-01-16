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

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData._

private case class FunctionResult(f1: String, f2: String)

class UDFSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("built-in fixed arity expressions") {
    val df = sqlContext.emptyDataFrame
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
    df.registerTempTable("tmp_table")
    checkAnswer(sql("select spark_partition_id() from tmp_table").toDF(), Row(0))
    sqlContext.dropTempTable("tmp_table")
  }

  test("SPARK-8005 input_file_name") {
    withTempPath { dir =>
      val data = sparkContext.parallelize(0 to 10, 2).toDF("id")
      data.write.parquet(dir.getCanonicalPath)
      sqlContext.read.parquet(dir.getCanonicalPath).registerTempTable("test_table")
      val answer = sql("select input_file_name() from test_table").head().getString(0)
      assert(answer.contains(dir.getCanonicalPath))
      assert(sql("select input_file_name() from test_table").distinct().collect().length >= 2)
      sqlContext.dropTempTable("test_table")
    }
  }

  test("error reporting for incorrect number of arguments") {
    val df = sqlContext.emptyDataFrame
    val e = intercept[AnalysisException] {
      df.selectExpr("substr('abcd', 2, 3, 4)")
    }
    assert(e.getMessage.contains("arguments"))
  }

  test("error reporting for undefined functions") {
    val df = sqlContext.emptyDataFrame
    val e = intercept[AnalysisException] {
      df.selectExpr("a_function_that_does_not_exist()")
    }
    assert(e.getMessage.contains("undefined function"))
  }

  test("Simple UDF") {
    sqlContext.udf.register("strLenScala", (_: String).length)
    assert(sql("SELECT strLenScala('test')").head().getInt(0) === 4)
  }

  test("ZeroArgument UDF") {
    sqlContext.udf.register("random0", () => { Math.random()})
    assert(sql("SELECT random0()").head().getDouble(0) >= 0.0)
  }

  test("TwoArgument UDF") {
    sqlContext.udf.register("strLenScala", (_: String).length + (_: Int))
    assert(sql("SELECT strLenScala('test', 1)").head().getInt(0) === 5)
  }

  test("UDF in a WHERE") {
    sqlContext.udf.register("oneArgFilter", (n: Int) => { n > 80 })

    val df = sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString))).toDF()
    df.registerTempTable("integerData")

    val result =
      sql("SELECT * FROM integerData WHERE oneArgFilter(key)")
    assert(result.count() === 20)
  }

  test("UDF in a HAVING") {
    sqlContext.udf.register("havingFilter", (n: Long) => { n > 5 })

    val df = Seq(("red", 1), ("red", 2), ("blue", 10),
      ("green", 100), ("green", 200)).toDF("g", "v")
    df.registerTempTable("groupData")

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

  test("UDF in a GROUP BY") {
    sqlContext.udf.register("groupFunction", (n: Int) => { n > 10 })

    val df = Seq(("red", 1), ("red", 2), ("blue", 10),
      ("green", 100), ("green", 200)).toDF("g", "v")
    df.registerTempTable("groupData")

    val result =
      sql(
        """
         | SELECT SUM(v)
         | FROM groupData
         | GROUP BY groupFunction(v)
        """.stripMargin)
    assert(result.count() === 2)
  }

  test("UDFs everywhere") {
    sqlContext.udf.register("groupFunction", (n: Int) => { n > 10 })
    sqlContext.udf.register("havingFilter", (n: Long) => { n > 2000 })
    sqlContext.udf.register("whereFilter", (n: Int) => { n < 150 })
    sqlContext.udf.register("timesHundred", (n: Long) => { n * 100 })

    val df = Seq(("red", 1), ("red", 2), ("blue", 10),
      ("green", 100), ("green", 200)).toDF("g", "v")
    df.registerTempTable("groupData")

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

  test("struct UDF") {
    sqlContext.udf.register("returnStruct", (f1: String, f2: String) => FunctionResult(f1, f2))

    val result =
      sql("SELECT returnStruct('test', 'test2') as ret")
        .select($"ret.f1").head().getString(0)
    assert(result === "test")
  }

  test("udf that is transformed") {
    sqlContext.udf.register("makeStruct", (x: Int, y: Int) => (x, y))
    // 1 + 1 is constant folded causing a transformation.
    assert(sql("SELECT makeStruct(1 + 1, 2)").first().getAs[Row](0) === Row(2, 2))
  }

  test("type coercion for udf inputs") {
    sqlContext.udf.register("intExpected", (x: Int) => x)
    // pass a decimal to intExpected.
    assert(sql("SELECT intExpected(1.0)").head().getInt(0) === 1)
  }

  test("udf in different types") {
    sqlContext.udf.register("testDataFunc", (n: Int, s: String) => { (n, s) })
    sqlContext.udf.register("decimalDataFunc",
      (a: java.math.BigDecimal, b: java.math.BigDecimal) => { (a, b) })
    sqlContext.udf.register("binaryDataFunc", (a: Array[Byte], b: Int) => { (a, b) })
    sqlContext.udf.register("arrayDataFunc",
      (data: Seq[Int], nestedData: Seq[Seq[Int]]) => { (data, nestedData) })
    sqlContext.udf.register("mapDataFunc",
      (data: scala.collection.Map[Int, String]) => { data })
    sqlContext.udf.register("complexDataFunc",
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
    val myUDF = sqlContext.udf.register("testDataFunc", (n: Int, s: String) => { (n, s.toInt) })

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
}
