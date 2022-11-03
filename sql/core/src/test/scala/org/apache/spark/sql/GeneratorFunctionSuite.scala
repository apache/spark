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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class GeneratorFunctionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("stack") {
    val df = spark.range(1)

    // Empty DataFrame suppress the result generation
    checkAnswer(spark.emptyDataFrame.selectExpr("stack(1, 1, 2, 3)"), Nil)

    // Rows & columns
    checkAnswer(df.selectExpr("stack(1, 1, 2, 3)"), Row(1, 2, 3) :: Nil)
    checkAnswer(df.selectExpr("stack(2, 1, 2, 3)"), Row(1, 2) :: Row(3, null) :: Nil)
    checkAnswer(df.selectExpr("stack(3, 1, 2, 3)"), Row(1) :: Row(2) :: Row(3) :: Nil)
    checkAnswer(df.selectExpr("stack(4, 1, 2, 3)"), Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)

    // Various column types
    checkAnswer(df.selectExpr("stack(3, 1, 1.1, 'a', 2, 2.2, 'b', 3, 3.3, 'c')"),
      Row(1, 1.1, "a") :: Row(2, 2.2, "b") :: Row(3, 3.3, "c") :: Nil)

    // Null values
    checkAnswer(df.selectExpr("stack(3, 1, 1.1, null, 2, null, 'b', null, 3.3, 'c')"),
      Row(1, 1.1, null) :: Row(2, null, "b") :: Row(null, 3.3, "c") :: Nil)

    // Repeat generation at every input row
    checkAnswer(spark.range(2).selectExpr("stack(2, 1, 2, 3)"),
      Row(1, 2) :: Row(3, null) :: Row(1, 2) :: Row(3, null) :: Nil)

    // The first argument must be a positive constant integer.
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("stack(1.1, 1, 2, 3)")
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"stack(1.1, 1, 2, 3)\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"1.1\"",
        "inputType" -> "\"DECIMAL(2,1)\"",
        "requiredType" -> "\"INT\""),
      context = ExpectedContext(
        fragment = "stack(1.1, 1, 2, 3)",
        start = 0,
        stop = 18
      )
    )

    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("stack(-1, 1, 2, 3)")
      },
      errorClass = "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
      parameters = Map(
        "sqlExpr" -> "\"stack(-1, 1, 2, 3)\"",
        "exprName" -> "`n`",
        "valueRange" -> "(0, 2147483647]",
        "currentValue" -> "-1"),
      context = ExpectedContext(
        fragment = "stack(-1, 1, 2, 3)",
        start = 0,
        stop = 17
      )
    )

    // The data for the same column should have the same type.
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("stack(2, 1, '2.2')")
      },
      errorClass = "DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES",
      parameters = Map(
        "sqlExpr" -> "\"stack(2, 1, 2.2)\"",
        "columnIndex" -> "0",
        "leftParamIndex" -> "1",
        "leftType" -> "\"INT\"",
        "rightParamIndex" -> "2",
        "rightType" -> "\"STRING\""),
      context = ExpectedContext(
        fragment = "stack(2, 1, '2.2')",
        start = 0,
        stop = 17
      )
    )

    // stack on column data
    val df2 = Seq((2, 1, 2, 3)).toDF("n", "a", "b", "c")
    checkAnswer(df2.selectExpr("stack(2, a, b, c)"), Row(1, 2) :: Row(3, null) :: Nil)

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("stack(n, a, b, c)")
      },
      errorClass = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      parameters = Map(
        "sqlExpr" -> "\"stack(n, a, b, c)\"",
        "inputName" -> "n",
        "inputType" -> "\"INT\"",
        "inputExpr" -> "\"n\""),
      context = ExpectedContext(
        fragment = "stack(n, a, b, c)",
        start = 0,
        stop = 16
      )
    )

    val df3 = Seq((2, 1, 2.0)).toDF("n", "a", "b")
    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("stack(2, a, b)")
      },
      errorClass = "DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES",
      parameters = Map(
        "sqlExpr" -> "\"stack(2, a, b)\"",
        "columnIndex" -> "0",
        "leftParamIndex" -> "1",
        "leftType" -> "\"INT\"",
        "rightParamIndex" -> "2",
        "rightType" -> "\"DOUBLE\""),
      context = ExpectedContext(
        fragment = "stack(2, a, b)",
        start = 0,
        stop = 13
      )
    )
  }

  test("single explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    checkAnswer(
      df.select(explode($"intList")),
      Row(1) :: Row(2) :: Row(3) :: Nil)
  }

  test("single explode_outer") {
    val df = Seq((1, Seq(1, 2, 3)), (2, Seq())).toDF("a", "intList")
    checkAnswer(
      df.select(explode_outer($"intList")),
      Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)
  }

  test("single posexplode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    checkAnswer(
      df.select(posexplode($"intList")),
      Row(0, 1) :: Row(1, 2) :: Row(2, 3) :: Nil)
  }

  test("single posexplode_outer") {
    val df = Seq((1, Seq(1, 2, 3)), (2, Seq())).toDF("a", "intList")
    checkAnswer(
      df.select(posexplode_outer($"intList")),
      Row(0, 1) :: Row(1, 2) :: Row(2, 3) :: Row(null, null) :: Nil)
  }

  test("explode and other columns") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")

    checkAnswer(
      df.select($"a", explode($"intList")),
      Row(1, 1) ::
      Row(1, 2) ::
      Row(1, 3) :: Nil)

    checkAnswer(
      df.select($"*", explode($"intList")),
      Row(1, Seq(1, 2, 3), 1) ::
      Row(1, Seq(1, 2, 3), 2) ::
      Row(1, Seq(1, 2, 3), 3) :: Nil)
  }

  test("explode_outer and other columns") {
    val df = Seq((1, Seq(1, 2, 3)), (2, Seq())).toDF("a", "intList")

    checkAnswer(
      df.select($"a", explode_outer($"intList")),
      Row(1, 1) ::
        Row(1, 2) ::
        Row(1, 3) ::
        Row(2, null) ::
        Nil)

    checkAnswer(
      df.select($"*", explode_outer($"intList")),
      Row(1, Seq(1, 2, 3), 1) ::
        Row(1, Seq(1, 2, 3), 2) ::
        Row(1, Seq(1, 2, 3), 3) ::
        Row(2, Seq(), null) ::
        Nil)
  }

  test("aliased explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")

    checkAnswer(
      df.select(explode($"intList").as("int")).select($"int"),
      Row(1) :: Row(2) :: Row(3) :: Nil)

    checkAnswer(
      df.select(explode($"intList").as("int")).select(sum($"int")),
      Row(6) :: Nil)
  }

  test("aliased explode_outer") {
    val df = Seq((1, Seq(1, 2, 3)), (2, Seq())).toDF("a", "intList")

    checkAnswer(
      df.select(explode_outer($"intList").as("int")).select($"int"),
      Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)

    checkAnswer(
      df.select(explode($"intList").as("int")).select(sum($"int")),
      Row(6) :: Nil)
  }

  test("explode on map") {
    val df = Seq((1, Map("a" -> "b"))).toDF("a", "map")

    checkAnswer(
      df.select(explode($"map")),
      Row("a", "b"))
  }

  test("explode_outer on map") {
    val df = Seq((1, Map("a" -> "b")), (2, Map[String, String]()),
      (3, Map("c" -> "d"))).toDF("a", "map")

    checkAnswer(
      df.select(explode_outer($"map")),
      Row("a", "b") :: Row(null, null) :: Row("c", "d") :: Nil)
  }

  test("explode on map with aliases") {
    val df = Seq((1, Map("a" -> "b"))).toDF("a", "map")

    checkAnswer(
      df.select(explode($"map").as("key1" :: "value1" :: Nil)).select("key1", "value1"),
      Row("a", "b"))
  }

  test("explode_outer on map with aliases") {
    val df = Seq((3, None), (1, Some(Map("a" -> "b")))).toDF("a", "map")

    checkAnswer(
      df.select(explode_outer($"map").as("key1" :: "value1" :: Nil)).select("key1", "value1"),
      Row("a", "b") :: Row(null, null) :: Nil)
  }

  test("self join explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    val exploded = df.select(explode($"intList").as("i"))

    checkAnswer(
      exploded.join(exploded, exploded("i") === exploded("i")).agg(count("*")),
      Row(3) :: Nil)
  }

  test("inline raises exception on array of null type") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.range(2).select(inline(array()))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"inline(array())\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"array()\"",
        "inputType" -> "\"ARRAY<VOID>\"",
        "requiredType" -> "\"ARRAY<STRUCT>\"")
    )
  }

  test("inline with empty table") {
    checkAnswer(
      spark.range(0).select(inline(array(struct(lit(10), lit(100))))),
      Nil)
  }

  test("inline on literal") {
    checkAnswer(
      spark.range(2).select(inline(array(struct(lit(10), lit(100)), struct(lit(20), lit(200)),
        struct(lit(30), lit(300))))),
      Row(10, 100) :: Row(20, 200) :: Row(30, 300) ::
        Row(10, 100) :: Row(20, 200) :: Row(30, 300) :: Nil)
  }

  test("inline on column") {
    val df = Seq((1, 2)).toDF("a", "b")

    checkAnswer(
      df.select(inline(array(struct('a), struct('a)))),
      Row(1) :: Row(1) :: Nil)

    checkAnswer(
      df.select(inline(array(struct('a, 'b), struct('a, 'b)))),
      Row(1, 2) :: Row(1, 2) :: Nil)

    // Spark think [struct<a:int>, struct<b:int>] is heterogeneous due to name difference.
    checkError(
      exception = intercept[AnalysisException] {
        df.select(inline(array(struct('a), struct('b))))
      },
      errorClass = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      parameters = Map(
        "sqlExpr" -> "\"array(struct(a), struct(b))\"",
        "functionName" -> "`array`",
        "dataType" -> "(\"STRUCT<a: INT>\" or \"STRUCT<b: INT>\")"))

    checkAnswer(
      df.select(inline(array(struct('a), struct('b.alias("a"))))),
      Row(1) :: Row(2) :: Nil)

    // Spark think [struct<a:int>, struct<col1:int>] is heterogeneous due to name difference.
    checkError(
      exception = intercept[AnalysisException] {
        df.select(inline(array(struct('a), struct(lit(2)))))
      },
      errorClass = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      parameters = Map(
        "sqlExpr" -> "\"array(struct(a), struct(2))\"",
        "functionName" -> "`array`",
        "dataType" -> "(\"STRUCT<a: INT>\" or \"STRUCT<col1: INT>\")"))

    checkAnswer(
      df.select(inline(array(struct('a), struct(lit(2).alias("a"))))),
      Row(1) :: Row(2) :: Nil)

    checkAnswer(
      df.select(struct('a)).select(inline(array("*"))),
      Row(1) :: Nil)

    checkAnswer(
      df.select(array(struct('a), struct('b.alias("a")))).selectExpr("inline(*)"),
      Row(1) :: Row(2) :: Nil)
  }

  test("inline_outer") {
    val df = Seq((1, "2"), (3, "4"), (5, "6")).toDF("col1", "col2")
    val df2 = df.select(
      when($"col1" === 1, null).otherwise(array(struct($"col1", $"col2"))).as("col1"))
    checkAnswer(
      df2.select(inline('col1)),
      Row(3, "4") :: Row(5, "6") :: Nil
    )
    checkAnswer(
      df2.select(inline_outer('col1)),
      Row(null, null) :: Row(3, "4") :: Row(5, "6") :: Nil
    )
  }

  test("SPARK-14986: Outer lateral view with empty generate expression") {
    checkAnswer(
      sql("select nil from values 1 lateral view outer explode(array()) n as nil"),
      Row(null) :: Nil
    )
  }

  test("outer explode()") {
    checkAnswer(
      sql("select * from values 1, 2 lateral view outer explode(array()) a as b"),
      Row(1, null) :: Row(2, null) :: Nil)
  }

  test("outer generator()") {
    spark.sessionState.functionRegistry
      .createOrReplaceTempFunction("empty_gen", _ => EmptyGenerator(), "scala_udf")
    checkAnswer(
      sql("select * from values 1, 2 lateral view outer empty_gen() a as b"),
      Row(1, null) :: Row(2, null) :: Nil)
  }

  test("generator in aggregate expression") {
    withTempView("t1") {
      Seq((1, 1), (1, 2), (2, 3)).toDF("c1", "c2").createTempView("t1")
      checkAnswer(
        sql("select explode(array(min(c2), max(c2))) from t1"),
        Row(1) :: Row(3) :: Nil
      )
      checkAnswer(
        sql("select posexplode(array(min(c2), max(c2))) from t1 group by c1"),
        Row(0, 1) :: Row(1, 2) :: Row(0, 3) :: Row(1, 3) :: Nil
      )
      // test generator "stack" which require foldable argument
      checkAnswer(
        sql("select stack(2, min(c1), max(c1), min(c2), max(c2)) from t1"),
        Row(1, 2) :: Row(1, 3) :: Nil
      )

      checkError(
        exception = intercept[AnalysisException] {
          sql("select 1 + explode(array(min(c2), max(c2))) from t1 group by c1")
        },
        errorClass = "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS",
        parameters = Map(
          "expression" -> "\"(1 + explode(array(min(c2), max(c2))))\""))


      checkError(
        exception = intercept[AnalysisException] {
          sql(
            """select
              |  explode(array(min(c2), max(c2))),
              |  posexplode(array(min(c2), max(c2)))
              |from t1 group by c1""".stripMargin)
        },
        errorClass = "UNSUPPORTED_GENERATOR.MULTI_GENERATOR",
        parameters = Map(
          "clause" -> "aggregate",
          "num" -> "2",
          "generators" -> ("\"explode(array(min(c2), max(c2)))\", " +
            "\"posexplode(array(min(c2), max(c2)))\"")))
    }
  }

  test("SPARK-30998: Unsupported nested inner generators") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT array(array(1, 2), array(3)) v").select(explode(explode($"v"))).collect
      },
      errorClass = "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS",
      parameters = Map("expression" -> "\"explode(explode(v))\""))
  }

  test("SPARK-30997: generators in aggregate expressions for dataframe") {
    val df = Seq(1, 2, 3).toDF("v")
    checkAnswer(df.select(explode(array(min($"v"), max($"v")))), Row(1) :: Row(3) :: Nil)
  }

  test("SPARK-38528: generator in stream of aggregate expressions") {
    val df = Seq(1, 2, 3).toDF("v")
    checkAnswer(
      df.select(Stream(explode(array(min($"v"), max($"v"))), sum($"v")): _*),
      Row(1, 6) :: Row(3, 6) :: Nil)
  }

  test("SPARK-37947: lateral view <func>_outer()") {
    checkAnswer(
      sql("select * from values 1, 2 lateral view explode_outer(array()) a as b"),
      Row(1, null) :: Row(2, null) :: Nil)

    checkAnswer(
      sql("select * from values 1, 2 lateral view outer explode_outer(array()) a as b"),
      Row(1, null) :: Row(2, null) :: Nil)

    withTempView("t1") {
      sql(
        """select * from values
          |array(struct(0, 1), struct(3, 4)),
          |array(struct(6, 7)),
          |array(),
          |null
          |as tbl(arr)
         """.stripMargin).createOrReplaceTempView("t1")
      checkAnswer(
        sql("select f1, f2 from t1 lateral view inline_outer(arr) as f1, f2"),
        Row(0, 1) :: Row(3, 4) :: Row(6, 7) :: Row(null, null) :: Row(null, null) :: Nil)
    }
  }

  def testNullStruct(): Unit = {
    val df = sql(
      """select * from values
        |(
        |  1,
        |  array(
        |    named_struct('c1', 0, 'c2', 1),
        |    null,
        |    named_struct('c1', 2, 'c2', 3),
        |    null
        |  )
        |)
        |as tbl(a, b)
         """.stripMargin)

    checkAnswer(
      df.select(inline('b)),
      Row(0, 1) :: Row(null, null) :: Row(2, 3) :: Row(null, null) :: Nil)

    checkAnswer(
      df.select('a, inline('b)),
      Row(1, 0, 1) :: Row(1, null, null) :: Row(1, 2, 3) :: Row(1, null, null) :: Nil)
  }

  test("SPARK-39061: inline should handle null struct") {
    testNullStruct
  }

  test("SPARK-39496: inline eval path should handle null struct") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      testNullStruct
    }
  }

  test("SPARK-40963: generator output has correct nullability") {
    // This test does not check nullability directly. Before SPARK-40963,
    // the below query got wrong results due to incorrect nullability.
    val df = sql(
      """select c1, explode(c4) as c5 from (
        |  select c1, array(c3) as c4 from (
        |    select c1, explode_outer(c2) as c3
        |    from values
        |    (1, array(1, 2)),
        |    (2, array(2, 3)),
        |    (3, null)
        |    as data(c1, c2)
        |  )
        |)
        |""".stripMargin)
    checkAnswer(df,
      Row(1, 1) :: Row(1, 2) :: Row(2, 2) :: Row(2, 3) :: Row(3, null) :: Nil)
  }
}

case class EmptyGenerator() extends Generator with LeafLike[Expression] {
  override def elementSchema: StructType = new StructType().add("id", IntegerType)
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = Seq.empty
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iteratorClass = classOf[Iterator[_]].getName
    ev.copy(code =
      code"$iteratorClass<InternalRow> ${ev.value} = $iteratorClass$$.MODULE$$.empty();")
  }
}
