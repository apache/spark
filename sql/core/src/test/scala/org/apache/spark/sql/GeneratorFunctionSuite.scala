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
    val m = intercept[AnalysisException] {
      df.selectExpr("stack(1.1, 1, 2, 3)")
    }.getMessage
    assert(m.contains("The number of rows must be a positive constant integer."))
    val m2 = intercept[AnalysisException] {
      df.selectExpr("stack(-1, 1, 2, 3)")
    }.getMessage
    assert(m2.contains("The number of rows must be a positive constant integer."))

    // The data for the same column should have the same type.
    val m3 = intercept[AnalysisException] {
      df.selectExpr("stack(2, 1, '2.2')")
    }.getMessage
    assert(m3.contains("data type mismatch: Argument 1 (int) != Argument 2 (string)"))

    // stack on column data
    val df2 = Seq((2, 1, 2, 3)).toDF("n", "a", "b", "c")
    checkAnswer(df2.selectExpr("stack(2, a, b, c)"), Row(1, 2) :: Row(3, null) :: Nil)

    val m4 = intercept[AnalysisException] {
      df2.selectExpr("stack(n, a, b, c)")
    }.getMessage
    assert(m4.contains("The number of rows must be a positive constant integer."))

    val df3 = Seq((2, 1, 2.0)).toDF("n", "a", "b")
    val m5 = intercept[AnalysisException] {
      df3.selectExpr("stack(2, a, b)")
    }.getMessage
    assert(m5.contains("data type mismatch: Argument 1 (int) != Argument 2 (double)"))

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
    val m = intercept[AnalysisException] {
      spark.range(2).selectExpr("inline(array())")
    }.getMessage
    assert(m.contains("data type mismatch"))
  }

  test("inline with empty table") {
    checkAnswer(
      spark.range(0).selectExpr("inline(array(struct(10, 100)))"),
      Nil)
  }

  test("inline on literal") {
    checkAnswer(
      spark.range(2).selectExpr("inline(array(struct(10, 100), struct(20, 200), struct(30, 300)))"),
      Row(10, 100) :: Row(20, 200) :: Row(30, 300) ::
        Row(10, 100) :: Row(20, 200) :: Row(30, 300) :: Nil)
  }

  test("inline on column") {
    val df = Seq((1, 2)).toDF("a", "b")

    checkAnswer(
      df.selectExpr("inline(array(struct(a), struct(a)))"),
      Row(1) :: Row(1) :: Nil)

    checkAnswer(
      df.selectExpr("inline(array(struct(a, b), struct(a, b)))"),
      Row(1, 2) :: Row(1, 2) :: Nil)

    // Spark think [struct<a:int>, struct<b:int>] is heterogeneous due to name difference.
    val m = intercept[AnalysisException] {
      df.selectExpr("inline(array(struct(a), struct(b)))")
    }.getMessage
    assert(m.contains("data type mismatch"))

    checkAnswer(
      df.selectExpr("inline(array(struct(a), named_struct('a', b)))"),
      Row(1) :: Row(2) :: Nil)

    // Spark think [struct<a:int>, struct<col1:int>] is heterogeneous due to name difference.
    val m2 = intercept[AnalysisException] {
      df.selectExpr("inline(array(struct(a), struct(2)))")
    }.getMessage
    assert(m2.contains("data type mismatch"))

    checkAnswer(
      df.selectExpr("inline(array(struct(a), named_struct('a', 2)))"),
      Row(1) :: Row(2) :: Nil)

    checkAnswer(
      df.selectExpr("struct(a)").selectExpr("inline(array(*))"),
      Row(1) :: Nil)

    checkAnswer(
      df.selectExpr("array(struct(a), named_struct('a', b))").selectExpr("inline(*)"),
      Row(1) :: Row(2) :: Nil)
  }

  test("inline_outer") {
    val df = Seq((1, "2"), (3, "4"), (5, "6")).toDF("col1", "col2")
    val df2 = df.select(
      when($"col1" === 1, null).otherwise(array(struct($"col1", $"col2"))).as("col1"))
    checkAnswer(
      df2.selectExpr("inline(col1)"),
      Row(3, "4") :: Row(5, "6") :: Nil
    )
    checkAnswer(
      df2.selectExpr("inline_outer(col1)"),
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

      val msg1 = intercept[AnalysisException] {
        sql("select 1 + explode(array(min(c2), max(c2))) from t1 group by c1")
      }.getMessage
      assert(msg1.contains("Generators are not supported when it's nested in expressions"))

      val msg2 = intercept[AnalysisException] {
        sql(
          """select
            |  explode(array(min(c2), max(c2))),
            |  posexplode(array(min(c2), max(c2)))
            |from t1 group by c1
          """.stripMargin)
      }.getMessage
      assert(msg2.contains("Only one generator allowed per aggregate clause"))
    }
  }

  test("SPARK-30998: Unsupported nested inner generators") {
    val errMsg = intercept[AnalysisException] {
      sql("SELECT array(array(1, 2), array(3)) v").select(explode(explode($"v"))).collect
    }.getMessage
    assert(errMsg.contains("Generators are not supported when it's nested in expressions, " +
      "but got: explode(explode(v))"))
  }

  test("SPARK-30997: generators in aggregate expressions for dataframe") {
    val df = Seq(1, 2, 3).toDF("v")
    checkAnswer(df.select(explode(array(min($"v"), max($"v")))), Row(1) :: Row(3) :: Nil)
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
