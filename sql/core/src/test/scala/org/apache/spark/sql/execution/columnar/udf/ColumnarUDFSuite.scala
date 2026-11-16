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

package org.apache.spark.sql.execution.columnar.udf

import java.util.Locale

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.columnar.{ColumnarScalarFunction, ColumnarUDFImplementation, ColumnarUserDefinedFunction}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/** Adds one to a bigint, and keeps nulls. */
class AddOneFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    if (in.hasNull) {
      while (i < n) {
        if (in.isNullAt(i)) out.putNull(i) else out.putLong(i, in.getLong(i) + 1)
        i += 1
      }
    } else {
      while (i < n) {
        out.putLong(i, in.getLong(i) + 1)
        i += 1
      }
    }
    out
  }
}

/** Adds two bigints, and returns null if either is null. */
class AddFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val a = args.column(0)
    val b = args.column(1)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    while (i < n) {
      if (a.isNullAt(i) || b.isNullAt(i)) {
        out.putNull(i)
      } else {
        out.putLong(i, a.getLong(i) + b.getLong(i))
      }
      i += 1
    }
    out
  }
}

/** The upper case of a string, followed by an exclamation mark. */
class ShoutFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, StringType)
    var i = 0
    while (i < n) {
      if (in.isNullAt(i)) {
        out.putNull(i)
      } else {
        val upper = in.getUTF8String(i).toString.toUpperCase(Locale.ROOT)
        val bytes = UTF8String.fromString(upper + "!").getBytes
        out.putByteArray(i, bytes)
      }
      i += 1
    }
    out
  }
}

class ColumnarUDFSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private val addOne =
    ColumnarUserDefinedFunction("add_one", Seq(LongType), LongType, Some(new AddOneFunction))
  private val add =
    ColumnarUserDefinedFunction("add", Seq(LongType, LongType), LongType, Some(new AddFunction))
  private val shout =
    ColumnarUserDefinedFunction("shout", Seq(StringType), StringType, Some(new ShoutFunction))

  private def evalNodes(plan: SparkPlan): Seq[EvalColumnarUDFsExec] = collect(plan) {
    case e: EvalColumnarUDFsExec => e
  }

  test("a call over a row-based child") {
    val df = spark.range(5).select(col("id"), addOne(col("id")))
    checkAnswer(df, (0 until 5).map(i => Row(i.toLong, i + 1L)))
    assert(evalNodes(df.queryExecution.executedPlan).length == 1)
  }

  test("a call from SQL, with an argument that is cast") {
    spark.udf.registerColumnar(addOne)
    checkAnswer(sql("SELECT add_one(1), add_one(CAST(NULL AS INT))"), Row(2L, null))
    checkAnswer(
      sql("SELECT sum(add_one(id)) FROM range(1000)"), Row((1 to 1000).map(_.toLong).sum))
  }

  test("a call over a columnar scan has no transition from rows") {
    withTempPath { path =>
      spark.range(100).selectExpr("id", "cast(id as string) as s")
        .write.parquet(path.getCanonicalPath)
      val df = spark.read.parquet(path.getCanonicalPath).select(col("s"), addOne(col("id")))
      checkAnswer(df, (0 until 100).map(i => Row(i.toString, i + 1L)))
      val plan = df.queryExecution.executedPlan
      val eval = evalNodes(plan)
      assert(eval.length == 1)
      assert(collect(plan) { case r: RowToColumnarExec => r }.isEmpty)
    }
  }

  test("sibling calls share a node, and nested calls do not") {
    val siblings = spark.range(5).select(addOne(col("id")), add(col("id"), col("id")))
    checkAnswer(siblings, (0 until 5).map(i => Row(i + 1L, 2L * i)))
    val siblingNodes = evalNodes(siblings.queryExecution.executedPlan)
    assert(siblingNodes.length == 1 && siblingNodes.head.udfs.length == 2)

    val nested = spark.range(5).select(addOne(addOne(col("id"))))
    checkAnswer(nested, (0 until 5).map(i => Row(i + 2L)))
    assert(evalNodes(nested.queryExecution.executedPlan).length == 2)
  }

  test("arguments that are not columns") {
    val df = spark.range(5).select(add(col("id") * 2, lit(10L)), addOne(col("id") * 2))
    checkAnswer(df, (0 until 5).map(i => Row(2L * i + 10, 2L * i + 1)))
  }

  test("a call in a filter, and a filter below a call") {
    val df = spark.range(10).filter(addOne(col("id")) > 5).select(col("id"))
    checkAnswer(df, (5 until 10).map(i => Row(i.toLong)))
    val pushed = spark.range(10).select(col("id"), addOne(col("id")).as("r")).filter("id < 3")
    checkAnswer(pushed, (0 until 3).map(i => Row(i.toLong, i + 1L)))
  }

  test("strings and nulls") {
    val df = Seq(Some("a"), None, Some("bc")).toDF("s").select(shout(col("s")))
    checkAnswer(df, Seq(Row("A!"), Row(null), Row("BC!")))
  }

  test("a function that Spark cannot run fails when it is executed") {
    val engineOnly = ColumnarUserDefinedFunction(
      "engine_only", Seq(LongType), LongType,
      engineImplementations = Seq(
        ColumnarUDFImplementation("some-engine", Map("library" -> "/lib.so", "name" -> "f"))))
    val df = spark.range(5).select(engineOnly(col("id")))
    // It is planned, with its implementations on the node, for an engine to replace.
    val node = evalNodes(df.queryExecution.executedPlan).head
    assert(node.udfs.head.engineImplementations.map(_.kind) == Seq("some-engine"))
    checkError(
      exception = intercept[SparkUnsupportedOperationException](df.collect()),
      condition = "COLUMNAR_UDF_NOT_EXECUTABLE",
      parameters = Map("name" -> "engine_only", "kinds" -> "[some-engine]"))
  }

  test("a function that returns a vector of another type fails") {
    val wrongType = ColumnarUserDefinedFunction(
      "wrong_type", Seq(StringType), LongType, Some(new ShoutFunction))
    val df = Seq("a").toDF("s").select(wrongType(col("s")))
    checkError(
      exception = intercept[SparkRuntimeException](df.collect()),
      condition = "COLUMNAR_UDF_RESULT_TYPE_MISMATCH",
      parameters = Map(
        "name" -> "wrong_type", "actualType" -> "\"STRING\"", "expectedType" -> "\"BIGINT\""))
  }
}
