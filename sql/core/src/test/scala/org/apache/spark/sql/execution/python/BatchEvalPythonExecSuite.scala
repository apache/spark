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

package org.apache.spark.sql.execution.python

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GreaterThan, In}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.BooleanType

class BatchEvalPythonExecSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

  val pythonUDF = new MyDummyPythonUDF
  val pandasUDF = new MyDummyScalarPandasUDF

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.udf.registerPython("dummyPythonUDF", pythonUDF)
    spark.udf.registerPython("dummyScalarPandasUDF", pandasUDF)
  }

  override def afterAll(): Unit = {
    spark.sessionState.functionRegistry.dropFunction(FunctionIdentifier("dummyPythonUDF"))
    spark.sessionState.functionRegistry.dropFunction(FunctionIdentifier("dummyScalarPandasUDF"))
    super.afterAll()
  }

  test("Python UDF: push down deterministic FilterExec predicates") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(b) and dummyPythonUDF(a) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: AttributeReference),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(FilterExec(_: In, _))) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Nested Python UDF: push down deterministic FilterExec predicates") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(a, dummyPythonUDF(a, b)) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(_: AttributeReference, InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(FilterExec(_: In, _))) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF: no push down on non-deterministic") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("b > 4 and dummyPythonUDF(a) and rand() > 0.3")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: GreaterThan),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(_: FilterExec)) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF: push down on deterministic predicates after the first non-deterministic") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(a) and rand() > 0.3 and b > 4")

    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: GreaterThan),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(_: FilterExec)) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF refers to the attributes from more than one child") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = Seq(("Hello", 4)).toDF("c", "d")
    val joinDF = df.crossJoin(df2).where("dummyPythonUDF(a, c) == dummyPythonUDF(d, c)")
    val qualifiedPlanNodes = joinDF.queryExecution.executedPlan.collect {
      case b: BatchEvalPythonExec => b
    }
    assert(qualifiedPlanNodes.size == 1)
  }

  private def collectPythonExec(spark: SparkPlan): Seq[BatchEvalPythonExec] = spark.collect {
    case b: BatchEvalPythonExec => b
  }

  private def collectPandasExec(spark: SparkPlan): Seq[ArrowEvalPythonExec] = spark.collect {
    case b: ArrowEvalPythonExec => b
  }

  test("Chained Python UDFs should be combined to a single physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", pythonUDF(col("a"))).withColumn("d", pythonUDF(col("c")))
    val pythonEvalNodes = collectPythonExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
  }

  test("Chained Pandas UDFs should be combined to a single physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", pandasUDF(col("a"))).withColumn("d", pandasUDF(col("c")))
    val arrowEvalNodes = collectPandasExec(df2.queryExecution.executedPlan)
    assert(arrowEvalNodes.size == 1)
  }

  test("Mixed Python UDFs and Pandas UDF should be separate physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", pythonUDF(col("a"))).withColumn("d", pandasUDF(col("b")))

    val pythonEvalNodes = collectPythonExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectPandasExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(arrowEvalNodes.size == 1)
  }

  test("Independent Python UDFs and Pandas UDFs should be combined separately") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c1", pythonUDF(col("a")))
      .withColumn("c2", pythonUDF(col("c1")))
      .withColumn("d1", pandasUDF(col("a")))
      .withColumn("d2", pandasUDF(col("d1")))

    val pythonEvalNodes = collectPythonExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectPandasExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(arrowEvalNodes.size == 1)
  }

  test("Dependent Python UDFs and Pandas UDFs should not be combined") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c1", pythonUDF(col("a")))
      .withColumn("d1", pandasUDF(col("c1")))
      .withColumn("c2", pythonUDF(col("d1")))
      .withColumn("d2", pandasUDF(col("c2")))

    val pythonEvalNodes = collectPythonExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectPandasExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 2)
    assert(arrowEvalNodes.size == 2)
  }
}

// This Python UDF is dummy and just for testing. Unable to execute.
class DummyUDF extends PythonFunction(
  command = Array[Byte](),
  envVars = Map("" -> "").asJava,
  pythonIncludes = ArrayBuffer("").asJava,
  pythonExec = "",
  pythonVer = "",
  broadcastVars = null,
  accumulator = null)

class MyDummyPythonUDF extends UserDefinedPythonFunction(
  name = "dummyUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_BATCHED_UDF,
  udfDeterministic = true)

class MyDummyScalarPandasUDF extends UserDefinedPythonFunction(
  name = "dummyPandasUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
  udfDeterministic = true)
