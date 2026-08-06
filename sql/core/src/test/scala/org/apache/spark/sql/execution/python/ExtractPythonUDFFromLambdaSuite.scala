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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.ArrowEvalPython
import org.apache.spark.sql.functions.{array_sort, col, forall, lit, map_filter, map_zip_with,
  transform, transform_keys, transform_values, when, zip_with}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType}

/**
 * Plan-shape tests for [[ExtractPythonUDFFromLambda]].
 *
 * These assert the structure the rewrite produces - that no `PythonUDF` is left inside a
 * `LambdaFunction`, that the lifted UDF is an element-wise UDF over an array, and that plans
 * without a UDF in a lambda are untouched. End-to-end result correctness is covered by
 * `pyspark.sql.tests.test_udf_in_higher_order_function`, which needs a real Python worker.
 */
class ExtractPythonUDFFromLambdaSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val pythonUDF = new MyDummyPythonUDF
  private val scalarPandasUDF = new MyDummyScalarPandasUDF
  // Used where a UDF call must receive two arguments, e.g. a pairwise comparator.
  private val pythonUDF2 = new MyDummyPythonUDF

  private def arrayDF = Seq(Seq(1, 2, 3)).toDF("values")

  /** All `PythonUDF`s that remain inside a lambda in the optimized plan. */
  private def udfsInsideLambda(df: org.apache.spark.sql.DataFrame): Seq[PythonUDF] = {
    df.queryExecution.optimizedPlan.expressions.flatMap { e =>
      e.collect { case l: LambdaFunction => l }.flatMap { l =>
        l.collect { case u: PythonUDF => u }
      }
    }
  }

  private def liftedUDFs(df: org.apache.spark.sql.DataFrame): Seq[PythonUDF] = {
    df.queryExecution.optimizedPlan.collect {
      case a: ArrowEvalPython => a.udfs
    }.flatten
  }

  test("transform: the UDF is lifted out of the lambda as an element-wise array UDF") {
    val df = arrayDF.select(transform(col("values"), x => pythonUDF(x)).as("r"))

    // The whole point of the rewrite: nothing Python-shaped is left inside a lambda.
    assert(udfsInsideLambda(df).isEmpty)

    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    assert(lifted.head.evalType == PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF)
    // The lifted UDF takes the array and returns an array of the original return type.
    assert(lifted.head.dataType == ArrayType(pythonUDF.dataType, containsNull = true))
    assert(lifted.head.children.size == 1)
    assert(lifted.head.children.head.dataType.isInstanceOf[ArrayType])
  }

  test("filter/exists/forall: the UDF is lifted out of the lambda") {
    val exprs = Seq(
      org.apache.spark.sql.functions.filter(col("values"), x => pythonUDF(x)),
      org.apache.spark.sql.functions.exists(col("values"), x => pythonUDF(x)),
      org.apache.spark.sql.functions.forall(col("values"), x => pythonUDF(x)))

    exprs.foreach { e =>
      val df = arrayDF.select(e.as("r"))
      assert(udfsInsideLambda(df).isEmpty, s"UDF still inside a lambda for $e")
      assert(liftedUDFs(df).forall(_.evalType == PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF))
    }
  }

  test("aggregate: a UDF on the element is lifted, one on the accumulator is rejected") {
    val onElement = arrayDF.select(
      org.apache.spark.sql.functions.aggregate(
        col("values"), lit(false), (acc, x) => acc || pythonUDF(x)).as("r"))
    assert(udfsInsideLambda(onElement).isEmpty)

    // A UDF reading the accumulator has no array to precompute over, so analysis must fail.
    val e = intercept[AnalysisException] {
      arrayDF.select(
        org.apache.spark.sql.functions.aggregate(
          col("values"), lit(false), (acc, x) => pythonUDF(acc) || x.cast("boolean")).as("r"))
        .collect()
    }
    assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF")
  }

  test("several UDFs and nested UDFs in one lambda are all lifted") {
    val several = arrayDF.select(
      transform(col("values"), x => pythonUDF(x) || pythonUDF(x + lit(1))).as("r"))
    assert(udfsInsideLambda(several).isEmpty)
    // Two distinct calls, so two lifted array UDFs.
    assert(liftedUDFs(several).size == 2)

    // The same call twice must be evaluated once.
    val duplicated = arrayDF.select(
      transform(col("values"), x => pythonUDF(x) || pythonUDF(x)).as("r"))
    assert(udfsInsideLambda(duplicated).isEmpty)
    assert(liftedUDFs(duplicated).size == 1)
  }

  test("nested higher-order functions: the UDF is lifted onto the outer array") {
    // The inner array is the outer lambda's variable, so one pass cannot lift the UDF out. The
    // rewrite composes instead: the inner pass lifts it onto the inner array, and the outer pass
    // lifts that result onto the outer array, incrementing the flatten depth.
    val df = Seq(Seq(Seq(1, 2), Seq(3))).toDF("values")
      .select(transform(col("values"), inner =>
        transform(inner, x => pythonUDF(x))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)

    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    // Two levels of nesting, so the worker must flatten the argument twice.
    assert(lifted.head.elementwiseDepths == Seq(2))
  }

  test("a UDF on the outer element of a nested array is lifted") {
    // Here the UDF applies to the outer array's element, which is a real column, so the rewrite
    // applies even though the element happens to be an array.
    val df = Seq(Seq(Seq(1, 2), Seq(3))).toDF("values")
      .select(transform(col("values"), inner => pythonUDF(inner)).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
  }

  test("a lambda with no Python UDF is left unchanged") {
    val df = arrayDF.select(transform(col("values"), x => x + lit(1)).as("r"))
    val analyzed = df.queryExecution.analyzed
    val optimized = df.queryExecution.optimizedPlan
    // The rule must be inert: no eval-python node is introduced.
    assert(optimized.collect { case a: ArrowEvalPython => a }.isEmpty)
    assert(!optimized.toString.contains("pythonUDF"))
    assert(analyzed.expressions.flatMap(_.collect { case u: PythonUDF => u }).isEmpty)
  }

  test("every lambda-taking higher-order function is rewritten") {
    // One assertion per function, so that a shape regressing to "UDF left inside a lambda" is
    // caught here rather than only by the end-to-end Python suite.
    val arrays = Seq((Seq(1, 2), Seq(3, 4))).toDF("l", "r")
    val maps = Seq((Map("a" -> 1), Map("a" -> 2))).toDF("l", "r")

    val arrayCases = Seq(
      "transform" -> transform(col("l"), x => pythonUDF(x)),
      "transform with index" -> transform(col("l"), (x, i) => pythonUDF(x) || i > 0),
      "filter" -> org.apache.spark.sql.functions.filter(col("l"), x => pythonUDF(x)),
      "exists" -> org.apache.spark.sql.functions.exists(col("l"), x => pythonUDF(x)),
      "forall" -> forall(col("l"), x => pythonUDF(x)),
      "zip_with" -> zip_with(col("l"), col("r"), (a, b) => pythonUDF(a) || pythonUDF(b)),
      "array_sort" -> array_sort(col("l"),
        (a, b) => when(pythonUDF(a) === pythonUDF(b), lit(0)).otherwise(lit(1))),
      "aggregate merge" -> org.apache.spark.sql.functions.aggregate(
        col("l"), lit(false), (acc, x) => acc || pythonUDF(x)),
      "aggregate finish" -> org.apache.spark.sql.functions.aggregate(
        col("l"), lit(0), (acc, x) => acc + x, acc => pythonUDF(acc)))
    arrayCases.foreach { case (name, expr) =>
      val df = arrays.select(expr.as("r"))
      assert(udfsInsideLambda(df).isEmpty, s"UDF left inside a lambda for $name")
    }

    val mapCases = Seq(
      "transform_keys" -> transform_keys(col("l"), (k, v) => pythonUDF(k).cast("string")),
      "transform_values" -> transform_values(col("l"), (k, v) => pythonUDF(v)),
      "map_filter" -> map_filter(col("l"), (k, v) => pythonUDF(v)),
      "map_zip_with" -> map_zip_with(col("l"), col("r"), (k, a, b) => pythonUDF(a) || pythonUDF(b)))
    mapCases.foreach { case (name, expr) =>
      val df = maps.select(expr.as("r"))
      assert(udfsInsideLambda(df).isEmpty, s"UDF left inside a lambda for $name")
    }
  }

  test("a pairwise array_sort comparator is lifted over the cross product") {
    // One UDF call receiving both elements has no per-element key, so the UDF is precomputed over
    // every ordered pair instead. The call must end up outside every lambda like any other.
    val df = arrayDF.select(
      array_sort(col("values"), (a, b) => pythonUDF2(a, b).cast("int")).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    // Both sides of the pair are flattened once, since each is a flat array of all n*n pairs.
    assert(lifted.head.elementwiseDepths == Seq(1, 1))
  }

  test("a pandas UDF inside a lambda still fails analysis") {
    val e = intercept[AnalysisException] {
      arrayDF.select(transform(col("values"), x => scalarPandasUDF(x))).collect()
    }
    assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF")
  }

  test("the rewrite can be disabled by conf, restoring the previous error") {
    withSQLConf(SQLConf.PYTHON_UDF_IN_HIGHER_ORDER_FUNCTION_ENABLED.key -> "false") {
      val e = intercept[AnalysisException] {
        arrayDF.select(transform(col("values"), x => pythonUDF(x))).collect()
      }
      assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF")
    }
  }

  test("the rule is not excludable") {
    // A plan that only works because of this rewrite must not be broken by excludedRules.
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ExtractPythonUDFFromLambda.ruleName) {
      val df = arrayDF.select(transform(col("values"), x => pythonUDF(x)).as("r"))
      assert(udfsInsideLambda(df).isEmpty)
    }
  }

  test("a UDF over only constants is still lifted per element") {
    // SPARK-27052: `transform(arr, x -> udf(lit(10)))` does not read the element, so it is
    // already valid outside the lambda and is left to ExtractPythonUDFs.
    val df = arrayDF.select(transform(col("values"), _ => pythonUDF(lit(10))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
  }

  test("a UDF argument that is an expression over the element is lifted") {
    val df = arrayDF.select(transform(col("values"), x => pythonUDF(x * lit(2))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    // The argument became an array-valued expression over the whole array.
    assert(lifted.head.children.head.dataType.isInstanceOf[ArrayType])
  }

  test("an outer column argument is passed through to the lifted UDF") {
    val df = Seq((Seq(1, 2), 10)).toDF("values", "base")
      .select(transform(col("values"), x => pythonUDF(x, col("base"))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    // The element argument became an array; the outer column stays a scalar to be broadcast.
    assert(lifted.head.children.head.dataType.isInstanceOf[ArrayType])
    assert(lifted.head.children.last.dataType == IntegerType)
  }
}
