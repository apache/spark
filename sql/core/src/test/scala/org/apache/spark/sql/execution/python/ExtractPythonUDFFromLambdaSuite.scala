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
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType}

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
  private val nondeterministicUDF = new MyDummyNondeterministicPythonUDF

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

  test("aggregate: a UDF anywhere in the fold is rejected") {
    // The fold is sequential, so no array can precompute the values the UDF sees. A UDF in `merge`
    // or in `finish` therefore has no rewrite and analysis must fail.
    val cases = Seq(
      "merge on element" ->
        org.apache.spark.sql.functions.aggregate(
          col("values"), lit(false), (acc, x) => acc || pythonUDF(x)),
      "merge on accumulator" ->
        org.apache.spark.sql.functions.aggregate(
          col("values"), lit(false), (acc, x) => pythonUDF(acc) || x.cast("boolean")),
      "finish" ->
        org.apache.spark.sql.functions.aggregate(
          col("values"), lit(0), (acc, x) => acc + x, acc => pythonUDF(acc)))
    cases.foreach { case (name, expr) =>
      val e = intercept[AnalysisException] {
        arrayDF.select(expr.as("r")).collect()
      }
      assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF",
        s"expected aggregate with a UDF in $name to be rejected")
    }
  }

  test("several UDFs and nested UDFs in one lambda are all lifted") {
    val several = arrayDF.select(
      transform(col("values"), x => pythonUDF(x) || pythonUDF(x + lit(1))).as("r"))
    assert(udfsInsideLambda(several).isEmpty)
    // Two distinct calls, so two lifted array UDFs.
    assert(liftedUDFs(several).size == 2)

    // The same deterministic call twice must be evaluated once.
    val duplicated = arrayDF.select(
      transform(col("values"), x => pythonUDF(x) || pythonUDF(x)).as("r"))
    assert(udfsInsideLambda(duplicated).isEmpty)
    assert(liftedUDFs(duplicated).size == 1)
  }

  test("identical nondeterministic calls are lifted distinctly, not deduplicated") {
    // Deduplicating `f(x)` with `f(x)` would collapse two independent draws into one; a
    // nondeterministic UDF must keep each call, so both are lifted.
    val df = arrayDF.select(
      transform(col("values"), x => nondeterministicUDF(x) || nondeterministicUDF(x)).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    assert(liftedUDFs(df).size == 2)
  }

  test("transform_values whose result type equals the key type replaces values, not keys") {
    // SPARK-27052: dispatch is by concrete function, not result type. For map<string, string>,
    // transform_values' lambda also returns string, which must not be treated as new keys.
    val maps = Seq(Map("a" -> "x")).toDF("m")
    val df = maps.select(transform_values(col("m"), (k, v) => pythonUDF(v).cast("string")).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    // Keys are preserved: the map still has the original key type and no new-key projection.
    val mapType = df.queryExecution.analyzed.schema.head.dataType.asInstanceOf[MapType]
    assert(mapType.keyType == StringType)
  }

  test("a UDF inside a nested higher-order function's lambda is rejected") {
    // The inner array is the outer lambda's variable, not a real column, so the UDF cannot be
    // lifted onto an array outside the lambda. This must fail analysis rather than be rewritten.
    val df = Seq(Seq(Seq(1, 2), Seq(3))).toDF("values")
    val e = intercept[AnalysisException] {
      df.select(transform(col("values"), inner =>
        transform(inner, x => pythonUDF(x))).as("r")).collect()
    }
    assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF")
  }

  test("a UDF on the outer element of a nested array is lifted") {
    // Here the UDF applies to the outer array's element, which is a real column (the element just
    // happens to be an array), so it is lifted onto that column like any other single-level array.
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

  test("every mapping higher-order function is rewritten") {
    // One assertion per function, so that a shape regressing to "UDF left inside a lambda" is
    // caught here rather than only by the end-to-end Python suite. `aggregate` / `reduce` are not
    // here: they fold rather than map, so a UDF in them is rejected (see the aggregate test above).
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
        (a, b) => when(pythonUDF(a) === pythonUDF(b), lit(0)).otherwise(lit(1))))
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
    // The UDF takes both pair sides, each a flat array of all n*n pairs.
    assert(lifted.head.children.size == 2)
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

  test("the rewrite cannot be disabled via excludedRules") {
    // The lambda rewrite is driven by `ExtractPythonUDFs` (not a standalone batch rule), and that
    // rule is non-excludable, so a plan that only works because of the rewrite cannot be broken by
    // excludedRules. Excluding either name must leave the UDF lifted out of the lambda.
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        Seq(ExtractPythonUDFFromLambda.ruleName, ExtractPythonUDFs.ruleName).mkString(",")) {
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

  test("an outer column argument is repeated into an aligned array for the lifted UDF") {
    val df = Seq((Seq(1, 2), 10)).toDF("values", "base")
      .select(transform(col("values"), x => pythonUDF(x, col("base"))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    // Both arguments are single-level arrays aligned with the iterated array: the element argument
    // is the array itself, and the outer column is repeated into an aligned array so the worker
    // flattens every argument uniformly.
    lifted.head.children.foreach { c =>
      assert(c.dataType.isInstanceOf[ArrayType])
      assert(c.dataType.asInstanceOf[ArrayType].elementType == IntegerType)
    }
  }
}
