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
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, Literal, NamedArgumentExpression,
  PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.ArrowEvalPython
import org.apache.spark.sql.functions.{array_sort, col, forall, lit, map_filter, map_zip_with,
  transform, transform_keys, transform_values, when, zip_with}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePointUDT, SharedSparkSession}
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
  private val scalarArrowUDF = new MyDummyScalarArrowUDF
  private val scalarPandasIterUDF = new MyDummyScalarPandasIterUDF
  private val scalarArrowIterUDF = new MyDummyScalarArrowIterUDF
  // Used where a UDF call must receive two arguments, e.g. a pairwise comparator.
  private val pythonUDF2 = new MyDummyPythonUDF
  private val nondeterministicUDF = new MyDummyNondeterministicPythonUDF
  private val udtUDF = new MyDummyUDTPythonUDF

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
      "merge" ->
        org.apache.spark.sql.functions.aggregate(
          col("values"), lit(false), (acc, x) => acc || pythonUDF(x)),
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

  test("a UDF nested inside a composite argument is lifted, not left inside a lambda") {
    // SPARK-27052: `f(g(x) + 1)` / `f(-g(x))`. The inner call `g(x)` is already lifted; its
    // occurrence buried inside the composite argument must be substituted too, or a raw `g` over a
    // lambda variable would be left inside the generated transform for `ExtractPythonUDFs` to
    // re-extract (the SPARK-48706 failure mode). Both nesting shapes must leave no UDF in a lambda.
    val plusOne = arrayDF.select(
      transform(col("values"), x => pythonUDF(pythonUDF(x).cast("int") + lit(1))).as("r"))
    assert(udfsInsideLambda(plusOne).isEmpty)
    // Two distinct calls (inner `g(x)`, outer `f(g(x) + 1)`), so two lifted array UDFs.
    assert(liftedUDFs(plusOne).size == 2)

    val negated = arrayDF.select(
      transform(col("values"), x => pythonUDF(-pythonUDF(x).cast("int"))).as("r"))
    assert(udfsInsideLambda(negated).isEmpty)
    assert(liftedUDFs(negated).size == 2)
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

  test("a UDF inside a nested higher-order function's lambda is lifted, deepening the nesting") {
    // `transform(matrix, row -> transform(row, x -> f(x)))`: the UDF is lifted onto the inner
    // variable and then re-lifted onto the real `array<array<int>>` column, so it becomes a
    // depth-2 element-wise UDF (flattening two array levels). Nothing is left inside a lambda.
    val df = Seq(Seq(Seq(1, 2), Seq(3))).toDF("values")
      .select(transform(col("values"), inner =>
        transform(inner, x => pythonUDF(x))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    assert(lifted.head.evalType == PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF)
    // Re-lifted once per enclosing lambda: depth 2, over an array<array<...>> argument.
    assert(lifted.head.elementwiseNestingDepth == 2)
    assert(lifted.head.dataType ==
      ArrayType(ArrayType(pythonUDF.dataType, containsNull = true), containsNull = true))
  }

  test("a UDF in a nested lambda that captures an enclosing lambda variable is lifted") {
    // `transform(m, row -> transform(row, x -> f(x, size(row))))`: the UDF reads the inner element
    // and the *enclosing* variable `row`. The captured value is repeated into an aligned array, so
    // the UDF still lifts to a depth-2 element-wise UDF over the real column and nothing is left in
    // a lambda. Relies on the fixed `HigherOrderFunction.canonicalized`, which no longer leaks the
    // captured variable into the lifted UDF's references.
    val df = Seq(Seq(Seq(1, 2), Seq(3))).toDF("values")
      .select(transform(col("values"), row =>
        transform(row, x => pythonUDF2(x, org.apache.spark.sql.functions.size(row)))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    val lifted = liftedUDFs(df)
    assert(lifted.size == 1)
    assert(lifted.head.elementwiseNestingDepth == 2)
  }

  test("a UDF on the outer element of a nested array is lifted") {
    // Here the UDF applies to the outer array's element, which is a real column (the element just
    // happens to be an array), so it is lifted onto that column like any other single-level array.
    val df = Seq(Seq(Seq(1, 2), Seq(3))).toDF("values")
      .select(transform(col("values"), inner => pythonUDF(inner)).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
  }

  test("a rewritable higher-order function inside another lambda leaves no UDF in any lambda") {
    // `transform(arr2, i -> array_max(transform(arr, x -> f(x))) + i)`: the inner `transform`
    // iterates the real column `arr` (not the outer lambda variable `i`), so it is rewritable and
    // its UDF is lifted out of the inner lambda. The lifted element-wise UDF reads only `arr`, so
    // `ExtractPythonUDFs` then hoists it out of the outer lambda too (evaluated once per row). The
    // end state must leave no `PythonUDF` inside any lambda.
    val df = Seq((Seq(1, 2, 3), Seq(10, 20))).toDF("arr", "arr2")
      .select(transform(col("arr2"), i =>
        org.apache.spark.sql.functions.array_max(
          transform(col("arr"), x => pythonUDF(x).cast("int"))) + i).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    assert(liftedUDFs(df).size == 1)
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

  test("a key-form array_sort comparator lifts one UDF per element, not per comparator side") {
    // `(a, b) -> udf(a) < udf(b)`: `udf(a)` and `udf(b)` canonicalize differently (distinct
    // variable exprIds) but both lift to the same function over the whole array, so they must be
    // deduplicated into one lifted UDF - otherwise the Python function runs 2n times instead of n.
    val df = arrayDF.select(
      array_sort(col("values"),
        (a, b) => when(pythonUDF(a) === pythonUDF(b), lit(0)).otherwise(lit(1))).as("r"))
    assert(udfsInsideLambda(df).isEmpty)
    assert(liftedUDFs(df).size == 1)
  }

  test("a vectorized scalar UDF inside a lambda is lifted to its element-wise eval type") {
    // Each vectorized scalar flavor lifts to its own element-wise eval type so the worker keeps
    // that flavor's batching contract (pandas Series, Arrow Array, or an iterator of batches).
    val cases = Seq(
      scalarPandasUDF -> PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
      scalarArrowUDF -> PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
      scalarPandasIterUDF -> PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
      scalarArrowIterUDF -> PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF)
    cases.foreach { case (udf, expectedEvalType) =>
      val df = arrayDF.select(transform(col("values"), x => udf(x)).as("r"))
      val evalTypeName = PythonEvalType.toString(expectedEvalType)
      assert(udfsInsideLambda(df).isEmpty, s"UDF still inside a lambda for $evalTypeName")
      val lifted = liftedUDFs(df)
      assert(lifted.size == 1)
      assert(lifted.head.evalType == expectedEvalType)
      assert(lifted.head.dataType == ArrayType(udf.dataType, containsNull = true))
      assert(lifted.head.children.head.dataType.isInstanceOf[ArrayType])
    }
  }

  test("a UDF with a UDT type inside a lambda still fails analysis") {
    // Lifting forces the Arrow element-wise eval type, which has no UDT fallback, so a UDF whose
    // argument or return type involves a UDT is not rewritable and keeps the previous analysis
    // error rather than failing at runtime.
    val e = intercept[AnalysisException] {
      arrayDF.select(transform(col("values"), x => udtUDF(x))).collect()
    }
    assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF")
  }

  test("the rewritable predicate: eval types, zero-argument / iterator-kwarg / UDT UDFs") {
    // A zero-arg call has no array to carry the iterated shape and would crash the worker; an
    // iterator UDF takes no kwargs; a UDT would hit the Arrow path with no fallback - so the shared
    // predicate rejects those, keeping the previous analysis error. A named argument on a
    // non-iterator flavor is accepted (the lift keeps the NamedArgumentExpression).
    val plain = PythonUDF("f", null, IntegerType, Seq(Literal(1)),
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
    assert(PythonUDF.isElementwiseRewritableUDF(plain))

    // Every row-at-a-time and vectorized scalar eval type is rewritable; each maps to its own
    // element-wise lifted eval type.
    Seq(
      PythonEvalType.SQL_ARROW_BATCHED_UDF -> PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_UDF -> PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF ->
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_UDF -> PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF ->
        PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF).foreach {
      case (base, lifted) =>
        assert(PythonUDF.isElementwiseRewritableUDF(plain.copy(evalType = base)))
        assert(PythonUDF.liftedElementwiseEvalType(base) == lifted)
    }
    assert(PythonUDF.liftedElementwiseEvalType(PythonEvalType.SQL_BATCHED_UDF) ==
      PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF)

    // An already-lifted element-wise UDF is rewritable again (nested lambdas re-lift it), and its
    // lifted eval type is itself - only the nesting depth changes.
    Seq(
      PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF).foreach { ew =>
      assert(PythonUDF.isElementwiseRewritableUDF(plain.copy(evalType = ew)))
      assert(PythonUDF.liftedElementwiseEvalType(ew) == ew)
    }

    val zeroArg = plain.copy(children = Seq.empty)
    assert(!PythonUDF.isElementwiseRewritableUDF(zeroArg))

    // A named argument is rewritable on the non-iterator flavors (the lift keeps the
    // NamedArgumentExpression as a direct child), but not on an iterator UDF (no kwargs there).
    val named = plain.copy(children = Seq(NamedArgumentExpression("k", Literal(1))))
    assert(PythonUDF.isElementwiseRewritableUDF(named))
    assert(PythonUDF.isElementwiseRewritableUDF(
      named.copy(evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF)))
    assert(!PythonUDF.isElementwiseRewritableUDF(
      named.copy(evalType = PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)))
    assert(!PythonUDF.isElementwiseRewritableUDF(
      named.copy(evalType = PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF)))

    val udtReturn = plain.copy(dataType = new ExamplePointUDT)
    assert(!PythonUDF.isElementwiseRewritableUDF(udtReturn))

    val udtArg = plain.copy(children = Seq(Literal.create(null, new ExamplePointUDT)))
    assert(!PythonUDF.isElementwiseRewritableUDF(udtArg))
  }

  test("a zero-argument UDF inside a lambda still fails analysis") {
    // `transform(arr, x -> f())` has no argument to carry the iterated shape, so it is not
    // rewritable and must keep failing analysis rather than crash the Python worker at runtime.
    val e = intercept[AnalysisException] {
      arrayDF.select(transform(col("values"), _ => pythonUDF())).collect()
    }
    assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF")
  }

  test("a nondeterministic iterated argument is rejected") {
    // The rewrite references the iterated argument several times and nondeterministic expressions
    // are not subexpression-eliminated, so a nondeterministic argument like `shuffle(arr)` would be
    // evaluated independently per reference and misalign the results. It must fail analysis.
    val e = intercept[AnalysisException] {
      arrayDF.select(
        org.apache.spark.sql.functions.filter(
          org.apache.spark.sql.functions.shuffle(col("values")),
          x => pythonUDF(x))).collect()
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
    // SPARK-27052: `transform(arr, x -> udf(lit(10)))` does not read the element, but it is still
    // lifted per element (`overArray` repeats the constant into an aligned array) so it keeps the
    // lambda's call domain - once per element, zero times for a null/empty array - rather than
    // being left to ExtractPythonUDFs, which would call it once per row.
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
