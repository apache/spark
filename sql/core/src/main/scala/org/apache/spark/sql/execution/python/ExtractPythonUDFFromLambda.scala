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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types.{ArrayType, IntegerType}

/**
 * Rewrites scalar Python UDFs that appear inside the lambda of a higher-order function so that
 * they can be evaluated at all.
 *
 * A `PythonUDF` is evaluated by a separate physical operator (`ArrowEvalPython`), which
 * [[ExtractPythonUDFs]] pulls out of the enclosing operator. A lambda's [[NamedLambdaVariable]]s
 * only exist while the higher-order function is iterating, so an extracted operator cannot see
 * them: the UDF can neither stay inside the lambda nor be lifted out by the existing extraction
 * rule. Historically `CheckAnalysis` therefore rejected this outright.
 *
 * The way out is to not evaluate the UDF per element inside the lambda at all. Instead the UDF is
 * applied once to the *whole array*, outside every lambda, and the lambda reads the precomputed
 * result positionally:
 *
 * {{{
 *   -- before (rejected)
 *   transform(values, x -> plus_one(x))
 *
 *   -- after (legal: the PythonUDF is outside every LambdaFunction)
 *   transform(
 *     arrays_zip(values AS c0, plus_one_over_array(values) AS u0),
 *     s -> s.u0)
 * }}}
 *
 * `plus_one_over_array` is the same user function, re-typed as `array<T> => array<R>` and run with
 * [[PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF]]. The JVM cannot rewrap a pickled Python function,
 * so the array-at-a-time behaviour lives in the Python worker: it flattens the incoming list
 * column, calls the user function once over the concatenated elements of the whole batch, and
 * re-nests the results with the input's offsets. This keeps one row in and one row out (no
 * `explode`, no shuffle), and crosses the Python boundary once per batch rather than once per row.
 *
 * Once the UDF result is an ordinary column, everything the lambda does around it is ordinary JVM
 * work, so arithmetic, `when`, casts, the element index, several UDFs in one lambda and nested
 * UDF calls all follow without special cases.
 *
 * This rule runs before [[ExtractPythonUDFs]], which then extracts the lifted UDF as a normal
 * top-level `PythonUDF` and needs no new physical operator.
 *
 * Shapes that cannot be rewritten this way are left untouched here and continue to be rejected by
 * `CheckAnalysis`:
 *
 *  - a UDF reading `aggregate`'s accumulator, which is sequential (step n depends on step n-1);
 *  - a UDF in `aggregate`'s `finish`, which runs once on the final accumulator: lifting it out
 *    would call it on the null that a fold over a null array produces, where native Spark does
 *    not evaluate `finish` at all;
 *  - a genuinely pairwise `array_sort` comparator, for which no per-element key exists;
 *  - a pandas UDF, which receives a `Series` rather than one value per call;
 *  - a nested higher-order function, e.g. `transform(arr, i -> transform(i, x -> udf(x)))`: the
 *    inner array only exists while the outer function iterates, so a UDF lifted onto it would
 *    still sit inside the outer lambda;
 *  - the map family (`transform_keys`, `transform_values`, `map_filter`, `map_zip_with`) and
 *    `zip_with`, which are not handled yet.
 */
object ExtractPythonUDFFromLambda extends Rule[LogicalPlan] {

  /**
   * A scalar Python UDF that this rule knows how to lift out of a lambda. Shared with
   * `CheckAnalysis` so that the shapes analysis lets through are exactly those rewritten here.
   */
  private def isRewritableUDF(e: Expression): Boolean =
    PythonUDF.isElementwiseRewritableUDF(e)

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.pythonUDFInHigherOrderFunctionEnabled) {
      plan
    } else {
      // Bottom-up so that the innermost higher-order function is considered first. Each rewrite
      // is local to one function, and nested ones are rejected during analysis.
      plan.transformUpWithPruning(
        _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION)) {
        case p =>
          p.transformExpressionsUpWithPruning(
            _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION))(rewrite)
      }
    }
  }

  /** Rewrites a single higher-order function whose lambda contains a rewritable Python UDF. */
  private val rewrite: PartialFunction[Expression, Expression] = {
    // `transform`, `exists` and `forall` all return the lambda's own value, so the carrier
    // struct can simply replace the argument and the rewritten body reads the UDF result.
    case ArrayTransform(argument, function) if hasLiftableUDF(argument, function) =>
      rewriteResultFromLambda(argument, function)(ArrayTransform(_, _))

    case ArrayExists(argument, function, followThreeValuedLogic)
        if hasLiftableUDF(argument, function) =>
      rewriteResultFromLambda(argument, function)(
        ArrayExists(_, _, followThreeValuedLogic))

    case ArrayForAll(argument, function) if hasLiftableUDF(argument, function) =>
      rewriteResultFromLambda(argument, function)(ArrayForAll(_, _))

    // `filter`'s lambda is a predicate, so its result is not the value we want: the surviving
    // *input* elements are. Filter over the carrier, then project the original element back out.
    case ArrayFilter(argument, function) if hasLiftableUDF(argument, function) =>
      val filtered = rewriteResultFromLambda(argument, function)(ArrayFilter(_, _))
      unwrapCarrier(filtered)

    // For `aggregate`, only `merge`'s element argument iterates the array, so a UDF on the
    // element can be precomputed. A UDF on the accumulator cannot (step n depends on step n-1)
    // and is left for CheckAnalysis to reject.
    case ArrayAggregate(argument, zero, merge @ LambdaFunction(
        _, Seq(accVar: NamedLambdaVariable, elementVar: NamedLambdaVariable), _), finish)
        if hasLiftableUDF(argument, merge) && !dependsOnAccumulator(merge) =>
      val (carrier, rewrittenBody, boundVar) =
        buildCarrier(argument, merge, elementVar, None)
      ArrayAggregate(
        carrier,
        zero,
        LambdaFunction(rewrittenBody, Seq(accVar, boundVar)),
        finish)
  }

  /**
   * True if the lambda body contains a Python UDF that this rule can lift out, and the array to
   * lift it onto can be computed outside every lambda.
   *
   * The second condition rules out nested higher-order functions such as
   * `transform(arr, inner -> transform(inner, x -> udf(x)))`: the inner array only exists while
   * the outer function iterates, so a UDF lifted onto it would still sit inside the outer lambda
   * and could not be extracted. `CheckAnalysis` rejects that shape using the same predicate, so
   * the two must agree: a plan analysis accepts must be one this rule actually rewrites.
   */
  private def hasLiftableUDF(argument: Expression, function: Expression): Boolean =
    function match {
      case LambdaFunction(body, _, _) =>
        body.exists(isRewritableUDF) && !PythonUDF.hasFreeLambdaVariable(argument)
      case _ => false
    }

  /**
   * True if any rewritable UDF in `merge` reads the accumulator. Such a fold is sequential and
   * has no array to precompute over, so it is not rewritten.
   */
  private def dependsOnAccumulator(merge: Expression): Boolean = merge match {
    case LambdaFunction(body, Seq(accVar: NamedLambdaVariable, _), _) =>
      body.exists {
        case udf: PythonUDF if isRewritableUDF(udf) =>
          udf.exists {
            case v: NamedLambdaVariable => v.exprId == accVar.exprId
            case _ => false
          }
        case _ => false
      }
    case _ => true
  }

  /**
   * Rewrites a single-argument higher-order function whose result is the lambda's own value.
   * `rebuild` reassembles the function from the carrier argument and the rewritten lambda.
   */
  private def rewriteResultFromLambda(
      argument: Expression,
      function: Expression)(
      rebuild: (Expression, LambdaFunction) => Expression): Expression = {
    val LambdaFunction(_, args, _) = function
    val elementVar = args.head.asInstanceOf[NamedLambdaVariable]
    val indexVar = args.tail.headOption.map(_.asInstanceOf[NamedLambdaVariable])
    val (carrier, rewrittenBody, boundVar) =
      buildCarrier(argument, function, elementVar, indexVar)
    rebuild(carrier, LambdaFunction(rewrittenBody, Seq(boundVar)))
  }

  /**
   * Builds the carrier array and the rewritten lambda body.
   *
   * The carrier is `arrays_zip` of the original array, one array per lifted UDF, and - when the
   * lambda declares an index parameter - an index array. The rewritten body reads each of those
   * through a struct field of the single lambda variable bound to the carrier.
   *
   * Returns the carrier argument, the rewritten body and the new lambda variable.
   */
  private def buildCarrier(
      argument: Expression,
      function: Expression,
      elementVar: NamedLambdaVariable,
      indexVar: Option[NamedLambdaVariable]): (Expression, Expression, NamedLambdaVariable) = {
    val LambdaFunction(body, _, _) = function

    // Collect the UDF calls to lift. Innermost first, so that a nested call like `f(g(x))` has
    // `g` lifted before `f`, letting `f`'s array UDF consume `g`'s array result.
    val liftable = collectLiftableUDFs(body, elementVar, indexVar)

    val elementType = argument.dataType.asInstanceOf[ArrayType].elementType
    val containsNull = argument.dataType.asInstanceOf[ArrayType].containsNull

    // Each lifted UDF becomes an `array<T> => array<R>` call over the whole array. Its
    // arguments are the same expressions, with the element/index variable replaced by the
    // array itself so the worker sees one list column per argument.
    val indexArray = indexVar.map { _ =>
      val v = NamedLambdaVariable("x", elementType, containsNull)
      val i = NamedLambdaVariable("i", IntegerType, nullable = false)
      ArrayTransform(argument, LambdaFunction(i, Seq(v, i)))
    }

    var arrayResults = Map.empty[Expression, Expression]
    val liftedArrays = liftable.map { udf =>
      val arrayArgs = udf.children.map { child =>
        overArray(child, argument, elementVar, indexVar, indexArray, arrayResults)
      }
      val lifted = PythonUDF(
        udf.name,
        udf.func,
        // The wrapper returns one element per input element, i.e. an array of the UDF's type.
        // Elements may be null (the UDF can return null), hence containsNull = true.
        ArrayType(udf.dataType, containsNull = true),
        arrayArgs,
        PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
        udf.udfDeterministic)
      arrayResults += (udf.canonicalized -> lifted)
      lifted
    }

    // The carrier: the original elements first, then one field per lifted UDF, then the index.
    val carrierFields =
      Seq(argument) ++ liftedArrays ++ indexArray.toSeq
    val carrierNames =
      Seq(CarrierElementField) ++
        liftedArrays.indices.map(i => s"$CarrierUDFFieldPrefix$i") ++
        indexArray.map(_ => CarrierIndexField).toSeq
    val carrier = ArraysZip(carrierFields, carrierNames.map(Literal(_)))

    // A single lambda variable bound to the carrier's struct element replaces both the original
    // element variable and the index variable.
    val structType = carrier.dataType.asInstanceOf[ArrayType].elementType
    val boundVar = NamedLambdaVariable("s", structType, nullable = false)

    // Rewrite the body: each lifted UDF call becomes a struct field read, the element variable
    // becomes the element field, and the index variable becomes the index field.
    //
    // This must be top-down: a UDF call is matched by its canonicalized form, and rewriting its
    // arguments first (the element variable becoming a struct field read) would change that form
    // so the call no longer matches and would be left inside the lambda. Replacing the call
    // outright also stops the traversal from descending into arguments that no longer exist.
    val udfFieldByCanonical = liftable.map(_.canonicalized).zipWithIndex.toMap
    val rewrittenBody = body.transformDown {
      case udf: PythonUDF if udfFieldByCanonical.contains(udf.canonicalized) =>
        val index = udfFieldByCanonical(udf.canonicalized)
        GetStructField(boundVar, 1 + index, Some(s"$CarrierUDFFieldPrefix$index"))
      case v: NamedLambdaVariable if v.exprId == elementVar.exprId =>
        GetStructField(boundVar, 0, Some(CarrierElementField))
      case v: NamedLambdaVariable if indexVar.exists(_.exprId == v.exprId) =>
        GetStructField(boundVar, carrierFields.length - 1, Some(CarrierIndexField))
    }

    (carrier, rewrittenBody, boundVar)
  }

  /**
   * Collects the Python UDF calls in `body` that must be lifted, innermost first.
   *
   * Only calls that actually read the lambda's variables need lifting; a UDF over constants or
   * outer columns is already valid outside the lambda and is left to [[ExtractPythonUDFs]].
   */
  private def collectLiftableUDFs(
      body: Expression,
      elementVar: NamedLambdaVariable,
      indexVar: Option[NamedLambdaVariable]): Seq[PythonUDF] = {
    val lambdaExprIds =
      Set(elementVar.exprId) ++ indexVar.map(_.exprId).toSet
    val collected = Seq.newBuilder[PythonUDF]
    def visit(e: Expression): Unit = {
      // Children first, so nested calls come out innermost-first.
      e.children.foreach(visit)
      e match {
        case udf: PythonUDF if isRewritableUDF(udf) && readsLambdaVariable(udf, lambdaExprIds) =>
          collected += udf
        case _ =>
      }
    }
    visit(body)
    // Deduplicate identical calls so the same UDF is evaluated once per array.
    val seen = scala.collection.mutable.LinkedHashMap.empty[Expression, PythonUDF]
    collected.result().foreach(udf => seen.getOrElseUpdate(udf.canonicalized, udf))
    seen.values.toSeq
  }

  private def readsLambdaVariable(e: Expression, lambdaExprIds: Set[ExprId]): Boolean =
    e.exists {
      case v: NamedLambdaVariable => lambdaExprIds.contains(v.exprId)
      case _ => false
    }

  /**
   * Turns a UDF argument expression, written in terms of a single element, into the equivalent
   * expression over the whole array.
   *
   *  - the element variable itself becomes the array;
   *  - the index variable becomes the index array;
   *  - an already-lifted nested UDF call becomes its array result;
   *  - anything else must be independent of the lambda (a constant or an outer column) and is
   *    passed through unchanged, to be broadcast per element by the worker.
   */
  private def overArray(
      child: Expression,
      argument: Expression,
      elementVar: NamedLambdaVariable,
      indexVar: Option[NamedLambdaVariable],
      indexArray: Option[Expression],
      arrayResults: Map[Expression, Expression]): Expression = child match {
    case v: NamedLambdaVariable if v.exprId == elementVar.exprId => argument
    case v: NamedLambdaVariable if indexVar.exists(_.exprId == v.exprId) => indexArray.get
    case udf: PythonUDF if arrayResults.contains(udf.canonicalized) =>
      arrayResults(udf.canonicalized)
    case e if !readsLambdaVariable(
        e, Set(elementVar.exprId) ++ indexVar.map(_.exprId).toSet) =>
      // Independent of the element: pass through and let the worker broadcast it.
      e
    case e =>
      // An expression over the element, e.g. `udf(x * 2)`. Compute it for every element with a
      // native `transform`, which stays inside the JVM, and hand the resulting array over.
      val v = NamedLambdaVariable(
        "x",
        argument.dataType.asInstanceOf[ArrayType].elementType,
        argument.dataType.asInstanceOf[ArrayType].containsNull)
      val body = e.transformUp {
        case old: NamedLambdaVariable if old.exprId == elementVar.exprId => v
      }
      ArrayTransform(argument, LambdaFunction(body, Seq(v)))
  }

  /**
   * Projects the original elements back out of a carrier array. Used by `filter`, whose result
   * is built from the input elements rather than from the lambda's return value.
   */
  private def unwrapCarrier(carrierArray: Expression): Expression = {
    val structType =
      carrierArray.dataType.asInstanceOf[ArrayType].elementType
    val v = NamedLambdaVariable("s", structType, nullable = false)
    ArrayTransform(
      carrierArray,
      LambdaFunction(GetStructField(v, 0, Some(CarrierElementField)), Seq(v)))
  }

  private val CarrierElementField = "c0"
  private val CarrierUDFFieldPrefix = "u"
  private val CarrierIndexField = "idx"
}
