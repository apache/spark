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
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType}

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
 * The way out is to not evaluate the UDF per element inside the lambda. Instead the UDF is applied
 * once to the *whole array*, outside every lambda, and the lambda reads the precomputed result
 * positionally:
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
 * so the array-at-a-time behaviour lives in the Python worker: it flattens each incoming list
 * column one level, calls the user function once over the concatenated elements of the whole batch,
 * and re-nests the results with the input's offsets. This keeps one row in and one row out (no
 * `explode`, no shuffle), and crosses the Python boundary once per batch rather than once per row.
 *
 * Every argument the lifted UDF receives is therefore a single-level `array<T>` aligned with the
 * iterated array - the same length per row and the same null rows - so the worker can flatten all
 * of them uniformly. An argument that does not depend on the element (an outer column or a
 * constant) is materialized into such an array with a native `transform` that repeats the value,
 * rather than being passed through as a scalar; that removes any per-argument shape metadata.
 *
 * Once the UDF result is an ordinary column, everything the lambda does around it is ordinary JVM
 * work, so arithmetic, `when`, casts, the element index, several UDFs in one lambda and nested UDF
 * *calls* (`f(g(x))`, lifted innermost-first within one carrier) all follow without special cases.
 *
 * This rule runs before [[ExtractPythonUDFs]], which then extracts the lifted UDF as a normal
 * top-level `PythonUDF` and needs no new physical operator.
 *
 * All ten mapping higher-order functions that take a single lambda are handled:
 *
 *  - `transform`, `exists`, `forall` return the lambda's own value, so the carrier replaces the
 *    argument directly;
 *  - `filter`'s lambda is a predicate, so the original elements are projected back out of the
 *    carrier afterwards;
 *  - `zip_with` zips both arrays into one carrier, which collapses it to the single-array form;
 *  - `array_sort` precomputes the UDF per element as a sort key for the JVM comparator to compare;
 *    when one call takes both elements it is precomputed over the cross product of pairs instead,
 *    which the comparator then indexes by position;
 *  - `transform_keys`, `transform_values`, `map_filter` and `map_zip_with` are desugared into the
 *    array case over `map_keys` / `map_values` and rebuilt with `map_from_arrays`.
 *
 * Shapes that genuinely cannot be rewritten are left untouched here and continue to be rejected by
 * `CheckAnalysis`:
 *
 *  - a UDF inside a *nested* higher-order function's lambda, e.g.
 *    `transform(arr, i -> transform(i, x -> f(x)))`. The inner array `i` is the outer lambda's
 *    variable, so it does not exist as a column outside the lambda and the UDF cannot be lifted
 *    onto a real array. (A UDF in a nested function's *argument*, `transform(arr, x ->
 *    transform(udf(x), y -> y))`, is fine: `udf(x)` is lifted onto `arr` like any other.)
 *  - a UDF anywhere in `aggregate` / `reduce`. The fold is sequential: `merge` runs on each element
 *    with the running accumulator, so its values are outputs of earlier steps rather than elements
 *    of a collection, and computing the final accumulator alternates Python and JVM work once per
 *    element, which no single UDF call can do. That is a limit of expression rewriting rather than
 *    of Spark: restating the fold as an iteration over the whole column with `UnionLoop` would
 *    work, but that is an operator-level restructuring, so it is left for a follow-up;
 *  - a pandas UDF: it receives a `Series` rather than one value per call, so the element-wise
 *    rewrite would change its meaning.
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
      // A single bottom-up pass lifts every liftable UDF: `transformExpressionsUpWithPruning`
      // visits the innermost higher-order function first, and each rewrite lifts all of that
      // lambda's UDFs at once. A UDF inside a *nested* function's lambda is not liftable at all
      // (its argument is the outer lambda's variable, which is not a real column) and is rejected
      // by `CheckAnalysis`, so no repeated fixed-point pass is needed.
      plan.transformUpWithPruning(
        _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION)) {
        case p =>
          p.transformExpressionsUpWithPruning(
            _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION))(rewrite)
      }
    }
  }

  /**
   * Where a higher-order function's result comes from, which is the one thing about its shape that
   * cannot be derived from the generic [[HigherOrderFunction]] API.
   *
   * A rewritten function iterates a *carrier* - a struct of the original elements plus the
   * precomputed UDF results - so a function whose result is built from its input elements has to
   * project them back out afterwards, while one that returns its lambda's own value does not.
   */
  private sealed trait ResultShape
  /** The result is the lambda's own value (`transform`, `exists`, `zip_with`, ...). */
  private case object FromLambda extends ResultShape
  /** The result is built from the input elements, so the carrier must be unwrapped (`filter`). */
  private case object FromElements extends ResultShape

  /**
   * Where a higher-order function's result comes from. This is read straight off the Catalyst
   * classification traits ([[ResultTypeFromArgument]] / [[ResultTypeFromFunction]]), which every
   * lambda-taking function carries: a function whose result type is its input's returns input
   * elements (`filter`, `array_sort`, `map_filter`), and one whose result type follows its lambda
   * returns the lambda's value. A boolean predicate (`exists`, `forall`) is neither, but the
   * rewrite treats it as `FromLambda` since it iterates a carrier the same way; `None` here means a
   * shape this rule does not handle.
   */
  private def resultShapeOf(hof: HigherOrderFunction): Option[ResultShape] = hof match {
    case _: ResultTypeFromArgument => Some(FromElements)
    case _: ResultTypeFromFunction => Some(FromLambda)
    case _: Predicate => Some(FromLambda)
    case _ => None
  }

  /**
   * Whether `hof`'s lambda is a comparator, i.e. its two parameters are two elements of the same
   * argument rather than one element of each of two arguments.
   *
   * This cannot be inferred from the parameter types: a comparator over `array<int>` and an indexed
   * lambda over `array<int>` both have parameters `(Int, Int)`.
   */
  private def isComparatorShaped(hof: HigherOrderFunction): Boolean = hof match {
    case _: ArraySort => true
    case _ => false
  }

  /**
   * Rewrites a single higher-order function whose lambda contains a rewritable Python UDF.
   *
   * There is one generic path for every mapping function. It never names a concrete class: it reads
   * the arguments, lambdas and parameter roles off the [[HigherOrderFunction]] API and rebuilds the
   * node with `withNewChildren`, relying on the fact that a higher-order function's children are
   * always its `arguments` followed by its `functions`.
   */
  private val rewrite: PartialFunction[Expression, Expression] = {
    // A comparator whose UDF receives both elements in one call has no per-element key, so it takes
    // the cross product rather than the ordinary element-wise rewrite.
    case sort @ ArraySort(_, function, _)
        if liftableHof(sort) && PythonUDF.comparatorTakesBothElements(function) =>
      rewritePairwiseComparator(sort)

    case hof: HigherOrderFunction if resultShapeOf(hof).isDefined && liftableHof(hof) =>
      rewriteMapping(hof, resultShapeOf(hof).get)
  }

  /**
   * Rewrites `array_sort(arr, (a, b) -> udf(a, b))`, where one UDF call receives both elements.
   *
   * There is no per-element key here, so nothing can be precomputed per element. What can be
   * precomputed is the UDF over every ordered pair: an n x n matrix whose (i, j) entry is
   * `udf(arr[i], arr[j])`. The comparator then reads its answer out of that matrix by the two
   * elements' positions, so no Python call happens while sorting.
   *
   * The pairs are laid out as two flat arrays of n*n elements - each element repeated n times
   * against the whole array repeated n times - and the UDF is applied to them **directly**, not
   * inside a `zip_with`. That matters: wrapping it in another higher-order function would put the
   * UDF back inside a lambda, which is exactly what this rule exists to undo.
   *
   * This costs O(n^2) Python calls where sorting needs only O(n log n) comparisons, and O(n^2)
   * memory for the matrix. Returning a per-element sort key instead stays on the O(n) path.
   */
  private def rewritePairwiseComparator(sort: ArraySort): Expression = {
    val ArraySort(argument, function, allowNull) = sort
    val LambdaFunction(body, Seq(leftVar: NamedLambdaVariable, rightVar: NamedLambdaVariable), _) =
      function
    val arrayType = argument.dataType.asInstanceOf[ArrayType]
    val elementType = arrayType.elementType
    val containsNull = arrayType.containsNull
    val n = Size(argument)

    // The two sides of the cross product. `array_repeat` avoids introducing a lambda that could
    // capture the UDF; the one lambda here holds only the repeat, never the UDF.
    val repeatVar = NamedLambdaVariable("a", elementType, containsNull)
    val lefts = Flatten(
      ArrayTransform(argument, LambdaFunction(ArrayRepeat(repeatVar, n), Seq(repeatVar))))
    val rights = Flatten(ArrayRepeat(argument, n))

    // The UDF over all n*n pairs, applied outside every lambda. Any part of the comparator body
    // around the UDF call is ordinary JVM work over the same flat arrays, so the body is rewritten
    // with each element variable bound to its flat array. This is exactly the element-wise rewrite
    // with the pair arrays standing in for the iterated arguments, so `buildCarrier` does the work:
    // it lifts each UDF over the pairs and rewrites the body to read the results from a carrier.
    // A `transform` over that carrier then evaluates whatever the comparator wrapped around the
    // call - a cast, a `when`, arithmetic - once per pair, in the JVM.
    val pairLambda = LambdaFunction(body, Seq(leftVar, rightVar))
    val pairCarrier = buildCarrier(Seq(lefts, rights), pairLambda, Seq(leftVar, rightVar), None)
    val flatCells = ArrayTransform(
      pairCarrier.carrier, LambdaFunction(pairCarrier.body, Seq(pairCarrier.boundVar)))

    // Carry each element's position so the comparator can index the matrix, sort by
    // `matrix[a.idx][b.idx]`, then drop the positions again. `element_at` is 1-based.
    val posElem = NamedLambdaVariable("x", elementType, containsNull)
    val posIdx = NamedLambdaVariable("i", IntegerType, nullable = false)
    val indexed = ArraysZip(
      Seq(argument, ArrayTransform(argument, LambdaFunction(posIdx, Seq(posElem, posIdx)))),
      Seq(Literal(s"${CarrierElementPrefix}0"), Literal(CarrierIndexField)))
    val indexedElement = indexed.dataType.asInstanceOf[ArrayType].elementType

    // The matrix, as n rows of n taken from the flat results.
    val rowElem = NamedLambdaVariable("x", elementType, containsNull)
    val rowIdx = NamedLambdaVariable("i", IntegerType, nullable = false)
    val matrix = ArrayTransform(
      argument,
      LambdaFunction(
        Slice(flatCells, Add(Multiply(rowIdx, n), Literal(1)), n),
        Seq(rowElem, rowIdx)))

    val cmpLeft = NamedLambdaVariable("a", indexedElement, nullable = false)
    val cmpRight = NamedLambdaVariable("b", indexedElement, nullable = false)
    def indexOf(v: NamedLambdaVariable): Expression =
      Add(GetStructField(v, 1, Some(CarrierIndexField)), Literal(1))
    val comparison = ElementAt(
      ElementAt(matrix, indexOf(cmpLeft), None, failOnError = false),
      indexOf(cmpRight),
      None,
      failOnError = false)

    unwrapCarrier(
      ArraySort(indexed, LambdaFunction(comparison, Seq(cmpLeft, cmpRight)), allowNull), 0)
  }



  /**
   * The generic rewrite for a mapping higher-order function.
   *
   * A map-valued argument is first desugared to its key and value arrays, so everything below works
   * in terms of arrays; the result is rebuilt as a map afterwards. The lambda's parameters are then
   * matched to those arrays, the UDFs are lifted onto them, and the node is rebuilt around a
   * carrier that the single new lambda parameter reads.
   */
  private def rewriteMapping(hof: HigherOrderFunction, shape: ResultShape): Expression = {
    val lambda = hof.functions.head.asInstanceOf[LambdaFunction]

    // Desugar maps into arrays. `map_zip_with` visits the union of both key sets and looks each map
    // up per key, which yields null for a key missing from one side - exactly its own semantics.
    val mapValued = hof.arguments.exists(_.dataType.isInstanceOf[MapType])
    val (arrays, rebuildResult): (Seq[Expression], Expression => Expression) =
      if (!mapValued) {
        (hof.arguments, identity)
      } else if (hof.arguments.length == 1) {
        val map = hof.arguments.head
        val keys = MapKeys(map)
        val values = MapValues(map)
        // `transform_keys` replaces the keys, `transform_values` the values, `map_filter` keeps
        // whichever pairs survive. Which of those applies follows from the result type and shape.
        val rebuild: Expression => Expression = shape match {
          case FromElements => (kept: Expression) =>
            MapFromArrays(unwrapCarrier(kept, 0), unwrapCarrier(kept, 1))
          case FromLambda if hof.dataType.asInstanceOf[MapType].keyType == lambda.dataType =>
            (newKeys: Expression) => MapFromArrays(newKeys, values)
          case FromLambda => (newValues: Expression) => MapFromArrays(keys, newValues)
        }
        (Seq(keys, values), rebuild)
      } else {
        val Seq(left, right) = hof.arguments
        val keys = ArrayUnion(MapKeys(left), MapKeys(right))
        val keyType = keys.dataType.asInstanceOf[ArrayType]
        def valuesFor(map: Expression): Expression = {
          val k = NamedLambdaVariable("k", keyType.elementType, keyType.containsNull)
          ArrayTransform(keys, LambdaFunction(ElementAt(map, k, None, failOnError = false), Seq(k)))
        }
        (Seq(keys, valuesFor(left), valuesFor(right)),
          (newValues: Expression) => MapFromArrays(keys, newValues))
      }

    // Match the lambda's parameters to the arrays they iterate. Leading parameters correspond
    // one-to-one with the arrays; a trailing extra parameter is the element index.
    //
    // A comparator is the one shape where that is not true: its two parameters are two elements of
    // the *same* array. It cannot be told apart from an indexed lambda by parameter types alone
    // (both are `(T, Int)` when `T` is `Int`), so it is identified by the function itself, which is
    // exactly what the declarative shape table is for.
    val params = lambda.arguments.map(_.asInstanceOf[NamedLambdaVariable])
    val (elementVars, indexVar, alsoBind) =
      if (isComparatorShaped(hof)) {
        (Seq(params.head), None, Seq(params.last))
      } else {
        (params.take(arrays.length), params.drop(arrays.length).headOption, Nil)
      }

    val built = buildCarrier(arrays, lambda, elementVars, indexVar, alsoBind)
    val newLambda = LambdaFunction(built.body, built.boundVar +: built.extraBoundVars)

    // Rebuild the node. The carrier collapses every argument into one, so a function that keeps its
    // own semantics (`filter` keeps only some elements, `exists`/`forall`/`array_sort` are not
    // mappings) is rebuilt generically with `withNewChildren` - a higher-order function's children
    // are its arguments followed by its functions, so one carrier plus the rewritten lambda
    // reconstructs any of them without naming the class. A function that is a plain mapping over
    // however many arguments becomes a `transform` over the carrier.
    val keepsOwnNode = hof.arguments.length == 1 && !mapValued
    val iterated =
      if (keepsOwnNode) {
        hof.withNewChildren(IndexedSeq(built.carrier, newLambda)).asInstanceOf[Expression]
      } else if (shape == FromElements) {
        // A map-valued `filter`: the carrier must survive the filtering so both the keys and the
        // values can be projected out of it afterwards.
        ArrayFilter(built.carrier, newLambda)
      } else {
        ArrayTransform(built.carrier, newLambda)
      }

    // `FromElements` means the result is the input elements rather than the lambda's value, so they
    // are projected back out of the carrier. For a map that projection is `rebuildResult`'s job,
    // since it knows which of the key/value sides to keep.
    if (!mapValued && shape == FromElements) rebuildResult(unwrapCarrier(iterated, 0))
    else rebuildResult(iterated)
  }

  /**
   * True if `hof`'s lambda holds a UDF this rule can lift out.
   *
   * The UDF must belong to *this* lambda, not to a nested higher-order function inside it. A UDF in
   * a nested lambda, `transform(arr, i -> transform(i, x -> udf(x)))`, reads the inner lambda's
   * variable, so it cannot be lifted onto an array outside the lambda at all; `CheckAnalysis`
   * rejects that shape before this rule runs, so it is simply never matched here.
   */
  private def liftableHof(hof: HigherOrderFunction): Boolean =
    hof.functions.length == 1 && (hof.functions.head match {
      case LambdaFunction(body, args, _) =>
        hasDirectRewritableUDF(body) && args.forall(_.isInstanceOf[NamedLambdaVariable])
      case _ => false
    })

  /**
   * Whether `body` holds a rewritable UDF that belongs to *this* lambda rather than to a nested
   * higher-order function's lambda.
   *
   * A nested function's own lambda is skipped: a UDF there reads that lambda's variable, a shape
   * `CheckAnalysis` rejects. Its *arguments* are not skipped, because they are evaluated outside
   * the nested lambda and so are part of this lambda's body: `transform(arr, x -> transform(udf(x),
   * y -> y))` has `udf(x)` in the inner function's argument, so it is lifted onto `arr` like any
   * other.
   */
  private def hasDirectRewritableUDF(body: Expression): Boolean = body match {
    case e if isRewritableUDF(e) => true
    case hof: HigherOrderFunction => hof.arguments.exists(hasDirectRewritableUDF)
    case e => e.children.exists(hasDirectRewritableUDF)
  }


  /**
   * Builds the carrier for `arguments` and hands the rewritten higher-order function to `rebuild`.
   */
  private def withCarrier(
      arguments: Seq[Expression],
      function: Expression)(
      rebuild: (Expression, LambdaFunction) => Expression): Expression = {
    val LambdaFunction(_, args, _) = function
    // The lambda's leading parameters correspond one-to-one with the arguments; a trailing extra
    // parameter is the element index (`transform`/`filter` with `(x, i) -> ...`).
    val elementVars = args.take(arguments.length).map(_.asInstanceOf[NamedLambdaVariable])
    val indexVar = args.drop(arguments.length).headOption.map(_.asInstanceOf[NamedLambdaVariable])
    val built = buildCarrier(arguments, function, elementVars, indexVar)
    rebuild(built.carrier, LambdaFunction(built.body, Seq(built.boundVar)))
  }

  /**
   * Builds the carrier for `array_sort`'s comparator.
   *
   * A comparator's two parameters are two elements of the *same* array, so unlike every other
   * shape they are not separate arguments: one carrier is built over the single array and both
   * parameters are bound to it. Each side then reads the precomputed key of its own element, which
   * is what makes the sort actually reorder rather than compare a value with itself.
   */
  private def withComparatorCarrier(
      argument: Expression,
      function: Expression)(
      rebuild: (Expression, LambdaFunction) => Expression): Expression = {
    val LambdaFunction(body, Seq(leftVar: NamedLambdaVariable, rightVar: NamedLambdaVariable), _) =
      function
    // Build the carrier as if the lambda took one element, using the left parameter. The right
    // parameter is then bound to a second variable over the same carrier.
    val built = buildCarrier(Seq(argument), function, Seq(leftVar), None, alsoBind = Seq(rightVar))
    val rightBound = built.extraBoundVars.head
    rebuild(built.carrier, LambdaFunction(built.body, Seq(built.boundVar, rightBound)))
  }

  /** The pieces produced by [[buildCarrier]]. */
  private case class Carrier(
      carrier: Expression,
      body: Expression,
      boundVar: NamedLambdaVariable,
      extraBoundVars: Seq[NamedLambdaVariable])

  /**
   * Builds the carrier array and the rewritten lambda body.
   *
   * The carrier is `arrays_zip` of the original arrays, one array per lifted UDF, and - when the
   * lambda declares an index parameter - an index array. The rewritten body reads each of those
   * through a struct field of the lambda variable bound to the carrier.
   *
   * `alsoBind` names further lambda variables that should read the same carrier; it exists for
   * `array_sort`'s comparator, whose two parameters are both elements of the same array.
   */
  private def buildCarrier(
      arguments: Seq[Expression],
      function: Expression,
      elementVars: Seq[NamedLambdaVariable],
      indexVar: Option[NamedLambdaVariable],
      alsoBind: Seq[NamedLambdaVariable] = Nil): Carrier = {
    val LambdaFunction(body, _, _) = function
    val lambdaExprIds =
      (elementVars ++ indexVar.toSeq ++ alsoBind).map(_.exprId).toSet

    // Collect the UDF calls to lift. Innermost first, so that a nested call like `f(g(x))` has
    // `g` lifted before `f`, letting `f`'s array UDF consume `g`'s array result.
    val liftableUDFs = collectLiftableUDFs(body, lambdaExprIds)

    // With more than one argument the arrays may be ragged (`zip_with` pads the shorter side with
    // nulls, and `map_zip_with`'s per-key lookups can too). Flattening ragged arrays independently
    // would yield different element counts and misalign them, so every argument is first projected
    // out of one common zip. That pads them all to the same per-row length, which is what the
    // positional rewrite requires.
    val alignedArguments =
      if (arguments.length > 1) {
        val names = arguments.indices.map(i => s"$CarrierElementPrefix$i")
        val zipped = ArraysZip(arguments, names.map(Literal(_)))
        arguments.indices.map(i => unwrapCarrier(zipped, i))
      } else {
        arguments
      }

    // An index array, when the lambda asked for the element index.
    val indexArray = indexVar.map { _ =>
      val head = alignedArguments.head
      val headType = head.dataType.asInstanceOf[ArrayType]
      val v = NamedLambdaVariable("x", headType.elementType, headType.containsNull)
      val i = NamedLambdaVariable("i", IntegerType, nullable = false)
      ArrayTransform(head, LambdaFunction(i, Seq(v, i)))
    }

    // Maps each element/index variable to the array it stands for, so a UDF argument written in
    // terms of the variables can be rewritten as an expression over whole arrays. For a
    // comparator, `alsoBind`'s variables denote the same array as the element variable.
    val arrayOfVar: Map[ExprId, Expression] =
      elementVars.map(_.exprId).zip(alignedArguments).toMap ++
        indexVar.map(_.exprId -> indexArray.get).toMap ++
        alsoBind.map(_.exprId -> alignedArguments.head).toMap

    var arrayResults = Map.empty[Expression, Expression]
    val liftedArrays = liftableUDFs.map { udf =>
      // Every argument is turned into an `array<T>` aligned with the iterated array, so the worker
      // flattens each one exactly once. `overArray` maps a lambda variable to its array, computes
      // an expression over the elements with a native `transform`, and broadcasts an
      // element-independent value into an aligned array too - so there is no per-argument shape to
      // track.
      val arrayArgs = udf.children.map { child =>
        overArray(child, alignedArguments.head, arrayOfVar, lambdaExprIds, arrayResults)
      }
      val lifted = PythonUDF(
        udf.name,
        udf.func,
        // The wrapper returns one element per input element, i.e. one array level on top of the
        // user function's scalar return. Elements may be null (the UDF can return null), hence
        // containsNull = true.
        ArrayType(udf.dataType, containsNull = true),
        arrayArgs,
        PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
        udf.udfDeterministic)
      arrayResults += (udf.canonicalized -> lifted)
      lifted
    }

    // The carrier: the original arrays first, then one field per lifted UDF, then the index.
    val carrierFields = alignedArguments ++ liftedArrays ++ indexArray.toSeq
    val carrierNames =
      arguments.indices.map(i => s"$CarrierElementPrefix$i") ++
        liftedArrays.indices.map(i => s"$CarrierUDFFieldPrefix$i") ++
        indexArray.map(_ => CarrierIndexField).toSeq
    val carrier = ArraysZip(carrierFields, carrierNames.map(Literal(_)))

    val structType = carrier.dataType.asInstanceOf[ArrayType].elementType
    val boundVar = NamedLambdaVariable("s", structType, nullable = false)
    val extraBoundVars = alsoBind.map(v =>
      NamedLambdaVariable(v.name, structType, nullable = false))

    // Which struct field each lambda variable reads. For a comparator, `alsoBind`'s variable reads
    // the same ordinals but through its own bound variable.
    val fieldOfVar: Map[ExprId, Int] =
      elementVars.map(_.exprId).zipWithIndex.toMap ++
        indexVar.map(_.exprId -> (carrierFields.length - 1)).toMap
    val extraVarOf: Map[ExprId, NamedLambdaVariable] =
      alsoBind.map(_.exprId).zip(extraBoundVars).toMap
    val udfFieldByCanonical = liftableUDFs.map(_.canonicalized).zipWithIndex.toMap

    // Rewrite the body. This must be top-down: a UDF call is matched by its canonicalized form,
    // and rewriting its arguments first (a variable becoming a struct field read) would change
    // that form so the call no longer matches and would be left inside the lambda. Replacing the
    // call outright also stops the traversal descending into arguments that no longer exist.
    def readerFor(v: NamedLambdaVariable, udfOrdinal: Option[Int]): Expression = {
      val base = extraVarOf.getOrElse(v.exprId, boundVar)
      udfOrdinal match {
        case Some(u) =>
          GetStructField(base, arguments.length + u, Some(s"$CarrierUDFFieldPrefix$u"))
        case None =>
          val ordinal = fieldOfVar(v.exprId)
          GetStructField(base, ordinal, Some(carrierNames(ordinal)))
      }
    }

    val rewrittenBody = body.transformDown {
      case udf: PythonUDF if udfFieldByCanonical.contains(udf.canonicalized) =>
        val ordinal = udfFieldByCanonical(udf.canonicalized)
        // A UDF over a comparator's right-hand element must read that element's key, so the
        // struct field is read through whichever bound variable the call's own arguments used.
        val side = udf.collectFirst {
          case v: NamedLambdaVariable if extraVarOf.contains(v.exprId) => v
        }
        side match {
          case Some(v) => readerFor(v, Some(ordinal))
          case None =>
            GetStructField(boundVar, arguments.length + ordinal,
              Some(s"$CarrierUDFFieldPrefix$ordinal"))
        }
      case v: NamedLambdaVariable if fieldOfVar.contains(v.exprId) => readerFor(v, None)
      case v: NamedLambdaVariable if extraVarOf.contains(v.exprId) =>
        // A comparator's right-hand element itself, read through its own bound variable.
        GetStructField(extraVarOf(v.exprId), 0, Some(carrierNames.head))
    }

    Carrier(carrier, rewrittenBody, boundVar, extraBoundVars)
  }

  /**
   * Collects the Python UDF calls in `body` that must be lifted, innermost first.
   *
   * Only calls that actually read the lambda's variables need lifting; a UDF over constants or
   * outer columns is already valid outside the lambda and is left to [[ExtractPythonUDFs]].
   */
  private def collectLiftableUDFs(
      body: Expression,
      lambdaExprIds: Set[ExprId]): Seq[PythonUDF] = {
    val collected = Seq.newBuilder[PythonUDF]
    def visit(e: Expression): Unit = {
      // A nested higher-order function's lambda is not ours to rewrite, but its arguments are
      // evaluated outside that lambda and so belong to this body. See `hasDirectRewritableUDF`.
      val children = e match {
        case hof: HigherOrderFunction => hof.arguments
        case other => other.children
      }
      // Children first, so nested calls come out innermost-first.
      children.foreach(visit)
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
   * Turns a UDF argument expression, written in terms of single elements, into the equivalent
   * `array<T>` aligned with the iterated array, so the worker can flatten every argument uniformly.
   *
   *  - a lambda variable becomes the array it stands for;
   *  - an already-lifted UDF call (a nested call `f(g(x))`) becomes its array result;
   *  - an expression independent of the lambda is repeated into an aligned array with a native
   *    `transform`, rather than passed through as a scalar to broadcast;
   *  - anything else is an expression over the elements, computed for every element by a native
   *    `transform` that stays inside the JVM.
   */
  private def overArray(
      child: Expression,
      firstArgument: Expression,
      arrayOfVar: Map[ExprId, Expression],
      lambdaExprIds: Set[ExprId],
      arrayResults: Map[Expression, Expression]): Expression = child match {
    case v: NamedLambdaVariable if arrayOfVar.contains(v.exprId) => arrayOfVar(v.exprId)
    case udf: PythonUDF if arrayResults.contains(udf.canonicalized) =>
      arrayResults(udf.canonicalized)
    case e if !readsLambdaVariable(e, lambdaExprIds) =>
      // Independent of the element (an outer column or constant): repeat it into an array aligned
      // with the iterated array, so every UDF argument is a single-level array the worker flattens
      // the same way. `transform(arr, _ -> e)` keeps the value constant while matching the shape.
      val arrType = firstArgument.dataType.asInstanceOf[ArrayType]
      val v = NamedLambdaVariable("x", arrType.elementType, arrType.containsNull)
      ArrayTransform(firstArgument, LambdaFunction(e, Seq(v)))
    case e =>
      // An expression over the element, e.g. `udf(x * 2)`. Compute it for every element with a
      // native `transform`, which stays inside the JVM, and hand the resulting array over.
      // Multi-argument shapes need the elements side by side, so zip them first.
      val arrays = arrayOfVar.values.toSeq.distinct
      if (arrays.length == 1) {
        val arr = arrays.head
        val elemType = arr.dataType.asInstanceOf[ArrayType]
        val v = NamedLambdaVariable("x", elemType.elementType, elemType.containsNull)
        val substituted = e.transformUp {
          case old: NamedLambdaVariable if arrayOfVar.contains(old.exprId) => v
        }
        ArrayTransform(arr, LambdaFunction(substituted, Seq(v)))
      } else {
        // Zip every array the expression may read, then project the fields it needs.
        val names = arrays.indices.map(i => s"$CarrierElementPrefix$i")
        val zipped = ArraysZip(arrays, names.map(Literal(_)))
        val structType = zipped.dataType.asInstanceOf[ArrayType].elementType
        val v = NamedLambdaVariable("z", structType, nullable = false)
        val ordinalOf = arrayOfVar.map { case (id, arr) => id -> arrays.indexOf(arr) }
        val substituted = e.transformUp {
          case old: NamedLambdaVariable if ordinalOf.contains(old.exprId) =>
            GetStructField(v, ordinalOf(old.exprId), Some(names(ordinalOf(old.exprId))))
        }
        ArrayTransform(zipped, LambdaFunction(substituted, Seq(v)))
      }
  }

  /**
   * Projects field `ordinal` back out of a carrier array. Used where the result is built from the
   * input elements rather than from the lambda's return value: `filter`, `array_sort` and the map
   * family.
   */
  private def unwrapCarrier(carrierArray: Expression, ordinal: Int): Expression = {
    val structType = carrierArray.dataType.asInstanceOf[ArrayType].elementType
    val v = NamedLambdaVariable("s", structType, nullable = false)
    ArrayTransform(
      carrierArray,
      LambdaFunction(
        GetStructField(v, ordinal, Some(s"$CarrierElementPrefix$ordinal")), Seq(v)))
  }

  private val CarrierElementPrefix = "c"
  private val CarrierUDFFieldPrefix = "u"
  private val CarrierIndexField = "idx"
}
