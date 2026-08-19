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
 * Rewrites scalar Python UDFs inside a higher-order function's lambda so they can be evaluated.
 *
 * A `PythonUDF` runs in a separate operator that [[ExtractPythonUDFs]] pulls out, but a lambda's
 * [[NamedLambdaVariable]]s only exist while the function iterates, so the UDF can neither stay in
 * the lambda nor be lifted out normally. Instead this rule applies the UDF once to the *whole
 * array*, outside every lambda, and has the lambda read the result positionally:
 *
 * {{{
 *   -- before (rejected)
 *   transform(values, x -> plus_one(x))
 *
 *   -- after (the PythonUDF is outside every lambda)
 *   transform(arrays_zip(values AS c0, plus_one_over_array(values) AS u0), s -> s.u0)
 * }}}
 *
 * `plus_one_over_array` is the same function re-typed as `array<T> => array<R>` and run with an
 * element-wise eval type chosen from the UDF's own flavor (see
 * [[PythonUDF.liftedElementwiseEvalType]]): the row-at-a-time UDFs share the pickle-based
 * [[org.apache.spark.api.python.PythonEvalType]]'s SQL_ARROW_ELEMENTWISE_UDF, while a scalar pandas
 * / Arrow UDF (and its iterator variant) lifts to its own element-wise type so the worker keeps
 * that flavor's batching contract.
 * The array-at-a-time behaviour lives in the Python worker: it flattens each list column once,
 * calls the function over all elements of the batch, and re-nests by the input's offsets - one row
 * in, one row out, one Python round trip per batch.
 *
 * Every lifted argument is a single-level `array<T>` aligned with the iterated array (an
 * element-independent value is repeated into one with a native `transform`), so the worker flattens
 * them uniformly with no per-argument metadata. With the result now an ordinary column, arithmetic,
 * `when`, casts, the element index, multiple UDFs and nested calls `f(g(x))` all just work.
 *
 * Runs before [[ExtractPythonUDFs]]. Handles all ten single-lambda functions: `transform`,
 * `filter`, `exists`, `forall`, `zip_with`, `array_sort`, and the four map functions (desugared to
 * `map_keys`/`map_values` arrays and rebuilt with `map_from_arrays`). `array_sort` precomputes a
 * per-element key, or, when one call takes both elements, the UDF over the cross product of pairs.
 * A UDF in a *nested* lambda, `transform(arr, i -> transform(i, x -> f(x)))`, is handled too: the
 * whole nest is rewritten root-first (see `apply` / `rewriteNest`), lifting the UDF out one lambda
 * level at a time so it ends up applied to the fully flattened leaves.
 *
 * `CheckAnalysis` still rejects what this rule does not handle:
 *  - a UDF in `aggregate` / `reduce`: the fold is sequential (the UDF sees earlier steps' outputs,
 *    not array elements), so it cannot be applied once to the whole array.
 */
object ExtractPythonUDFFromLambda extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.pythonUDFInHigherOrderFunctionEnabled) {
      plan
    } else {
      // Rewrite each expression tree top-down. The first (outermost) higher-order function that is
      // a liftable *nest root* - it iterates real columns and its whole, possibly nested, nest is
      // rewritable - has its entire nest rewritten in one action by `rewriteNest`. Handling the
      // nest atomically means a UDF in a nested lambda is never momentarily left as a free-variable
      // `PythonUDF` that downstream could not evaluate (SPARK-48706): the rule either rewrites a
      // nest completely or leaves it untouched for `CheckAnalysis` to reject. The gate mirrors
      // `CheckAnalysis` exactly, so analysis accepts precisely what this rewrites.
      plan.transformUpWithPruning(
        _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION)) {
        case p =>
          p.transformExpressionsDownWithPruning(
            _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION)) {
            case hof: HigherOrderFunction
                if hof.functions.exists(_.exists(_.isInstanceOf[PythonUDF])) &&
                  PythonUDF.canRewritePythonUDFInLambda(hof) =>
              rewriteNest(hof)
          }
      }
    }
  }

  /**
   * Rewrites a whole nest of higher-order functions rooted at `root`, already validated liftable by
   * [[PythonUDF.canRewritePythonUDFInLambda]]. `transformUp` visits the innermost function first,
   * so each inner lambda's UDFs are lifted onto that lambda's (enclosing-variable) argument -
   * becoming element-wise UDFs in an argument position - and then the enclosing function re-lifts
   * them onto its own array one level deeper (see `buildCarrier` / `overArray`). A single bottom-up
   * pass therefore lifts every UDF out of every lambda in the nest, innermost first.
   */
  private def rewriteNest(root: HigherOrderFunction): Expression = root.transformUp(rewriteOne)

  /**
   * Whether one UDF call in an `array_sort` comparator takes both elements, e.g.
   * `(a, b) -> udf(a, b)`. Such a call has no per-element key, so it is precomputed over the cross
   * product of pairs rather than per element.
   */
  private def comparatorTakesBothElements(function: Expression): Boolean = function match {
    case LambdaFunction(body, Seq(left: NamedLambdaVariable, right: NamedLambdaVariable), _) =>
      body.exists {
        case udf: PythonUDF if PythonUDF.isElementwiseRewritableUDF(udf) =>
          def reads(id: ExprId) = udf.exists {
            case v: NamedLambdaVariable => v.exprId == id
            case _ => false
          }
          reads(left.exprId) && reads(right.exprId)
        case _ => false
      }
    case _ => false
  }

  /**
   * Rewrites one higher-order function whose own lambda holds a rewritable Python UDF. The generic
   * path reads arguments, lambdas and parameter roles off the [[HigherOrderFunction]] API and
   * rebuilds with `withNewChildren` (children are `arguments` then `functions`), naming a concrete
   * class only where a shape cannot be inferred otherwise (`ArraySort`'s comparator, and the
   * result-type traits telling `ArrayFilter` from `ArrayTransform`). A pairwise `array_sort`
   * comparator, whose single call takes both elements, needs its own path.
   *
   * Applied by `rewriteNest` to every function in a validated nest, innermost first. Unlike the
   * nest-root gate in `apply`, `liftableHof` here does not re-check free lambda variables: within
   * an already-validated nest an inner function iterating an enclosing variable is expected, and
   * `rewriteNest` guarantees the enclosing function re-lifts whatever an inner step leaves in an
   * argument position.
   */
  private val rewriteOne: PartialFunction[Expression, Expression] = {
    case sort @ ArraySort(_, function, _)
        if liftableHof(sort) && comparatorTakesBothElements(function) =>
      rewritePairwiseComparator(sort)

    // Every result-typed higher-order function is handled; anything else is left alone.
    case hof: HigherOrderFunction
        if liftableHof(hof) &&
          (hof.isInstanceOf[ResultTypeFromArgument] || hof.isInstanceOf[ResultTypeFromFunction]) =>
      rewriteMapping(hof)
  }

  /**
   * Rewrites `array_sort(arr, (a, b) -> udf(a, b))`, where one call takes both elements so there is
   * no per-element key. Precomputes the UDF over every ordered pair - an n x n matrix with
   * `udf(arr[i], arr[j])` at (i, j) - and the comparator reads it by the two elements' positions,
   * so no Python runs while sorting. Costs O(n^2) calls and memory vs. O(n) for a per-element key.
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

    // The UDF over all n*n pairs: this is just the element-wise rewrite with the pair arrays as the
    // iterated arguments, so `buildCarrier` lifts the UDF and a `transform` runs the rest of the
    // comparator body (cast, `when`, arithmetic) once per pair in the JVM.
    val pairLambda = LambdaFunction(body, Seq(leftVar, rightVar))
    val pairCarrier = buildCarrier(Seq(lefts, rights), pairLambda, Seq(leftVar, rightVar), None)
    val flatCells = ArrayTransform(
      pairCarrier.carrier, LambdaFunction(pairCarrier.body, Seq(pairCarrier.boundVar)))

    // Carry each element's position and the shared flat result array so the comparator can read
    // its pair's precomputed cell, sort, then drop them again. `flatCells` must be built here, in
    // the sort's *argument*, not inside the comparator: `ArraySort` re-evaluates the whole
    // comparator body on every comparison, and `ExtractPythonUDFs` hoists only the `PythonUDF`
    // node - the surrounding `arrays_zip`/`transform`/`flatten`/`array_repeat` that build the cells
    // would otherwise be rebuilt O(n^2) per comparison (O(n^3 log n) overall). In interpreted
    // evaluation `array_repeat` stores n references to the one computed `flatCells` array, so the
    // carry is O(n^2); a later copy of the carrier into the Unsafe format would materialize each
    // reference into O(n^3) bytes. Either way the whole pairwise path is already O(n^2) in Python
    // calls, so it is only intended for small arrays (see the config doc).
    // Also carry the row width `n` so the comparator does not re-evaluate `Size(argument)` on every
    // comparison (negligible for a column, but `argument` may be a computed expression). Like the
    // cells, it is repeated into the carrier once and read as a struct field.
    val posElem = NamedLambdaVariable("x", elementType, containsNull)
    val posIdx = NamedLambdaVariable("i", IntegerType, nullable = false)
    val cellsField = "cells"
    val sizeField = "n"
    val indexed = ArraysZip(
      Seq(
        argument,
        ArrayTransform(argument, LambdaFunction(posIdx, Seq(posElem, posIdx))),
        ArrayRepeat(flatCells, n),
        ArrayRepeat(n, n)),
      Seq(
        Literal(s"${carrierElementPrefix}0"),
        Literal(carrierIndexField),
        Literal(cellsField),
        Literal(sizeField)))
    val indexedElement = indexed.dataType.asInstanceOf[ArrayType].elementType

    // Index the flat n*n results directly: cell (i, j) is at `i * n + j`. The cells and `n` live in
    // struct fields carried by every element, so the comparator only does field reads plus an
    // `element_at`, all O(1). `element_at` is 1-based.
    val cmpLeft = NamedLambdaVariable("a", indexedElement, nullable = false)
    val cmpRight = NamedLambdaVariable("b", indexedElement, nullable = false)
    def idxOf(v: NamedLambdaVariable): Expression = GetStructField(v, 1, Some(carrierIndexField))
    val cells = GetStructField(cmpLeft, 2, Some(cellsField))
    val width = GetStructField(cmpLeft, 3, Some(sizeField))
    val comparison = ElementAt(
      cells,
      Add(Add(Multiply(idxOf(cmpLeft), width), idxOf(cmpRight)), Literal(1)),
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
  private def rewriteMapping(hof: HigherOrderFunction): Expression = {
    val lambda = hof.functions.head.asInstanceOf[LambdaFunction]
    // The result is the input elements (so the carrier is unwrapped afterwards) rather than the
    // lambda's value: `filter` / `array_sort` / `map_filter` keep the input's type.
    val isFromElements = hof.isInstanceOf[ResultTypeFromArgument]

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
        // Rebuild by the concrete function, not the result type: `transform_keys` replaces the
        // keys, `transform_values` the values, `map_filter` keeps whichever pairs survive. Keying
        // off the type would be wrong for e.g. `transform_values` on `map<string, string>`, whose
        // lambda result type equals the key type.
        val rebuild: Expression => Expression = hof match {
          case _: TransformKeys => (newKeys: Expression) => MapFromArrays(newKeys, values)
          case _: TransformValues => (newValues: Expression) => MapFromArrays(keys, newValues)
          case _: MapFilter => (kept: Expression) =>
            MapFromArrays(unwrapCarrier(kept, 0), unwrapCarrier(kept, 1))
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

    // Match lambda parameters to the arrays they iterate: leading ones map to the arrays, a
    // trailing extra one is the element index. `array_sort` is the one exception - its lambda is a
    // comparator whose two parameters are two elements of the *same* array, indistinguishable from
    // an indexed lambda by types alone (both `(T, Int)`), so it is special-cased by class here.
    val params = lambda.arguments.map(_.asInstanceOf[NamedLambdaVariable])
    val (elementVars, indexVar, alsoBind) =
      if (hof.isInstanceOf[ArraySort]) {
        (Seq(params.head), None, Seq(params.last))
      } else {
        (params.take(arrays.length), params.drop(arrays.length).headOption, Nil)
      }

    val built = buildCarrier(arrays, lambda, elementVars, indexVar, alsoBind)
    val newLambda = LambdaFunction(built.body, built.boundVar +: built.extraBoundVars)

    // Rebuild the node over the single carrier. A single-array function keeps its own class (via
    // `withNewChildren`, children being arguments then functions); a desugared map or a multi-array
    // one becomes a `transform`, or an `ArrayFilter` when the carrier must survive the filtering so
    // both key and value sides can be projected out.
    val keepsOwnNode = hof.arguments.length == 1 && !mapValued
    val iterated =
      if (keepsOwnNode) {
        hof.withNewChildren(IndexedSeq(built.carrier, newLambda)).asInstanceOf[Expression]
      } else if (isFromElements) {
        ArrayFilter(built.carrier, newLambda)
      } else {
        ArrayTransform(built.carrier, newLambda)
      }

    // A from-elements result (e.g. `filter`) is the input elements, so project them back out of the
    // carrier; for a map `rebuildResult` knows which of the key/value sides to keep.
    if (!mapValued && isFromElements) rebuildResult(unwrapCarrier(iterated, 0))
    else rebuildResult(iterated)
  }

  /**
   * True if `hof`'s single lambda holds a UDF to lift at *this* level - either directly in the body
   * or in a nested function's argument (which `hasDirectRewritableUDF` reaches, but a nested
   * function's own lambda is not this level's concern; `rewriteNest` handles that level itself).
   *
   * Does not re-check free lambda variables: `apply` enters `rewriteNest` only on a validated nest
   * root, so within the nest an inner function iterating an enclosing variable is expected, and the
   * enclosing function is guaranteed to re-lift whatever this level leaves in an argument position.
   */
  private def liftableHof(hof: HigherOrderFunction): Boolean =
    hof.functions.length == 1 && (hof.functions.head match {
      case LambdaFunction(body, args, _) =>
        hasDirectRewritableUDF(body) && args.forall(_.isInstanceOf[NamedLambdaVariable])
      case _ => false
    })

  /**
   * Whether `body` holds a rewritable UDF belonging to *this* lambda. A nested function's lambda is
   * skipped (its UDF reads that lambda's variable), but its *arguments* are not: in
   * `transform(arr, x -> transform(udf(x), y -> y))`, `udf(x)` is in the inner argument and lifts
   * onto `arr`.
   */
  private def hasDirectRewritableUDF(body: Expression): Boolean = body match {
    case e if PythonUDF.isElementwiseRewritableUDF(e) => true
    case hof: HigherOrderFunction => hof.arguments.exists(hasDirectRewritableUDF)
    case e => e.children.exists(hasDirectRewritableUDF)
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
    val liftableUDFs = collectLiftableUDFs(body)

    // With more than one argument the arrays may be ragged (`zip_with` / `map_zip_with` pad with
    // nulls), so flattening them independently would misalign the elements. Projecting each out of
    // one common `arrays_zip` pads them to the same per-row length, which the positional rewrite
    // requires.
    val alignedArguments =
      if (arguments.length > 1) {
        val names = arguments.indices.map(i => s"$carrierElementPrefix$i")
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

    // One lifted array UDF per distinct call. `arrayResults` maps each original call (by `liftKey`)
    // to the array holding its per-element results, so a nested call `f(g(x))` and the carrier
    // lookups can find it. Deterministic calls that lift to the *same* function over the *same*
    // array arguments share one lifted UDF: a key-form comparator's `udf(a)` and `udf(b)`
    // canonicalize differently (`a` != `b`) but both read the whole array, so without this the
    // Python function would run 2n times instead of n. Nondeterministic calls stay distinct (their
    // signature is the lifted node itself, carrying a distinct `resultId`), matching `liftKey`.
    var arrayResults = Map.empty[Expression, Expression]
    val distinctLifted = scala.collection.mutable.ArrayBuffer.empty[PythonUDF]
    val ordinalBySignature = scala.collection.mutable.HashMap.empty[Expression, Int]
    val udfFieldByKey = scala.collection.mutable.LinkedHashMap.empty[Expression, Int]
    liftableUDFs.foreach { udf =>
      // `overArray` turns each argument into an `array<T>` aligned with the iterated array, so the
      // worker flattens every one exactly once (no per-argument shape to track). A keyword argument
      // keeps its `NamedArgumentExpression` wrapper as a direct child of the lifted UDF - only its
      // value is lifted - so the runner still derives the kwargs mapping from the direct children.
      val arrayArgs = udf.children.map {
        case NamedArgumentExpression(key, value) =>
          NamedArgumentExpression(
            key, overArray(value, alignedArguments.head, arrayOfVar, lambdaExprIds, arrayResults))
        case child =>
          overArray(child, alignedArguments.head, arrayOfVar, lambdaExprIds, arrayResults)
      }
      // Each lift wraps the arguments in exactly one more `array` level. Lifting a base UDF gives
      // depth 1; re-lifting an already-lifted element-wise UDF (a UDF from a *nested* lambda,
      // lifted once onto the inner variable and now again onto the enclosing array) adds one more
      // level, so the worker flattens `depth` levels down to the scalar element and re-nests them.
      val newDepth =
        if (PythonEvalType.isElementwiseUDF(udf.evalType)) udf.elementwiseNestingDepth + 1 else 1
      val lifted = PythonUDF(
        udf.name,
        udf.func,
        // The wrapper returns one element per input element, i.e. one array level on top of the
        // UDF's previous return type. Elements may be null (the UDF can return null), hence
        // containsNull = true.
        ArrayType(udf.dataType, containsNull = true),
        arrayArgs,
        // Each rewritable flavor lifts to its own element-wise eval type so the worker keeps that
        // flavor's batching contract (pickle row-at-a-time, pandas Series, Arrow Array, or an
        // iterator of batches); an already-lifted type maps to itself. See
        // `PythonUDF.liftedElementwiseEvalType`.
        PythonUDF.liftedElementwiseEvalType(udf.evalType),
        udf.udfDeterministic,
        elementwiseNestingDepth = newDepth)
      val signature: Expression = if (udf.udfDeterministic) lifted.canonicalized else lifted
      val ordinal = ordinalBySignature.getOrElseUpdate(signature, {
        val o = distinctLifted.length
        distinctLifted += lifted
        o
      })
      arrayResults += (liftKey(udf) -> distinctLifted(ordinal))
      udfFieldByKey += (liftKey(udf) -> ordinal)
    }
    val liftedArrays = distinctLifted.toSeq

    // The carrier: the original arrays first, then one field per lifted UDF, then the index.
    val carrierFields = alignedArguments ++ liftedArrays ++ indexArray.toSeq
    val carrierNames =
      arguments.indices.map(i => s"$carrierElementPrefix$i") ++
        liftedArrays.indices.map(i => s"$carrierUDFFieldPrefix$i") ++
        indexArray.map(_ => carrierIndexField).toSeq
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

    // Rewrite the body. This must be top-down: a UDF call is matched by its canonicalized form,
    // and rewriting its arguments first (a variable becoming a struct field read) would change
    // that form so the call no longer matches and would be left inside the lambda. Replacing the
    // call outright also stops the traversal descending into arguments that no longer exist.
    def readerFor(v: NamedLambdaVariable, udfOrdinal: Option[Int]): Expression = {
      val base = extraVarOf.getOrElse(v.exprId, boundVar)
      udfOrdinal match {
        case Some(u) =>
          GetStructField(base, arguments.length + u, Some(s"$carrierUDFFieldPrefix$u"))
        case None =>
          val ordinal = fieldOfVar(v.exprId)
          GetStructField(base, ordinal, Some(carrierNames(ordinal)))
      }
    }

    val rewrittenBody = body.transformDown {
      case udf: PythonUDF if udfFieldByKey.contains(liftKey(udf)) =>
        val ordinal = udfFieldByKey(liftKey(udf))
        // A UDF over a comparator's right-hand element must read that element's key, so the
        // struct field is read through whichever bound variable the call's own arguments used.
        val side = udf.collectFirst {
          case v: NamedLambdaVariable if extraVarOf.contains(v.exprId) => v
        }
        side match {
          case Some(v) => readerFor(v, Some(ordinal))
          case None =>
            GetStructField(boundVar, arguments.length + ordinal,
              Some(s"$carrierUDFFieldPrefix$ordinal"))
        }
      case v: NamedLambdaVariable if fieldOfVar.contains(v.exprId) => readerFor(v, None)
      case v: NamedLambdaVariable if extraVarOf.contains(v.exprId) =>
        // A comparator's right-hand element itself, read through its own bound variable.
        GetStructField(extraVarOf(v.exprId), 0, Some(carrierNames.head))
    }

    Carrier(carrier, rewrittenBody, boundVar, extraBoundVars)
  }

  /**
   * Collects the Python UDF calls directly in `body` that must be lifted, innermost first.
   *
   * Every rewritable UDF directly in the lambda body is lifted, even one whose arguments do not
   * read the lambda variable (`transform(arr, _ -> f(lit(10)))`). It cannot stay inside the lambda,
   * and lifting it - `overArray` repeats a constant/outer-column argument into an aligned array -
   * gives it the lambda's own call domain: once per element, and zero times for a null or empty
   * array. Leaving it to [[ExtractPythonUDFs]] would instead call it once per row, including rows
   * whose array is null or empty where the lambda never runs.
   */
  private def collectLiftableUDFs(body: Expression): Seq[PythonUDF] = {
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
        case udf: PythonUDF if PythonUDF.isElementwiseRewritableUDF(udf) =>
          collected += udf
        case _ =>
      }
    }
    visit(body)
    // Deduplicate identical calls so the same UDF is evaluated once per array. Nondeterministic
    // calls are kept distinct (see `liftKey`).
    val seen = scala.collection.mutable.LinkedHashMap.empty[Expression, PythonUDF]
    collected.result().foreach(udf => seen.getOrElseUpdate(liftKey(udf), udf))
    seen.values.toSeq
  }

  private def readsLambdaVariable(e: Expression, lambdaExprIds: Set[ExprId]): Boolean =
    e.exists {
      case v: NamedLambdaVariable => lambdaExprIds.contains(v.exprId)
      case _ => false
    }

  /**
   * The key that decides whether two UDF calls are "the same call" for lifting. A deterministic
   * call is deduplicated by canonical form, so an identical call is evaluated once. A
   * nondeterministic call must stay distinct - `transform(arr, x -> f(x) + f(x))` calls `f` twice
   * and each call may return a different value - so it is keyed by its own `resultId`-bearing node
   * (`canonicalized` erases `resultId`, which would collapse the two calls into one).
   */
  private def liftKey(udf: PythonUDF): Expression =
    if (udf.udfDeterministic) udf.canonicalized else udf

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
   *
   * An already-lifted UDF call nested inside a composite argument (`f(g(x) + 1)`, `f(-g(x))`) is
   * handled by the last case too: each such call is replaced by a synthetic variable standing for
   * that call's aligned array result, which is then zipped in like any element array. Leaving the
   * raw `g` inside the generated `transform` lambda would put a `PythonUDF` back inside a lambda -
   * `ExtractPythonUDFs` would then extract a `g` whose child is a `NamedLambdaVariable` (the
   * SPARK-48706 failure mode), so this substitution is for correctness, not just efficiency.
   */
  private def overArray(
      child: Expression,
      firstArgument: Expression,
      arrayOfVar: Map[ExprId, Expression],
      lambdaExprIds: Set[ExprId],
      arrayResults: Map[Expression, Expression]): Expression = child match {
    case v: NamedLambdaVariable if arrayOfVar.contains(v.exprId) => arrayOfVar(v.exprId)
    case udf: PythonUDF if arrayResults.contains(liftKey(udf)) =>
      arrayResults(liftKey(udf))
    case e =>
      // Replace each already-lifted nested UDF call with a synthetic variable standing for that
      // call's aligned array result, then fold those arrays into the variable-to-array map so the
      // logic below treats them exactly like element variables. This keeps a lifted UDF buried in
      // a composite argument (`f(g(x) + 1)`) from being left as a raw UDF inside a lambda.
      val nestedVars = scala.collection.mutable.LinkedHashMap.empty[Expression, NamedLambdaVariable]
      val expr = e.transformUp {
        case u: PythonUDF if arrayResults.contains(liftKey(u)) =>
          nestedVars.getOrElseUpdate(liftKey(u), {
            val arrType = arrayResults(liftKey(u)).dataType.asInstanceOf[ArrayType]
            NamedLambdaVariable("g", arrType.elementType, arrType.containsNull)
          })
      }
      val fullArrayOf = arrayOfVar ++
        nestedVars.map { case (key, v) => v.exprId -> arrayResults(key) }

      if (!readsLambdaVariable(expr, fullArrayOf.keySet)) {
        // Independent of the element (an outer column or constant): repeat it into an array aligned
        // with the iterated array, so every UDF argument is a single-level array the worker
        // flattens the same way. `transform(arr, _ -> e)` keeps the value constant, matching shape.
        val arrType = firstArgument.dataType.asInstanceOf[ArrayType]
        val v = NamedLambdaVariable("x", arrType.elementType, arrType.containsNull)
        ArrayTransform(firstArgument, LambdaFunction(expr, Seq(v)))
      } else {
        // An expression over the element(s) and/or nested results, e.g. `udf(x * 2)` or
        // `f(g(x) + 1)`. Compute it for every element with a native `transform`, which stays inside
        // the JVM. Multi-argument shapes need the values side by side, so zip them first.
        val arrays = fullArrayOf.values.toSeq.distinct
        if (arrays.length == 1) {
          val arr = arrays.head
          val elemType = arr.dataType.asInstanceOf[ArrayType]
          val v = NamedLambdaVariable("x", elemType.elementType, elemType.containsNull)
          val substituted = expr.transformUp {
            case old: NamedLambdaVariable if fullArrayOf.contains(old.exprId) => v
          }
          ArrayTransform(arr, LambdaFunction(substituted, Seq(v)))
        } else {
          // Zip every array the expression may read, then project the fields it needs.
          val names = arrays.indices.map(i => s"$carrierElementPrefix$i")
          val zipped = ArraysZip(arrays, names.map(Literal(_)))
          val structType = zipped.dataType.asInstanceOf[ArrayType].elementType
          val v = NamedLambdaVariable("z", structType, nullable = false)
          val ordinalOf = fullArrayOf.map { case (id, arr) => id -> arrays.indexOf(arr) }
          val substituted = expr.transformUp {
            case old: NamedLambdaVariable if ordinalOf.contains(old.exprId) =>
              GetStructField(v, ordinalOf(old.exprId), Some(names(ordinalOf(old.exprId))))
          }
          ArrayTransform(zipped, LambdaFunction(substituted, Seq(v)))
        }
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
        GetStructField(v, ordinal, Some(s"$carrierElementPrefix$ordinal")), Seq(v)))
  }

  private val carrierElementPrefix = "c"
  private val carrierUDFFieldPrefix = "u"
  private val carrierIndexField = "idx"
}
