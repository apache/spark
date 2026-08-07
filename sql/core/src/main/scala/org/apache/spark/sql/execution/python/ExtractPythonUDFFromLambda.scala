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
 * `plus_one_over_array` is the same function re-typed as `array<T> => array<R>` and run with
 * [[PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF]]. The array-at-a-time behaviour lives in the Python
 * worker: it flattens each list column once, calls the function over all elements of the batch, and
 * re-nests by the input's offsets - one row in, one row out, one Python round trip per batch.
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
 *
 * `CheckAnalysis` still rejects what cannot be rewritten:
 *  - a UDF in a *nested* lambda, `transform(arr, i -> transform(i, x -> f(x)))`: the inner array
 *    `i` is not a real column. (A UDF in a nested *argument*, `transform(arr, x ->
 *    transform(udf(x), y -> y))`, is fine - `udf(x)` lifts onto `arr`.)
 *  - a UDF in `aggregate` / `reduce`: the fold is sequential, so the UDF sees earlier steps'
 *    outputs, not array elements.
 *  - a pandas UDF: it takes a `Series`, not one value per call.
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
   * Whether the result is built from the input elements (so the carrier must be unwrapped) rather
   * than from the lambda's value. Read straight off the Catalyst result-type traits:
   * `filter` / `array_sort` / `map_filter` keep the input's type ([[ResultTypeFromArgument]]);
   * `transform` and friends follow the lambda ([[ResultTypeFromFunction]]).
   */
  private def resultFromElements(hof: HigherOrderFunction): Boolean =
    hof.isInstanceOf[ResultTypeFromArgument]

  /**
   * Whether one UDF call in an `array_sort` comparator takes both elements, e.g.
   * `(a, b) -> udf(a, b)`. Such a call has no per-element key, so it is precomputed over the cross
   * product of pairs rather than per element.
   */
  private def comparatorTakesBothElements(function: Expression): Boolean = function match {
    case LambdaFunction(body, Seq(left: NamedLambdaVariable, right: NamedLambdaVariable), _) =>
      body.exists {
        case udf: PythonUDF if isRewritableUDF(udf) =>
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
   * Rewrites one higher-order function whose lambda holds a rewritable Python UDF. The generic path
   * never names a concrete class: it reads arguments, lambdas and parameter roles off the
   * [[HigherOrderFunction]] API and rebuilds with `withNewChildren` (children are `arguments` then
   * `functions`). Only a pairwise `array_sort` comparator needs a separate path.
   */
  private val rewrite: PartialFunction[Expression, Expression] = {
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
  private def rewriteMapping(hof: HigherOrderFunction): Expression = {
    val lambda = hof.functions.head.asInstanceOf[LambdaFunction]
    val fromElements = resultFromElements(hof)

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
        // `map_filter` keeps whichever pairs survive; `transform_keys` replaces the keys and
        // `transform_values` the values, told apart by whether the result key type is the lambda's.
        val rebuild: Expression => Expression =
          if (fromElements) { (kept: Expression) =>
            MapFromArrays(unwrapCarrier(kept, 0), unwrapCarrier(kept, 1))
          } else if (hof.dataType.asInstanceOf[MapType].keyType == lambda.dataType) {
            (newKeys: Expression) => MapFromArrays(newKeys, values)
          } else {
            (newValues: Expression) => MapFromArrays(keys, newValues)
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
    // trailing extra one is the element index. An `array_sort` comparator is the exception - its
    // two parameters are two elements of the *same* array, indistinguishable from an indexed lambda
    // by types alone (both `(T, Int)`), so it is recognized by class.
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
      } else if (fromElements) {
        ArrayFilter(built.carrier, newLambda)
      } else {
        ArrayTransform(built.carrier, newLambda)
      }

    // A from-elements result (e.g. `filter`) is the input elements, so project them back out of the
    // carrier; for a map `rebuildResult` knows which of the key/value sides to keep.
    if (!mapValued && fromElements) rebuildResult(unwrapCarrier(iterated, 0))
    else rebuildResult(iterated)
  }

  /**
   * True if `hof`'s single lambda holds a UDF belonging to *this* lambda (not a nested function's
   * lambda). A UDF in a nested lambda is rejected by `CheckAnalysis`, so it is never matched here.
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
    case e if isRewritableUDF(e) => true
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
    val liftableUDFs = collectLiftableUDFs(body, lambdaExprIds)

    // With more than one argument the arrays may be ragged (`zip_with` / `map_zip_with` pad with
    // nulls), so flattening them independently would misalign the elements. Projecting each out of
    // one common `arrays_zip` pads them to the same per-row length, which the positional rewrite
    // requires.
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
      // `overArray` turns each argument into an `array<T>` aligned with the iterated array, so the
      // worker flattens every one exactly once (no per-argument shape to track).
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
