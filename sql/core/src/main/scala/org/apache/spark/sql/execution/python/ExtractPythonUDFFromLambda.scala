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
 * All twelve higher-order functions that take a lambda are handled:
 *
 *  - `transform`, `exists`, `forall` return the lambda's own value, so the carrier replaces the
 *    argument directly;
 *  - `filter`'s lambda is a predicate, so the original elements are projected back out of the
 *    carrier afterwards;
 *  - `zip_with` zips both arrays into one carrier, which collapses it to the single-array form;
 *  - `aggregate` / `reduce` precompute over `merge`'s element, and a UDF in `finish` is applied to
 *    the fold's result, guarded so a fold over a null array stays null;
 *  - `array_sort` precomputes the UDF per element as a sort key that the JVM comparator compares;
 *  - `transform_keys`, `transform_values`, `map_filter` and `map_zip_with` are desugared into the
 *    array case over `map_keys` / `map_values` and rebuilt with `map_from_arrays`.
 *
 * Nested higher-order functions work too. A UDF in `transform(arr, i -> transform(i, x -> f(x)))`
 * cannot be lifted in one pass, because the inner array is the outer lambda's variable and so does
 * not exist outside it. Rewriting innermost-first and repeating to a fixed point resolves this: the
 * inner rewrite turns the inner lambda's UDF into a `PythonUDF` over `i`, which the outer pass then
 * lifts onto `arr` as an array-of-arrays argument.
 *
 * Shapes that genuinely cannot be rewritten are left untouched here and continue to be rejected by
 * `CheckAnalysis`:
 *
 *  - a UDF reading `aggregate`'s accumulator: the fold is sequential (step n depends on step n-1),
 *    so there is no array to precompute over;
 *  - a UDF in an `array_sort` comparator that receives *both* elements in one call: there are
 *    O(n log n) pairwise comparisons and no array to precompute them over. Returning a per-element
 *    sort key instead is the supported form;
 *  - a pandas UDF: it receives a `Series` rather than one value per call, so the element-wise
 *    rewrite would change its meaning.
 */
object ExtractPythonUDFFromLambda extends Rule[LogicalPlan] {

  /**
   * A scalar Python UDF that this rule knows how to lift out of a lambda. Shared with
   * `CheckAnalysis` so that the shapes analysis lets through are exactly those rewritten here.
   */
  private def isRewritableUDF(e: Expression): Boolean =
    PythonUDF.isElementwiseRewritableUDF(e) || isAlreadyLifted(e)

  /**
   * An element-wise UDF this rule produced on an earlier pass.
   *
   * Such a UDF is lifted again when it turns out to sit inside a nested higher-order function: the
   * inner pass lifts `f` onto the inner array `i`, and because `i` is the outer lambda's variable
   * the result is still inside the outer lambda, so the outer pass lifts it onto `arr` as well.
   * Lifting composes by simply incrementing the flatten depth - `f` still receives one scalar per
   * call, it just sits one array deeper.
   */
  private def isAlreadyLifted(e: Expression): Boolean = e match {
    case udf: PythonUDF => udf.evalType == PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.pythonUDFInHigherOrderFunctionEnabled) {
      plan
    } else {
      plan.transformUpWithPruning(
        _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION)) {
        case p =>
          // One pass lifts the UDF out of the innermost higher-order function. A nested one
          // becomes rewritable only after that, because its argument stops referencing the inner
          // lambda's variable, so the pass is repeated until nothing is left to rewrite.
          //
          // Progress is measured by whether any liftable UDF remains rather than by comparing
          // plans: every pass mints fresh `NamedLambdaVariable` exprIds, so a plan comparison
          // would never report equality and the loop would not terminate. Each pass strictly
          // reduces the number of higher-order functions holding a liftable UDF, so this
          // terminates; the bound is a safety net against an unforeseen non-shrinking rewrite.
          var current = p
          var remaining = countRewritable(current)
          var iterations = 0
          while (remaining > 0 && iterations < MaxRewritePasses) {
            current = current.transformExpressionsUpWithPruning(
              _.containsAllPatterns(PYTHON_UDF, HIGH_ORDER_FUNCTION))(rewrite orElse rewriteMap)
            val next = countRewritable(current)
            // No progress: the remaining shapes are ones this rule cannot handle.
            if (next >= remaining) remaining = 0 else remaining = next
            iterations += 1
          }
          current
      }
    }
  }

  /**
   * How many higher-order functions in `plan` still hold a Python UDF inside a lambda. Used to
   * drive the rewrite loop: it must strictly decrease, and reaching zero means every UDF has been
   * lifted out.
   */
  private def countRewritable(plan: LogicalPlan): Int =
    plan.expressions.map { e =>
      e.collect {
        case hof: HigherOrderFunction if hof.functions.exists {
          case LambdaFunction(body, _, _) => hasDirectRewritableUDF(body)
          case _ => false
        } => hof
      }.size
    }.sum

  /**
   * Upper bound on rewrite passes, as a safety net: nesting deeper than this is not realistic, and
   * a bound guarantees the optimizer cannot loop even if a future rewrite fails to shrink the
   * count.
   */
  private val MaxRewritePasses = 32

  /** Rewrites a single higher-order function whose lambda contains a rewritable Python UDF. */
  private val rewrite: PartialFunction[Expression, Expression] = {
    // `transform`, `exists` and `forall` all return the lambda's own value, so the carrier
    // struct can simply replace the argument and the rewritten body reads the UDF result.
    case ArrayTransform(argument, function) if liftable(Seq(argument), function) =>
      withCarrier(Seq(argument), function)((carrier, lambda) =>
        ArrayTransform(carrier, lambda))

    case ArrayExists(argument, function, followThreeValuedLogic)
        if liftable(Seq(argument), function) =>
      withCarrier(Seq(argument), function)((carrier, lambda) =>
        ArrayExists(carrier, lambda, followThreeValuedLogic))

    case ArrayForAll(argument, function) if liftable(Seq(argument), function) =>
      withCarrier(Seq(argument), function)((carrier, lambda) =>
        ArrayForAll(carrier, lambda))

    // `filter`'s lambda is a predicate, so its result is not the value we want: the surviving
    // *input* elements are. Filter over the carrier, then project the original element back out.
    case ArrayFilter(argument, function) if liftable(Seq(argument), function) =>
      val filtered = withCarrier(Seq(argument), function)((carrier, lambda) =>
        ArrayFilter(carrier, lambda))
      unwrapCarrier(filtered, 0)

    // `zip_with` walks two arrays at once. Zipping both into the carrier reduces it to the
    // single-array form. `arrays_zip` pads the shorter side with nulls, which is exactly what
    // `zip_with` does itself.
    case ZipWith(left, right, function) if liftable(Seq(left, right), function) =>
      withCarrier(Seq(left, right), function)((carrier, lambda) =>
        ArrayTransform(carrier, lambda))

    // `array_sort`'s comparator sees two elements at once, so its Python part cannot be evaluated
    // pairwise. What can be precomputed is the UDF applied to each element, giving a sort key the
    // JVM comparator then compares. Each side of the comparison must read the key belonging to
    // its own element, so the carrier is built once and both comparator parameters are bound to
    // it.
    case ArraySort(argument, function, allowNull)
        if liftable(Seq(argument), function) &&
          !PythonUDF.comparatorTakesBothElements(function) =>
      val sorted = withComparatorCarrier(argument, function) { (carrier, lambda) =>
        ArraySort(carrier, lambda, allowNull)
      }
      unwrapCarrier(sorted, 0)

    // `aggregate` / `reduce`. Only `merge`'s element argument iterates the array, so a UDF there
    // is precomputable. A UDF reading the accumulator is not, and is left for CheckAnalysis.
    case a @ ArrayAggregate(argument, zero, merge, finish)
        if liftableFold(argument, merge, finish) =>
      rewriteFold(a)
  }

  // ---------------------------------------------------------------------------------------------
  // Map-valued higher-order functions.
  //
  // Each is desugared to the array case: split the map into its key and value arrays, rewrite
  // over those, and rebuild a map. Written as separate cases below because the reassembly differs.
  // ---------------------------------------------------------------------------------------------

  /**
   * Rewrites the map family. Kept apart from [[rewrite]] only to keep each method a readable size;
   * both are applied by the same traversal.
   */
  private val rewriteMap: PartialFunction[Expression, Expression] = {
    // `transform_keys(m, (k, v) -> body)`: rebuild a map from the transformed keys and the
    // original values.
    case TransformKeys(argument, function) if liftableMap(argument, function) =>
      val keys = MapKeys(argument)
      val values = MapValues(argument)
      val newKeys = withCarrier(Seq(keys, values), function)((carrier, lambda) =>
        ArrayTransform(carrier, lambda))
      MapFromArrays(newKeys, values)

    // `transform_values(m, (k, v) -> body)`: keys are unchanged, values are transformed.
    case TransformValues(argument, function) if liftableMap(argument, function) =>
      val keys = MapKeys(argument)
      val values = MapValues(argument)
      val newValues = withCarrier(Seq(keys, values), function)((carrier, lambda) =>
        ArrayTransform(carrier, lambda))
      MapFromArrays(keys, newValues)

    // `map_filter(m, (k, v) -> pred)`: filter the zipped key/value pairs, then rebuild.
    case MapFilter(argument, function) if liftableMap(argument, function) =>
      val keys = MapKeys(argument)
      val values = MapValues(argument)
      val kept = withCarrier(Seq(keys, values), function)((carrier, lambda) =>
        ArrayFilter(carrier, lambda))
      MapFromArrays(unwrapCarrier(kept, 0), unwrapCarrier(kept, 1))

    // `map_zip_with(l, r, (k, lv, rv) -> body)`: the visited key set is the union of both maps'
    // keys, and each map is looked up per key. `element_at` yields null for a key missing from one
    // side, which is exactly `map_zip_with`'s own behaviour.
    case MapZipWith(left, right, function)
        if left.dataType.isInstanceOf[MapType] && right.dataType.isInstanceOf[MapType] &&
          liftable(Seq(left, right), function) =>
      val keys = ArrayUnion(MapKeys(left), MapKeys(right))
      val keyType = keys.dataType.asInstanceOf[ArrayType]
      def valuesFor(map: Expression): Expression = {
        val k = NamedLambdaVariable("k", keyType.elementType, keyType.containsNull)
        ArrayTransform(
          keys,
          LambdaFunction(ElementAt(map, k, None, failOnError = false), Seq(k)))
      }
      val newValues = withCarrier(
        Seq(keys, valuesFor(left), valuesFor(right)), function)((carrier, lambda) =>
        ArrayTransform(carrier, lambda))
      MapFromArrays(keys, newValues)
  }

  /**
   * True if `function`'s lambda holds a UDF this rule can lift onto `arguments`.
   *
   * The UDF must belong to *this* lambda, not to a nested higher-order function inside it. For
   * `transform(arr, i -> transform(i, x -> udf(x)))` the outer lambda transitively contains the
   * UDF, but lifting it onto `arr` is wrong: the UDF reads the inner lambda's variable, so it must
   * be lifted onto `i` by the inner rewrite first. Rewriting the outer function at that point
   * would build a carrier that precomputes nothing and leave the UDF where it was.
   */
  private def liftable(arguments: Seq[Expression], function: Expression): Boolean =
    function match {
      case LambdaFunction(body, _, _) => hasDirectRewritableUDF(body)
      case _ => false
    }

  /**
   * Whether `body` holds a rewritable UDF that belongs to *this* lambda rather than to a nested
   * higher-order function's lambda.
   *
   * A nested function's own lambda is skipped: its UDF reads that lambda's variable and must be
   * lifted by the inner rewrite first. Its *arguments* are not skipped, because they are evaluated
   * outside the nested lambda and so are part of this lambda's body. That is exactly where an
   * earlier pass leaves a lifted UDF - `transform(arrays_zip(i, f_over(i)), s -> s.u0)` - and
   * finding it there is what lets the outer pass lift it one level further.
   */
  private def hasDirectRewritableUDF(body: Expression): Boolean = body match {
    case e if isRewritableUDF(e) => true
    case hof: HigherOrderFunction => hof.arguments.exists(hasDirectRewritableUDF)
    case e => e.children.exists(hasDirectRewritableUDF)
  }


  private def liftableMap(argument: Expression, function: Expression): Boolean =
    argument.dataType.isInstanceOf[MapType] && liftable(Seq(argument), function)

  /**
   * `aggregate` is rewritable when the UDFs in `merge` read only the element (not the accumulator)
   * and/or a UDF appears in `finish`.
   */
  private def liftableFold(
      argument: Expression, merge: Expression, finish: Expression): Boolean = {
    val mergeLiftable = liftable(Seq(argument), merge) && !mergeReadsAccumulator(merge)
    val finishLiftable = finish match {
      case LambdaFunction(body, _, _) => body.exists(isRewritableUDF)
      case _ => false
    }
    mergeLiftable || finishLiftable
  }

  private def mergeReadsAccumulator(merge: Expression): Boolean = merge match {
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
   * Rewrites `aggregate` / `reduce`.
   *
   * A UDF on `merge`'s element is precomputed over the array, exactly as for `transform`. A UDF in
   * `finish` needs no array at all: `finish` runs once on the final accumulator, which is an
   * ordinary column by then, so the UDF is applied to the fold's result.
   *
   * The `finish` case has a subtlety. A fold over a `null` array is `null`, and Spark does not
   * evaluate `finish` for it. Applying the UDF outside the fold *would* call it on that null, and
   * a null-unaware Python function then raises where plain Spark returns null. Keeping `finish`
   * as the identity and applying the UDF to the fold's result would change that, so the UDF stays
   * inside a `finish` lambda whose accumulator is a real column: the fold itself, not a lambda
   * variable. That keeps Spark's own null handling for the fold, and the UDF is only reached when
   * the fold produced a value.
   */
  private def rewriteFold(agg: ArrayAggregate): Expression = {
    val ArrayAggregate(argument, zero, merge, finish) = agg

    // Step 1: precompute the element UDFs of `merge`, if any.
    val withMergeRewritten =
      if (liftable(Seq(argument), merge) && !mergeReadsAccumulator(merge)) {
        val LambdaFunction(_, Seq(accVar: NamedLambdaVariable, elementVar), _) = merge
        val built = buildCarrier(
          Seq(argument), merge, Seq(elementVar.asInstanceOf[NamedLambdaVariable]), None)
        ArrayAggregate(
          built.carrier,
          zero,
          LambdaFunction(built.body, Seq(accVar, built.boundVar)),
          finish)
      } else {
        agg
      }

    // Step 2: `finish` runs once on the final accumulator, which is an ordinary column by then -
    // not an element of an array - so the UDF there is not element-wise at all. Turning the fold
    // into a one-element array lets the same element-wise machinery apply it exactly once.
    //
    // The null handling is the subtle part. A fold over a null array is null, and Spark does not
    // evaluate `finish` for it, so a null-unaware Python function must not be called on that
    // null. `filter` on the single-element array drops it when the fold is null, leaving an empty
    // array that `transform` never calls the UDF for; reading element 0 of an empty array then
    // yields null, matching Spark. (Wrapping in `when(fold IS NOT NULL, udf(fold))` would *not*
    // work: Spark evaluates both branches, so the UDF would still see the null.)
    withMergeRewritten match {
      case ArrayAggregate(arg, z, m, LambdaFunction(body, Seq(accVar), _))
          if body.exists(isRewritableUDF) =>
        val fold = ArrayAggregate(arg, z, m, LambdaFunction.identity)
        val elem = NamedLambdaVariable("acc", fold.dataType, nullable = true)
        val nonNullFold = ArrayFilter(
          CreateArray(Seq(fold)), LambdaFunction(IsNotNull(elem), Seq(elem)))
        val applied = ArrayTransform(
          nonNullFold,
          LambdaFunction(
            body.transformUp {
              case v: NamedLambdaVariable if v.exprId == accVar.exprId => elem
            },
            Seq(elem)))
        GetArrayItem(applied, Literal(0), failOnError = false)
      case other => other
    }
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
      val arrayArgs = udf.children.map { child =>
        overArray(child, alignedArguments.head, arrayOfVar, lambdaExprIds, arrayResults)
      }
      // Each argument that became an array gains one flatten level; one that stayed a per-row
      // value is broadcast instead, which depth 0 marks. Lifting an already-lifted UDF (a nested
      // higher-order function) composes by incrementing its existing depths rather than resetting
      // them, so the user's scalar function still gets one value per call.
      val previousDepths =
        if (udf.elementwiseDepths.nonEmpty) udf.elementwiseDepths
        else Seq.fill(udf.children.length)(0)
      val depths = udf.children.zip(arrayArgs).zip(previousDepths).map {
        case ((child, arrayArg), previous) =>
          if (arrayArg.fastEquals(child) && !readsLambdaVariable(child, lambdaExprIds)) previous
          else previous + 1
      }
      val lifted = PythonUDF(
        udf.name,
        udf.func,
        // The wrapper returns one element per input element, i.e. one array level on top of what
        // this UDF already returned. Elements may be null (the UDF can return null), hence
        // containsNull = true.
        ArrayType(udf.dataType, containsNull = true),
        arrayArgs,
        PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
        udf.udfDeterministic,
        elementwiseDepths = depths)
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
   * expression over whole arrays.
   *
   *  - a lambda variable becomes the array it stands for;
   *  - an already-lifted nested UDF call becomes its array result;
   *  - an expression independent of the lambda is passed through, to be broadcast per element;
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
      // Independent of the element: pass through and let the worker broadcast it.
      e
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
