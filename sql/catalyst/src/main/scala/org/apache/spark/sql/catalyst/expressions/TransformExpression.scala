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

package org.apache.spark.sql.catalyst.expressions

import java.util.concurrent.ConcurrentHashMap

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SparkException, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{ARGS, CLASS_NAME, ERROR, FUNCTION_NAME, FUNCTION_PARAM}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, UninitializedPhysicalType}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.connector.expressions.{Literal => V2Literal, LiteralValue}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, IntegerType, MapType, StructType, UpCastRule, UserDefinedType}

/**
 * The identity of a partition transform: what [[TransformExpression.isSameFunction]] compares, and
 * what [[TransformExpression.reducedWith]] stores to name the key space a reduce produced. Both
 * questions are answered by this one value, so "same function" and "same reduced key space" cannot
 * disagree about which transforms are the same.
 *
 * It carries no exprIds (a column argument is [[TransformFunctionId.ArgumentShape.ColumnRef]], not
 * the attribute, and an expression argument is normalized), so an expression can hold one in a
 * plain field and expression equality still answers the same after canonicalization.
 *
 * @param canonicalName the transform function's canonical name
 * @param argumentShapes one entry per argument, in order. Positions are part of the identity, so
 *                       `truncate(id, 2)` and `truncate(2, store_id)` are different transforms, and
 *                       a nested transform is compared recursively, so `bucket(4, years(c))` and
 *                       `bucket(4, days(c))` are too.
 */
case class TransformFunctionId(
    canonicalName: String,
    argumentShapes: Seq[TransformFunctionId.ArgumentShape])

object TransformFunctionId {
  /** The shape of one argument of a partition transform, as its identity sees it. */
  sealed trait ArgumentShape

  object ArgumentShape {
    /** A literal parameter, e.g. the bucket count of `bucket` or the width of `truncate`. */
    case class Param(value: Literal) extends ArgumentShape

    /** A nested transform, compared by its own identity, recursively. */
    case class Nested(id: TransformFunctionId) extends ArgumentShape

    /**
     * A bare column, an [[Attribute]]. Which column is not part of the identity -- the two sides
     * of a join reference different columns by construction, and `KeyedShuffleSpec.keyPositions`
     * reconciles them separately.
     */
    case object ColumnRef extends ArgumentShape

    /**
     * Any other argument over one column, such as `c + 1` or a struct field `s.a`, with that column
     * replaced by a placeholder. Two such arguments match when they are the same function of their
     * columns, which is the shape `KeyedShuffleSpec` pairs on.
     */
    case class Expr(normalized: Expression) extends ArgumentShape
  }
}

/**
 * Represents a partition transform expression, for instance, `bucket`, `days`, `years`, etc.
 *
 * @param function the transform function itself. Spark will use it to decide whether two
 *                 partition transform expressions are compatible.
 * @param reducedWith the transform this one's partition keys were reduced together with, if they
 *                    were. A storage-partitioned join can reduce both sides' keys onto a common
 *                    key space, and when both sides reduce that space is a third one that neither
 *                    transform describes. The keys become `r1(f1(x))` = `r2(f2(x))`, so this
 *                    expression computes neither their values nor their data type. Set, it says
 *                    exactly that, and names the other half of the pairing that produced the space,
 *                    which is the only thing that tells two such key spaces apart. See
 *                    `KeyedShuffleSpec.reducersBothWays`, which is the only producer.
 * @param fromBucketTransform whether this was resolved from a V2 `bucket(numBuckets, col)`
 *                            transform. Only a pair of such transforms is offered the deprecated
 *                            `ReducibleFunction.reducer(int, ReducibleFunction, int)`, which is
 *                            documented for the bucket function. Nothing else tells a bucket
 *                            apart: a `truncate(width, col)` has the same layout, and the bound
 *                            function may report any name. Not part of the transform's identity
 *                            (`functionId`), nor of its plan string, but part of case-class
 *                            equality, since a marked and an unmarked transform are offered
 *                            different reducers. Every production transform is resolved by
 *                            `V2ExpressionUtils`, which sets it, and rebuilt with `copy`, which
 *                            keeps it.
 */
case class TransformExpression(
    function: BoundFunction,
    children: Seq[Expression],
    reducedWith: Option[TransformFunctionId] = None,
    fromBucketTransform: Boolean = false) extends Expression {

  override def nullable: Boolean = true

  /** The function and the children, and `reducedWith` when it is set. */
  override protected def stringArgs: Iterator[Any] =
    Iterator(function, children) ++ Option.when(reducedWith.isDefined)(reducedWith).iterator

  /**
   * Extract literal children (constant parameters) from this transform. These are constant
   * arguments like the bucket count in `bucket(n, col)` or the width in `truncate(col, width)`.
   * Positions are dropped, so this is for handing parameters to a connector reducer, NOT for
   * identity -- see [[functionId]] and [[TransformFunctionId.argumentShapes]].
   */
  private lazy val literalChildren: Seq[Literal] =
    children.collect { case l: Literal => l }

  /**
   * Rewrites this transform's column arguments with `rewriteColumn`, leaving its literal parameters
   * untouched.
   *
   * Since literal parameters (the bucket count, the truncate width) live in `children` rather than
   * in a field of their own, any path that rewrites a transform's children has to skip them. A
   * parameter is not an aliasable value: substituting it changes what the transform computes, and
   * gives the transform a second reference, so `KeyedShuffleSpec.keyPositions` maps it to no
   * cluster key and SPJ is silently lost. Use it for any retargeting of a partition transform.
   *
   * Callers: `KeyedShuffleSpec.rebuiltOver`, which rebuilds a transform over the other side's key,
   * and `GroupPartitionsExec`, which rebuilds a reduced expression over each member's argument.
   */
  def rewriteColumnSlots(rewriteColumn: Expression => Expression): TransformExpression =
    copy(children = children.map {
      case l: Literal => l
      case c => rewriteColumn(c)
    })

  /** The non-literal arguments. */
  def columnSlots: Seq[Expression] = children.filterNot(_.isInstanceOf[Literal])

  /**
   * This transform's identity, or None if some argument references more than one column or is not
   * deterministic (e.g. `b + c`); such a transform is not the same as any other.
   */
  lazy val functionId: Option[TransformFunctionId] = {
    import TransformFunctionId.ArgumentShape
    val shapes = children.map {
      case l: Literal => Some(ArgumentShape.Param(l))
      case t: TransformExpression => t.functionId.map(ArgumentShape.Nested(_))
      case e => TransformExpression.columnShape(e)
    }
    if (shapes.forall(_.isDefined)) {
      Some(TransformFunctionId(function.canonicalName(), shapes.flatten))
    } else {
      None
    }
  }

  /**
   * Whether this [[TransformExpression]] has the same semantics as `other`. For instance,
   * `bucket(32, c)` is equal to `bucket(32, d)`, but not to `bucket(16, d)` or `year(c)`.
   * Similarly, `truncate(c, 2)` is equal to `truncate(d, 2)`, but not to `truncate(c, 4)`.
   *
   * This will be used, for instance, by Spark to determine whether storage-partitioned join can
   * be triggered, by comparing partition transforms from both sides of the join and checking
   * whether they are compatible.
   *
   * It compares the transforms only. A caller that compares partition keys has to consult
   * `reducedWith` as well, since a reduced key space is not the one its transform names.
   *
   * Two transforms are the same when they have the same function name, the same arity, and each
   * pair of corresponding children matches:
   *   - literal arguments must be equal (e.g. numBuckets for bucket, width for truncate), so that
   *     `bucket(32, c)` is not the same as `bucket(16, c)`;
   *   - nested transform arguments must recursively be the same function, so that
   *     `bucket(4, years(c))` is not the same as `bucket(4, days(c))`;
   *   - a bare column must face a bare column. Which column is intentionally ignored (it is
   *     reconciled separately, via `keyPositions`);
   *   - any other argument must be the same expression of its one column, so that
   *     `bucket(4, c + 1)` is the same as `bucket(4, d + 1)` but not as `bucket(4, c + 2)` or
   *     `bucket(4, c)`. An argument over several columns, or a non-deterministic one, leaves a
   *     transform with no identity, so it is not the same as anything.
   *
   * The comparison is of [[functionId]], which is per position rather than a set of literal
   * values. Comparing only the values would make `truncate(id, 2)` and `truncate(2, store_id)` the
   * same function; `isCompatible` short-circuits on this method, so `sameArgumentLayout` -- which
   * guards exactly that, but only inside `reducers` -- would never run, and the pair would be
   * paired as-is with no reduce. `hasSameReducedKeys` compares the same identity, so the two never
   * disagree about which transforms are the same.
   *
   * @param other the transform expression to compare to
   * @return true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean =
    functionId.isDefined && functionId == other.functionId

  /**
   * Whether this [[TransformExpression]]'s function is compatible with the `other`
   * [[TransformExpression]]'s function.
   *
   * This is true if both are instances of [[ReducibleFunction]] and there exists a [[Reducer]] r(x)
   * such that r(t1(x)) = t2(x), or r(t2(x)) = t1(x), for all input x.
   *
   * @param other the transform expression to compare to
   * @return true if compatible, false if not
   */
  def isCompatible(other: TransformExpression): Boolean =
    isSameFunction(other) || {
      val (r1, r2) = pairReducers(other)
      r1.isDefined || r2.isDefined
    }

  /**
   * Extract all literal parameters of this transform as V2 [[V2Literal]]s, preserving each value's
   * internal representation and its `DataType`, with no coercion. The generalized reducer is asked
   * only when they match the declared input types ([[literalParamsMatchInputTypes]]); the
   * deprecated one reads a single `IntegerType` parameter as an `Int`. Memoized.
   *
   * Examples:
   *   bucket(4, col)        => [Literal(4, IntegerType)]
   *   truncate(col, 3)      => [Literal(3, IntegerType)]
   *   days(col)             => []  (no literals)
   */
  private lazy val extractParameters: Array[V2Literal[_]] =
    literalChildren.map(l => LiteralValue(l.value, l.dataType): V2Literal[_]).toArray

  /**
   * Whether every literal parameter has the type the bound function declares at its position in
   * `inputTypes()`. Asked before the generalized reducer, which gets the literals' values without
   * Analyzer type coercion: a literal of another type (a legal implicit cast under
   * [[BoundFunction]]) would be handed to a connector that reads it as the declared type. Then the
   * generalized overload counts as not implemented, and the other overloads, which read no literal
   * by type, may still be asked. A literal past the declared arity has no declared type to compare
   * against, e.g. for an arity-flexible function. Column arguments are not checked, since they are
   * not passed to the connector; where Spark evaluates a transform, [[evaluableFunction]] checks
   * every argument.
   */
  lazy val literalParamsMatchInputTypes: Boolean = {
    val declaredTypes = function.inputTypes()
    children.zipWithIndex.forall {
      case (l: Literal, i) => i >= declaredTypes.length || l.dataType == declaredTypes(i)
      case _ => true
    }
  }

  /**
   * Whether each position holds a literal on both sides, a bare column on both sides, the same
   * nested transform on both sides, or the same expression of one column on both sides (e.g.
   * `b + 1` and `c + 1`, or `s.a` and `t.a`). These are the shapes [[functionId]] tells apart, and
   * a nested transform is compared as it does, by identity, so two instances of one function bound
   * for two scans line up. A pair this admits differs at most in literal values and arity, which
   * the connector reducer decides.
   */
  private def sameArgumentLayout(other: TransformExpression): Boolean =
    children.zip(other.children).forall {
      case (_: Literal, _: Literal) => true
      case (_: Attribute, _: Attribute) => true
      case (t1: TransformExpression, t2: TransformExpression) =>
        t1.functionId.isDefined && t1.functionId == t2.functionId
      case (c1, c2) => TransformExpression.sameShape(c1, c2)
    }

  /**
   * Whether no literal parameter has an array, map, struct or UDT type; those are not passed to a
   * connector reducer. Other scalars, such as intervals, are.
   */
  private def noComplexLiteralParams: Boolean =
    literalChildren.forall(_.dataType match {
      case _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] => false
      case _ => true
    })

  /**
   * Return a [[Reducer]] that maps this transform's partition keys onto `other`'s, or None if there
   * is none: when either function is not a [[ReducibleFunction]], when the argument layouts or
   * literal parameters rule a reducer out, or when the connector reports it is not reducible.
   * Handles both parameterized (bucket, truncate) and non-parameterized (days, hours) functions.
   *
   * This answers one direction only, so it must not decide a pair: a direction that threw would
   * look merely not reducible, and the other direction like a one-side reduce. Use
   * [[pairReducers]].
   */
  private[sql] def reducers(other: TransformExpression): Option[Reducer[_, _]] =
    TransformExpression.reducerOf(reducerOutcome(other))

  /**
   * Both directions' reducers, `reducers(other)` and `other.reducers(this)`, or `(None, None)` when
   * either direction's reducer threw: then the pair is not reducible at all and shuffles. Reading a
   * thrown direction as merely "not reducible" would leave the other direction looking like a
   * one-side reduce, which is only right for a reducer that satisfies `r(f1(x)) = f2(x)`; a
   * two-sided one such as a gcd of bucket counts would then pair keys from two different spaces.
   *
   * Also `(None, None)` when either side has no identity, since a reduce could be neither recorded
   * (`reducedTogetherWith`) nor reported for it. `sameArgumentLayout` already refuses every such
   * pair; deciding it here keeps `isCompatible` and `reducersBothWays` from disagreeing on one.
   */
  def pairReducers(other: TransformExpression)
      : (Option[Reducer[_, _]], Option[Reducer[_, _]]) = {
    import TransformExpression._
    if (functionId.isEmpty || other.functionId.isEmpty) return (None, None)
    (reducerOutcome(other), other.reducerOutcome(this)) match {
      case (Threw(_, _), _) | (_, Threw(_, _)) => (None, None)
      case (o1, o2) => (reducerOf(o1), reducerOf(o2))
    }
  }

  /** What probing the connector for a reducer onto `other` gave, logged when it failed. */
  private def reducerOutcome(other: TransformExpression): TransformExpression.Outcome = {
    import TransformExpression._
    val thisFunction: ReducibleFunction[_, _] = function match {
      case f: ReducibleFunction[_, _] => f
      case _ => return NotReducible
    }
    val otherFunction: ReducibleFunction[_, _] = other.function match {
      case o: ReducibleFunction[_, _] => o
      case _ => return NotReducible
    }
    if (!sameArgumentLayout(other) ||
        !noComplexLiteralParams || !other.noComplexLiteralParams ||
        literalChildren.exists(_.value == null) || other.literalChildren.exists(_.value == null)) {
      return NotReducible
    }

    val thisParams = extractParameters
    val otherParams = other.extractParameters
    val thisName = function.canonicalName()

    // A single IntegerType parameter on each side is the shape the deprecated
    // reducer(int, ..., int) overload accepts. Null parameters were refused above.
    def isSingleInt(p: Array[V2Literal[_]]): Boolean = {
      p.length == 1 && p(0).dataType == IntegerType
    }

    // Connector code that throws, a `LinkageError` included, e.g. a `NoClassDefFoundError` for a
    // missing dependency, makes the outcome `Threw`, so the pair is not reducible rather than
    // failing the query. `Try` alone would let a `LinkageError` through.
    def guarded(outcome: => Outcome): Outcome =
      try outcome catch {
        case e @ (NonFatal(_) | _: LinkageError) => Threw(e)
      }

    // Probe one reducer overload into an Outcome. Pure -- logging is decided once, below.
    def probe(call: => Reducer[_, _]): Outcome = guarded {
      try Option(call).fold[Outcome](NotReducible)(Reducible(_)) catch {
        case _: UnsupportedOperationException => Unimplemented
      }
    }

    // Prefer the generalized overload. It reads its literal parameters by type, so it is asked only
    // when they have their declared types; otherwise it counts as not implemented. Use another
    // overload only if the generalized one is not implemented; any other outcome is final. The type
    // check reads the connector's `inputTypes()`, so it is guarded too.
    val outcome =
      guarded {
        if (!literalParamsMatchInputTypes) {
          LiteralTypesDiffer
        } else {
          // The other side's check runs the other connector's code. A failure there is that
          // side's to report, which the other direction does when it asks; here it only vetoes
          // the pair.
          val otherMatches: Try[Boolean] =
            try Success(other.literalParamsMatchInputTypes) catch {
              case e @ (NonFatal(_) | _: LinkageError) => Failure(e)
            }
          otherMatches match {
            case Failure(e) => Threw(e, ownCode = false)
            case Success(false) => LiteralTypesDiffer
            case Success(true) =>
              // A copy of the memoized arrays per call, so a connector cannot change them.
              probe(thisFunction.reducer(thisParams.clone(), otherFunction, otherParams.clone()))
          }
        }
      } match {
        // A parameterless pair falls back to the parameterless overload.
        case Unimplemented if thisParams.isEmpty && otherParams.isEmpty =>
          probe(thisFunction.reducer(otherFunction))
        // The deprecated overload is documented for bucket against bucket, and master offered it
        // only a pair resolved from two V2 `bucket` transforms, see `fromBucketTransform`.
        case Unimplemented | LiteralTypesDiffer
            if fromBucketTransform && other.fromBucketTransform &&
              isSingleInt(thisParams) && isSingleInt(otherParams) &&
              function.canonicalName() == other.function.canonicalName() =>
          probe(thisFunction.reducer(
            thisParams(0).value().asInstanceOf[Int], otherFunction,
            otherParams(0).value().asInstanceOf[Int]))
        case outcome => outcome
      }

    // A missing reducer costs nothing for a pair of the same function, which needs none.
    outcome match {
      case Threw(e, true) => logReducerThrew(function, thisName, e)
      case Unimplemented if !isSameFunction(other) => logNoReducer(function, thisName)
      case LiteralTypesDiffer if !isSameFunction(other) =>
        // Name the side whose literal parameters do not have its declared types. That is this
        // side unless its own check passed; the other side's check ran only if it did.
        val differs = if (literalParamsMatchInputTypes) other.function else function
        logLiteralTypesDiffer(differs,
          TransformExpression.orElse(differs.canonicalName(), differs.getClass.getName))
      case _ =>
    }
    outcome
  }

  /**
   * The unordered pair of transforms whose reduce produced this expression's keys, which is what
   * identifies the key space they landed in. Reducing is symmetric, so the two sides of one reduce
   * carry the same pair.
   */
  private def reducedKeySpace: Option[Set[TransformFunctionId]] =
    for (self <- functionId; partner <- reducedWith) yield Set(self, partner)

  /**
   * Whether this and `other` describe the same reduced key space, i.e. whether the same pair of
   * transforms was reduced together to produce both.
   *
   * Two reduces that happen to land on the same space through different pairings are not
   * recognised as one. For instance `bucket(12)` with `bucket(8)` and `bucket(12)` with
   * `bucket(20)` both reduce onto `id % 4`. The [[Reducer]] API does not name the space it reduces
   * onto, so the pairing is all there is to compare.
   */
  def hasSameReducedKeys(other: TransformExpression): Boolean =
    reducedKeySpace.isDefined && reducedKeySpace == other.reducedKeySpace

  /**
   * Records that this expression's keys were reduced together with `other`'s. Both need an
   * identity; this fails rather than leave reduced keys looking raw. Unreachable, since
   * `pairReducers` returns no reducer for a pair where either side has none. A planner-built
   * transform can lack one, e.g. `bucket(4, b + c)` from `createPartitioning` or a broadcast
   * join's expansion, so that guard, not the scan's `supportsExpressions`, is what keeps this from
   * failing.
   */
  def reducedTogetherWith(other: TransformExpression): TransformExpression =
    (functionId, other.functionId) match {
      case (Some(_), Some(partner)) => copy(reducedWith = Some(partner))
      case _ =>
        throw SparkException.internalError(
          s"Cannot record a reduce of $this with $other: a transform without an identity " +
            "cannot be reduced, and pairReducers returns no reducer for one.")
    }

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  /**
   * The bound function, when Spark could call it for this transform: a [[ScalarFunction]], over
   * keys no join reduced. See [[resolvedFunction]].
   */
  private def scalarFunction: Option[ScalarFunction[_]] = function match {
    case scalarFunc: ScalarFunction[_] if reducedWith.isEmpty => Some(scalarFunc)
    case _ => None
  }

  /** The arguments of the call: the children, the literal parameters included. */
  private def callArguments: Seq[Expression] = children

  /**
   * The scalar function call this transform stands for, over its arguments as they are. `None`
   * when the bound function is not a [[ScalarFunction]], and also when a join reduced this
   * expression's keys, since then the call no longer computes them. Evaluating it to place a row
   * would send the row to a partition it does not belong to. That second arm is a local gate. No
   * caller reaches it today, because every consumer of a reduced partitioning refuses it first,
   * and the write path never sees one. The write path (`DistributionAndOrderingUtils`) casts the
   * arguments to the function's input types itself. `eval` uses [[evaluableFunction]] instead.
   */
  lazy val resolvedFunction: Option[Expression] =
    scalarFunction.map(V2ExpressionUtils.resolveScalarFunction(_, callArguments))

  /**
   * The call `eval` runs: [[resolvedFunction]], with each argument at the type the function
   * declares for it in `inputTypes()`, see `atDeclaredType` for which arguments suit. `None` when
   * an argument does not, when there is another number of arguments, when the call fails to
   * resolve, and wherever `resolvedFunction` is `None`. `eval` rethrows a failure to resolve. A
   * storage-partitioned join shuffles instead of evaluating such a transform, so each case but
   * reduced keys is logged once, see `logNotScalar`, `logArgumentsMismatch` and
   * `logCannotResolve`.
   */
  def evaluableFunction: Option[Expression] = evaluableFunctionAttempt match {
    case Success(call) => call
    case _ => None
  }

  /**
   * The call, or why it failed to resolve, e.g. a `ScalarFunction` that inherits `produceResult`
   * from a base class, connector code that throws, or a `LinkageError` for a connector class whose
   * dependency is missing, which the planner would otherwise hit for a layout it does not pick.
   * Computed and logged once, since a `lazy val` whose initializer throws runs it again on each
   * access. Not serialized, so an executor resolves it again rather than receiving the call (or
   * the failure) the driver resolved while planning.
   */
  @transient private lazy val evaluableFunctionAttempt: Try[Option[Expression]] =
    try Success(resolveEvaluableFunction()) catch {
      case e @ (NonFatal(_) | _: LinkageError) =>
        TransformExpression.logCannotResolve(function, e)
        Failure(e)
    }

  private def resolveEvaluableFunction(): Option[Expression] = scalarFunction match {
    case None =>
      // Reduced keys are the planner's own doing, so only a function Spark cannot call is logged.
      if (!function.isInstanceOf[ScalarFunction[_]]) TransformExpression.logNotScalar(function)
      None
    case Some(scalarFunc) =>
      val arguments = callArguments
      val declaredTypes = scalarFunc.inputTypes().toSeq
      val typed = arguments.zip(declaredTypes).map { case (argument, declaredType) =>
        TransformExpression.atDeclaredType(argument, declaredType)
      }
      if (arguments.length == declaredTypes.length && typed.forall(_.isDefined)) {
        val call = V2ExpressionUtils.resolveScalarFunction(scalarFunc, typed.flatten)
        TransformExpression.methodLookupFailure(call) match {
          case None => Some(call)
          case failure =>
            // The types the call was resolved over, i.e. after the casts of narrower arguments.
            TransformExpression.logArgumentsMismatch(scalarFunc, typed.flatten.map(_.dataType),
              declaredTypes, failure)
            None
        }
      } else {
        TransformExpression.logArgumentsMismatch(scalarFunc, arguments.map(_.dataType),
          declaredTypes, None)
        None
      }
  }

  override def eval(input: InternalRow): Any = evaluableFunctionAttempt match {
    case Success(Some(call)) => call.eval(input)
    case Failure(e) => throw e
    case _ => throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

object TransformExpression extends Logging {
  /**
   * What was logged once, keyed by its kind: `notScalar:functionClass`,
   * `mismatch:functionClass:argumentTypes:declaredTypes` and its `unreachable:` counterpart for a
   * method that cannot be invoked, since one class can declare other input types per instance,
   * e.g. one per column type, and `failure:functionClass:failureClass`, so that a function class
   * that fails in another way is warned about again.
   */
  private val loggedOnce: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()

  /**
   * Runs `line` the first time `key` is seen while its level is `enabled`. A line its level drops
   * does not take the key, so it is logged once the level is on.
   */
  private def logOnce(key: String, enabled: Boolean)(line: => Unit): Unit =
    if (enabled && loggedOnce.add(key)) line

  /**
   * `value`, or `fallback` when it throws. `name()`, `inputTypes()` and a failure's message are
   * connector code, which can throw as the call did, e.g. a `NoClassDefFoundError` for a missing
   * dependency. A log line or an error message must not fail planning.
   */
  private[sql] def orElse[T](value: => T, fallback: => T): T =
    try value catch { case NonFatal(_) | _: LinkageError => fallback }

  /**
   * `dataType` for a log line, a user-defined type by its class, since its `catalogString` is its
   * `sqlType`'s.
   */
  private def typeName(dataType: DataType): String = dataType match {
    case udt: UserDefinedType[_] => s"${udt.getClass.getName}(${udt.sqlType.catalogString})"
    case other => other.catalogString
  }

  private def typeList(types: Seq[DataType]): String = types.map(typeName).mkString("(", ", ", ")")

  /** `function` for a log line: "partition transform NAME (CLASS)". */
  private def transformLabel(function: BoundFunction): MessageWithContext = {
    val functionClass = function.getClass.getName
    log"partition transform ${MDC(FUNCTION_NAME, orElse(function.name(), functionClass))} " +
      log"(${MDC(CLASS_NAME, functionClass)})"
  }

  /**
   * What `e`, a failure to look up the method a call invokes, says about the lookup, for the
   * info line: a `SparkThrowable`'s message template filled in, without the condition and SQL
   * state that its message carries, and that read as a Spark bug for the `INTERNAL_ERROR` of
   * `methodNotFoundError`. Anything else by `toString`, so with its class. The failure's class
   * alone when that text throws, is null or is empty. The debug line has the whole failure.
   */
  private[expressions] def failureText(e: Throwable): String = {
    val text = orElse(e match {
      case t: SparkThrowable if t.getCondition != null =>
        SparkThrowableHelper.errorReader.getErrorMessage(t.getCondition,
          t.getMessageParameters.asScala.toMap)
      case _ => e.toString
    }, null)
    Option(text).map(_.trim).filter(_.nonEmpty).getOrElse(e.getClass.getName)
  }

  /**
   * The line `logArgumentsMismatch` logs, for the argument and declared type lists. With
   * `lookupFailure`, the arguments suit the input types, cast where they are narrower, but the
   * method the call resolves to cannot be invoked with them, and the line ends with what the
   * lookup says. Without it, there is no `ERROR` in its context.
   */
  private[expressions] def argumentsMismatchMessage(
      function: BoundFunction,
      arguments: String,
      declared: String,
      lookupFailure: Option[Throwable]): MessageWithContext = {
    val head = log"The " + transformLabel(function) + log" declares the input types " +
      log"${MDC(FUNCTION_PARAM, declared)}, "
    lookupFailure match {
      case None =>
        head + log"which do not take arguments of types ${MDC(ARGS, arguments)}, so a " +
          log"storage-partitioned join does not evaluate it"
      case Some(e) =>
        head + log"but the method its call resolves to cannot be invoked with arguments of " +
          log"types ${MDC(ARGS, arguments)}, since it or its class is not public or it does not " +
          log"take their Java classes, so a storage-partitioned join does not evaluate it: " +
          log"${MDC(ERROR, failureText(e))}"
    }
  }

  /**
   * Logs that `function`'s input types do not take `argumentTypes`, or, with `lookupFailure`, that
   * the method its call resolves to cannot be invoked with them, see `evaluableFunction` and
   * `argumentsMismatchMessage`. A lookup failure is logged with its stack trace at debug level.
   */
  private def logArgumentsMismatch(
      function: BoundFunction,
      argumentTypes: Seq[DataType],
      declaredTypes: Seq[DataType],
      lookupFailure: Option[Throwable]): Unit = {
    val arguments = typeList(argumentTypes)
    val declared = typeList(declaredTypes)
    val kind = if (lookupFailure.isDefined) "unreachable" else "mismatch"
    logOnce(s"$kind:${function.getClass.getName}:$arguments:$declared", log.isInfoEnabled) {
      logInfo(argumentsMismatchMessage(function, arguments, declared, lookupFailure))
    }
    lookupFailure.foreach { e =>
      logDebug(log"Cannot invoke the method a call of the " + transformLabel(function) +
        log" resolves to", e)
    }
  }

  /** Logs that `function` is not a `ScalarFunction`, so Spark cannot call it. */
  private def logNotScalar(function: BoundFunction): Unit = {
    logOnce(s"notScalar:${function.getClass.getName}", log.isInfoEnabled) {
      logInfo(log"The " + transformLabel(function) + log" is not a ScalarFunction, so Spark " +
        log"does not evaluate it")
    }
  }

  /** Logs that a call of `function` failed to resolve, see `evaluableFunction`. */
  private def logCannotResolve(function: BoundFunction, e: Throwable): Unit = {
    logOnce(s"failure:${function.getClass.getName}:${e.getClass.getName}", log.isWarnEnabled) {
      logWarning(log"Cannot resolve a call of the " + transformLabel(function) +
        log", so a storage-partitioned join does not evaluate it: " +
        log"${MDC(ERROR, orElse(e.toString, e.getClass.getName))}")
    }
    logDebug(log"Cannot resolve a call of the " + transformLabel(function), e)
  }

  /**
   * Logs that `function`'s reducer threw: at WARN once per function, since every planning pass and
   * AQE re-plan asks the pair again, and at DEBUG, with the stack trace, every time.
   */
  private def logReducerThrew(function: BoundFunction, name: String, e: Throwable): Unit = {
    logOnce(s"reducerThrew:${function.getClass.getName}:$name", log.isWarnEnabled) {
      logWarning(log"V2 function ${MDC(FUNCTION_NAME, name)} reducer threw an exception; " +
        log"treating as not reducible: ${MDC(ERROR, orElse(e.toString, e.getClass.getName))}. " +
        log"Further failures of this function are logged at DEBUG.")
    }
    logDebug(log"V2 function ${MDC(FUNCTION_NAME, name)} reducer threw an exception.", e)
  }

  /**
   * Logs that `function` declares other types for its literal parameters than the transform's
   * literals have, so Spark does not ask its generalized reducer: at WARN once per function, and
   * at DEBUG.
   */
  private def logLiteralTypesDiffer(function: BoundFunction, name: String): Unit = {
    def line: MessageWithContext = log"V2 function ${MDC(FUNCTION_NAME, name)} declares other " +
      log"input types than the transform's literal parameters have, so Spark does not offer them " +
      log"to reducer(Literal[], ReducibleFunction, Literal[]); treating as not reducible."
    logOnce(s"literalTypes:${function.getClass.getName}:$name", log.isWarnEnabled)(logWarning(line))
    logDebug(line)
  }

  /** Logs that `function` implements no reducer: at WARN once per function, and at DEBUG. */
  private def logNoReducer(function: BoundFunction, name: String): Unit = {
    def line: MessageWithContext = log"V2 function ${MDC(FUNCTION_NAME, name)} implements no " +
      log"reducer; treating as not reducible. Override " +
      log"reducer(Literal[], ReducibleFunction, Literal[]) to enable SPJ."
    logOnce(s"noReducer:${function.getClass.getName}:$name", log.isWarnEnabled)(logWarning(line))
    logDebug(line)
  }

  /**
   * `e` with its one column replaced by a placeholder, so that `x + 1` and `y + 1` compare equal;
   * None when `e` is unresolved, references other than one column, or is not deterministic.
   */
  private[sql] def expressionShape(e: Expression): Option[Expression] =
    if (e.resolved && e.deterministic && e.references.size == 1) {
      Some(QueryPlan.normalizeExpressions(e, e.references.toSeq))
    } else {
      None
    }

  /**
   * The shape an identity gives a column argument: `ColumnRef` for a bare column, otherwise its
   * [[expressionShape]], or `None` when it has none. `functionId` and `withColumnShape` both use
   * it, so a rebuilt member's identity and its partner's stay comparable.
   */
  private[sql] def columnShape(e: Expression): Option[TransformFunctionId.ArgumentShape] =
    e match {
      case _: Attribute => Some(TransformFunctionId.ArgumentShape.ColumnRef)
      case _ => expressionShape(e).map(TransformFunctionId.ArgumentShape.Expr(_))
    }

  /**
   * `id` with each column argument's shape replaced by `argument`'s, see [[columnShape]].
   * Unchanged when `argument` has no shape.
   */
  private[sql] def withColumnShape(id: TransformFunctionId, argument: Expression)
      : TransformFunctionId = {
    import TransformFunctionId.ArgumentShape
    columnShape(argument).fold(id) { s =>
      id.copy(argumentShapes = id.argumentShapes.map {
        case ArgumentShape.ColumnRef | ArgumentShape.Expr(_) => s
        case other => other
      })
    }
  }

  /** Whether `a` and `b` are the same expression of one column each, see [[expressionShape]]. */
  private[sql] def sameShape(a: Expression, b: Expression): Boolean =
    expressionShape(a).exists(expressionShape(b).contains)

  /**
   * Whether `e` is a column reference: an [[Attribute]] or a [[GetStructField]] chain
   * (struct-field access on a column). Used by `KeyedPartitioning.supportsExpressions`, which
   * decides whether a scan may report a transform over this argument.
   */
  @tailrec
  private[sql] def isColumnRef(e: Expression): Boolean = e match {
    case _: Attribute => true
    case g: GetStructField => isColumnRef(g.child)
    case _ => false
  }

  /**
   * Whether `e` is a partition expression whose keys a join reduced onto a key space that no
   * transform describes, so that `e` no longer computes them. False after a one-side reduce, where
   * the expression reported is the target transform and describes the reduced keys exactly. False
   * for anything that is not a [[TransformExpression]], an attribute in particular, since a reduce
   * always leaves a transform behind.
   */
  def hasReducedKeys(e: Expression): Boolean = e match {
    case t: TransformExpression => t.reducedWith.isDefined
    case _ => false
  }

  /** The result of probing one reducer overload, for the dispatch in [[TransformExpression]]. */
  private sealed trait Outcome
  private case class Reducible(reducer: Reducer[_, _]) extends Outcome
  private case object NotReducible extends Outcome
  /** Connector code threw; `ownCode` says whether it was this side's, so this side reports it. */
  private case class Threw(e: Throwable, ownCode: Boolean = true) extends Outcome
  private case object Unimplemented extends Outcome
  /** The generalized overload was not asked, since a literal parameter's type is not declared. */
  private case object LiteralTypesDiffer extends Outcome

  /** The reducer an [[Outcome]] found, if it found one. */
  private def reducerOf(o: Outcome): Option[Reducer[_, _]] = o match {
    case Reducible(r) => Some(r)
    case _ => None
  }

  /**
   * `argument` at `declaredType`, or `None` when the function could not take it. See
   * [[TransformExpression.evaluableFunction]].
   *
   * An argument goes as it is when it is stored the way the declared type is (`storedAlike`),
   * which is how a function read it before this check. A numeric argument narrower than declared
   * (`UpCastRule.legalNumericPrecedence`), e.g. a `SMALLINT` against an `INT` or an `INT` against a
   * `DOUBLE`, is held as another JVM type, so it is cast. That gives the value a magic `invoke` got
   * from Java's widening before this check, which can lose digits for a `BIGINT` against a
   * `DOUBLE` as it did there, and needs no time zone. Anything else is refused,
   * a `DATE` against a `TIMESTAMP` in particular, whose cast takes the session time zone. A call
   * whose arguments all suit is still refused when the method it invokes cannot take them, see
   * `methodLookupFailure`.
   */
  private def atDeclaredType(argument: Expression, declaredType: DataType): Option[Expression] =
    (argument.dataType, declaredType) match {
      case (argumentType, _) if storedAlike(argumentType, declaredType, nested = false) =>
        Some(argument)
      case (argumentType, _) if UpCastRule.legalNumericPrecedence(argumentType, declaredType) =>
        Some(Cast(argument, declaredType))
      case _ =>
        None
    }

  /**
   * Why the method `call` invokes does not take its arguments, or `None` when it does. `Invoke`
   * and `StaticInvoke` look it up again from the arguments' classes, a user-defined type's being
   * `Object`, and only among public methods, so a magic `invoke` the declared types found may not
   * be reachable. Looked up here, so that such a call is refused as the argument mismatch it is,
   * rather than failing in `eval`.
   */
  private def methodLookupFailure(call: Expression): Option[Throwable] =
    try {
      call match {
        case i: objects.Invoke => i.method
        case s: objects.StaticInvoke => s.method
        case _ =>
      }
      None
    } catch {
      case NonFatal(e) => Some(e)
    }

  /**
   * Whether a key of type `from` and one of type `to` holding one value are stored as the same
   * bytes in an unsafe row, and order alike read as either type. A cogroup pairs the groups of its
   * two sides by comparing their key rows, ordered by its left side's key types, so keys of other
   * types pair their groups wrongly, or fail, see `KeyedShuffleSpec.createPartitioning`. As
   * `storedAlike` for a nested value, apart from decimals: two decimals of one scale are stored
   * alike, whatever their precision, when both are stored as a `long`, up to
   * `Decimal.MAX_LONG_DIGITS` digits, or both as bytes. A row, the key itself or a struct field,
   * reads a `long` at a smaller precision than its value has as it is. An array element, or a
   * map's key or value, is read with a check that fails for a value of more digits than that
   * precision, as one stored as bytes is anywhere. So a cogroup over such keys can fail, but never
   * pairs its groups wrongly.
   */
  private[sql] def keysStoredAlike(from: DataType, to: DataType): Boolean =
    storedAlike(from, to, nested = true, anyPrecision = true)

  /**
   * Whether a value of type `from` is stored the way one of type `to` is, so that a function
   * declaring `to` reads it as it is:
   *  - two types with one physical type, e.g. a `VARCHAR` and a string of the same collation, a
   *    `DATE` and an `INT`, or a `TIMESTAMP` and a `BIGINT`. A user-defined type is stored as its
   *    `sqlType`;
   *  - two structs, arrays or maps whose fields, elements, keys and values are stored alike, by
   *    position. Field names and nullability do not change how a value is stored. A function
   *    declaring a non-nullable field gets the values a nullable one holds;
   *  - two decimals of one scale, so that the unscaled digits a function reads are the same. At the
   *    top level the function gets the `Decimal` itself, whatever precision it declares, e.g. a
   *    `DECIMAL(12,2)` key of a cogroup that does not coerce its keys against a function declaring
   *    `DECIMAL(10,2)`, see `KeyedShuffleSpec.createPartitioning`. Nested in an unsafe row, a
   *    decimal is read at the declared precision: it is stored as a `long` up to
   *    `Decimal.MAX_LONG_DIGITS` digits and as bytes above, so there `to` needs no less precision,
   *    and both on one side of that. With `anyPrecision`, as `keysStoredAlike` asks, any precision
   *    on one side of that.
   */
  private def storedAlike(
      from: DataType,
      to: DataType,
      nested: Boolean,
      anyPrecision: Boolean = false): Boolean =
    (from, to) match {
      case _ if from == to => true
      case (udt: UserDefinedType[_], _) => storedAlike(udt.sqlType, to, nested, anyPrecision)
      case (_, udt: UserDefinedType[_]) => storedAlike(from, udt.sqlType, nested, anyPrecision)
      case (f: StructType, t: StructType) =>
        f.length == t.length && f.fields.zip(t.fields).forall { case (ff, tf) =>
          storedAlike(ff.dataType, tf.dataType, nested = true, anyPrecision)
        }
      case (f: ArrayType, t: ArrayType) =>
        storedAlike(f.elementType, t.elementType, nested = true, anyPrecision)
      case (f: MapType, t: MapType) =>
        storedAlike(f.keyType, t.keyType, nested = true, anyPrecision) &&
          storedAlike(f.valueType, t.valueType, nested = true, anyPrecision)
      case (f: DecimalType, t: DecimalType) =>
        f.scale == t.scale && (!nested || ((anyPrecision || f.precision <= t.precision) &&
          DecimalType.isByteArrayDecimalType(f) == DecimalType.isByteArrayDecimalType(t)))
      case _ =>
        val physical = PhysicalDataType(from)
        physical != UninitializedPhysicalType && physical == PhysicalDataType(to)
    }
}
