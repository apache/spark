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

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.FUNCTION_NAME
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.connector.expressions.{Literal => V2Literal, LiteralValue}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructType, UserDefinedType}

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
 */
case class TransformExpression(
    function: BoundFunction,
    children: Seq[Expression],
    reducedWith: Option[TransformFunctionId] = None) extends Expression with Logging {

  override def nullable: Boolean = true

  /** Drops the trailing `reducedWith` when it is unset, so the ordinary form is unchanged. */
  override protected def stringArgs: Iterator[Any] =
    if (reducedWith.isEmpty) super.stringArgs.take(productArity - 1) else super.stringArgs

  /**
   * Extract literal children (constant parameters) from this transform. These are constant
   * arguments like the bucket count in `bucket(n, col)` or the width in `truncate(col, width)`.
   * Positions are dropped, so this is for handing parameters to a connector reducer, NOT for
   * identity -- see [[functionId]] and [[TransformFunctionId.argumentShapes]].
   */
  private lazy val literalChildren: Seq[Literal] =
    children.collect { case l: Literal => l }

  /**
   * This transform's identity, or None if some argument references more than one column or is not
   * deterministic (e.g. `b + c`); such a transform is not the same as any other.
   */
  lazy val functionId: Option[TransformFunctionId] = {
    import TransformFunctionId.ArgumentShape
    val shapes = children.map {
      case l: Literal => Some(ArgumentShape.Param(l))
      case t: TransformExpression => t.functionId.map(ArgumentShape.Nested(_))
      case _: Attribute => Some(ArgumentShape.ColumnRef)
      case e => TransformExpression.expressionShape(e).map(ArgumentShape.Expr(_))
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
   * guards exactly that, but only inside `reducer` -- would never run, and the pair would be paired
   * as-is with no reduce. `hasSameReducedKeys` compares the same identity, so the two never
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
   * Rewrites this transform's column arguments with `rewriteColumn`, leaving its literal parameters
   * untouched.
   *
   * Since literal parameters (the bucket count, the truncate width) live in `children` rather than
   * in a field of their own, any path that rewrites a transform's children has to skip them. A
   * parameter is not an aliasable value: substituting it changes what the transform computes, drops
   * it out of `functionId`, adds a second entry to `references` (tripping
   * `KeyedShuffleSpec.keyPositions`' single-reference assert), and makes
   * `KeyedPartitioning.supportsExpressions` reject the partitioning outright -- so SPJ is silently
   * lost. Use it for any retargeting of a partition transform.
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
   * Extract all literal parameters of this transform as V2 [[V2Literal]]s, preserving each value's
   * internal representation and its `DataType`. Only consulted once a reducer path has confirmed
   * the literal params already match the declared input types (see
   * [[literalParamsMatchInputTypes]]), so no type coercion happens here. Memoized.
   *
   * Examples:
   *   bucket(4, col)        => [Literal(4, IntegerType)]
   *   truncate(col, 3)      => [Literal(3, IntegerType)]
   *   days(col)             => []  (no literals)
   */
  private lazy val extractParameters: Array[V2Literal[_]] =
    literalChildren.map(l => LiteralValue(l.value, l.dataType): V2Literal[_]).toArray

  /**
   * Whether the `select`ed children match the bound function's declared input type at their
   * positions. A child beyond the declared arity has no declared type to compare against (e.g. an
   * arity-flexible function), so it is left to the connector reducer / other guards. The DataType
   * match is exact by design: any mismatch (including cosmetic ones like Array `containsNull` or
   * Decimal precision/scale) fails safe to a shuffle. See the two predicates below for the callers.
   *
   * Reading `dataType` can throw rather than answer, e.g. for a `GetStructField` over a non-struct
   * column. Both callers are gates that must answer "not reducible" rather than fail the query, so
   * a child whose type cannot be read is treated as not matching.
   */
  private def inputTypesMatch(select: Expression => Boolean): Boolean = {
    val declaredTypes = function.inputTypes()
    children.zipWithIndex.forall {
      case (c, i) =>
        !select(c) || i >= declaredTypes.length ||
          Try(c.dataType).toOption.contains(declaredTypes(i))
    }
  }

  /**
   * Whether every literal parameter matches its declared input type. Used by the transform-vs-
   * transform reducer path, which hands literal *values* to the connector reducer without Analyzer
   * type coercion. A literal whose type differs from the declared input type (a legal implicit cast
   * under [[BoundFunction]]) is not reducible: the join falls back to a shuffle rather than handing
   * the connector a value the partitions were not built on, which it would then mis-cast. Column
   * slots are not checked (they are not passed to the connector); the eval path uses
   * [[argsMatchInputTypes]] instead.
   */
  lazy val literalParamsMatchInputTypes: Boolean = inputTypesMatch(_.isInstanceOf[Literal])

  /**
   * Whether every argument has its declared type, at exactly the declared arity. Required before
   * evaluating the transform directly: no cast is applied, and a mismatch would fail at evaluation.
   */
  lazy val argsMatchInputTypes: Boolean =
    children.length == function.inputTypes().length && inputTypesMatch(_ => true)

  /**
   * Whether each position holds a literal on both sides, a column reference on both sides, or the
   * same expression of a column on both sides (e.g. `b + 1` and `c + 1`). Literal values and arity
   * may differ; the connector reducer decides those.
   */
  private def sameArgumentLayout(other: TransformExpression): Boolean =
    children.zip(other.children).forall {
      case (_: Literal, _: Literal) => true
      case (c1, c2) if TransformExpression.isColumnRef(c1) && TransformExpression.isColumnRef(c2) =>
        true
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
   * This answers one direction only. To decide a pair, use [[pairReducers]], which also refuses
   * the pair when the other direction's reducer threw.
   */
  def reducers(other: TransformExpression): Option[Reducer[_, _]] =
    TransformExpression.reducerOf(reducerOutcome(other))

  /**
   * Both directions' reducers, `reducers(other)` and `other.reducers(this)`, or `(None, None)` when
   * either direction's reducer threw: then the pair is not reducible at all and shuffles. Reading a
   * thrown direction as merely "not reducible" would leave the other direction looking like a
   * one-side reduce, which is only right for a reducer that satisfies `r(f1(x)) = f2(x)`; a
   * two-sided one such as a gcd of bucket counts would then pair keys from two different spaces.
   */
  def pairReducers(other: TransformExpression)
      : (Option[Reducer[_, _]], Option[Reducer[_, _]]) = {
    import TransformExpression._
    (reducerOutcome(other), other.reducerOutcome(this)) match {
      case (Threw(_), _) | (_, Threw(_)) => (None, None)
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
        !literalParamsMatchInputTypes || !other.literalParamsMatchInputTypes ||
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

    // Probe one reducer overload into an Outcome. Pure -- logging is decided once, below.
    def probe(call: => Reducer[_, _]): Outcome = Try(Option(call)) match {
      case Success(Some(r)) => Reducible(r)
      case Success(None) => NotReducible
      case Failure(_: UnsupportedOperationException) => Unimplemented
      case Failure(e) => Threw(e)
    }

    // Prefer the generalized overload. Use the deprecated one only if the generalized one is not
    // implemented; any other outcome is final.
    val outcome =
      probe(thisFunction.reducer(thisParams, otherFunction, otherParams)) match {
        // A parameterless pair falls back to the parameterless overload.
        case Unimplemented if thisParams.isEmpty && otherParams.isEmpty =>
          probe(thisFunction.reducer(otherFunction))
        // The deprecated overload is documented for bucket against bucket, so only offer it a
        // pair of the same function.
        case Unimplemented if isSingleInt(thisParams) && isSingleInt(otherParams) &&
            function.canonicalName() == other.function.canonicalName() =>
          probe(thisFunction.reducer(
            thisParams(0).value().asInstanceOf[Int], otherFunction,
            otherParams(0).value().asInstanceOf[Int]))
        case outcome => outcome
      }

    outcome match {
      case Threw(e) =>
        logWarning(log"V2 function ${MDC(FUNCTION_NAME, thisName)} reducer threw an exception; " +
          log"treating as not reducible.", e)
      case Unimplemented =>
        logWarning(log"V2 function ${MDC(FUNCTION_NAME, thisName)} implements no reducer; " +
          log"treating as not reducible. Override " +
          log"reducer(Literal[], ReducibleFunction, Literal[]) to enable SPJ.")
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
   * supportsExpressions admits only transforms that have one.
   */
  def reducedTogetherWith(other: TransformExpression): TransformExpression =
    (functionId, other.functionId) match {
      case (Some(_), Some(partner)) => copy(reducedWith = Some(partner))
      case _ =>
        throw SparkException.internalError(
          s"Cannot record a reduce of $this with $other: a transform without an identity " +
            "cannot be reduced, since KeyedPartitioning.supportsExpressions rejects it.")
    }

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  /**
   * The scalar function call this transform stands for. `None` when the bound function is not a
   * [[ScalarFunction]], and also when a join reduced this expression's keys, since then the call
   * no longer computes them. Evaluating it to place a row would send the row to a partition it
   * does not belong to. That second arm is a local gate. No caller reaches it today, because every
   * consumer of a reduced partitioning refuses it first, and the write path never sees one.
   */
  lazy val resolvedFunction: Option[Expression] = function match {
    case scalarFunc: ScalarFunction[_] if reducedWith.isEmpty =>
      Some(V2ExpressionUtils.resolveScalarFunction(scalarFunc, children))
    case _ => None
  }

  override def eval(input: InternalRow): Any = resolvedFunction match {
    case Some(fn) => fn.eval(input)
    case None => throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

object TransformExpression {
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
   * `id` with each column argument's shape replaced by `argument`'s: `ColumnRef` for a bare column,
   * otherwise its [[expressionShape]]. Unchanged when `argument` has no shape.
   */
  private[sql] def withColumnShape(id: TransformFunctionId, argument: Expression)
      : TransformFunctionId = {
    import TransformFunctionId.ArgumentShape
    val shape = argument match {
      case _: Attribute => Some(ArgumentShape.ColumnRef)
      case e => expressionShape(e).map(ArgumentShape.Expr(_))
    }
    shape.fold(id) { s =>
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
   * (struct-field access on a column). Used by `sameArgumentLayout` and by
   * `KeyedPartitioning.supportsExpressions`, which both decide whether a transform's non-literal
   * argument is a plain column.
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
  private case class Threw(e: Throwable) extends Outcome
  private case object Unimplemented extends Outcome

  /** The reducer an [[Outcome]] found, if it found one. */
  private def reducerOf(o: Outcome): Option[Reducer[_, _]] = o match {
    case Reducible(r) => Some(r)
    case _ => None
  }
}
