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

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.FUNCTION_NAME
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.connector.expressions.{Literal => V2Literal, LiteralValue}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructType, UserDefinedType}

/**
 * The identity of a reduced key space, stored in [[TransformExpression.reducedWith]] and compared
 * by `hasSameReducedKeys`. It is NOT the "same function" test -- that is
 * [[TransformExpression.isSameFunction]], a structural per-position walk that also recurses into
 * nested transforms. This type exists because `reducedWith` has to *store* an identity in a
 * field, which a structural predicate cannot express.
 *
 * @param canonicalName the transform function's canonical name
 * @param argSlots one entry per argument, in order: `Some(literal)` for a literal parameter (e.g.
 *                 the bucket count for `bucket`, or the width for `truncate`), `None` for any
 *                 other slot. Positions are part of the identity, so `truncate(id, 2)` and
 *                 `truncate(2, store_id)` are different key spaces.
 *
 *                 Known coarseness: a nested transform maps to `None`, so
 *                 `bucket(4, years(c))` and `bucket(4, days(c))` share an identity here. That is
 *                 unreachable for SPJ -- `KeyedPartitioning.supportsExpressions` requires the
 *                 single non-literal child to be a column reference, which rejects nested
 *                 transforms -- and `isSameFunction` distinguishes them regardless.
 */
case class TransformFunctionId(canonicalName: String, argSlots: Seq[Option[Literal]])

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
   * identity -- see [[functionId]] and [[TransformFunctionId.argSlots]].
   */
  private lazy val literalChildren: Seq[Literal] =
    children.collect { case l: Literal => l }

  /**
   * The identity of this expression's transform function: its canonical name plus the per-argument
   * shape, so a literal in a different argument position is a different identity. See
   * [[TransformFunctionId.argSlots]] for why the positions matter.
   */
  lazy val functionId: TransformFunctionId =
    TransformFunctionId(function.canonicalName(), children.map {
      case l: Literal => Some(l)
      case _ => None
    })

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
   *   - everything else must be a plain column reference on both sides. Column identity is
   *     intentionally ignored (it is reconciled separately, via `keyPositions`), but a
   *     non-reference slot such as `c + 1` or `cast(c)`, or a literal-vs-reference mismatch, is
   *     treated as not the same.
   *
   * The walk is per position, not a set of literal values. Comparing only the values would make
   * `truncate(id, 2)` and `truncate(2, store_id)` the same function; `isCompatible` short-circuits
   * on this method, so `sameArgumentLayout` -- which guards exactly that, but only inside `reducer`
   * -- would never run, and the pair would be paired as-is with no reduce.
   *
   * @param other the transform expression to compare to
   * @return true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean =
    function.canonicalName() == other.function.canonicalName() &&
      children.length == other.children.length &&
      children.zip(other.children).forall {
        case (l1: Literal, l2: Literal) => l1 == l2
        case (t1: TransformExpression, t2: TransformExpression) => t1.isSameFunction(t2)
        // Any other pair must be a plain column reference on both sides. Column identity is
        // ignored (reconciled separately via keyPositions); a non-reference slot (Add, Cast, ...)
        // or a literal/transform-vs-reference mismatch is "not the same".
        case (c1, c2) => TransformExpression.isColumnRef(c1) && TransformExpression.isColumnRef(c2)
      }

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
  def isCompatible(other: TransformExpression): Boolean = {
    if (isSameFunction(other)) {
      true
    } else {
      (function, other.function) match {
        case (f: ReducibleFunction[_, _], o: ReducibleFunction[_, _]) =>
          val thisReducer = reducer(f, this, o, other)
          val otherReducer = reducer(o, other, f, this)
          thisReducer.isDefined || otherReducer.isDefined
        case _ => false
      }
    }
  }

  /**
   * Return a [[Reducer]] for this transform expression on another
   * on the transform expression.
   * <p>
   * A [[Reducer]] exists for a transform expression function if it is
   * 'reducible' on the other expression function.
   * <p>
   * @return reducer function or None if not reducible on the other transform expression
   */
  def reducers(other: TransformExpression): Option[Reducer[_, _]] = {
    (function, other.function) match {
      case (e1: ReducibleFunction[_, _], e2: ReducibleFunction[_, _]) =>
        reducer(e1, this, e2, other)
      case _ => None
    }
  }

  /**
   * Re-targets this partition transform expression at `attr`. A partition transform expression
   * has a single leaf attribute (`KeyedPartitioning.supportsExpressions`), so this replaces that
   * attribute and keeps the rest of the expression: any field path above the leaf comes from this
   * expression, not from the re-targeted key. Re-targeting is therefore only faithful when the
   * source and the target key expressions have the same path shape.
   */
  def withReference(attr: Attribute): TransformExpression =
    transform { case _: AttributeReference => attr }.asInstanceOf[TransformExpression]

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
   */
  private def inputTypesMatch(select: Expression => Boolean): Boolean = {
    val declaredTypes = function.inputTypes()
    children.zipWithIndex.forall {
      case (c, i) => !select(c) || i >= declaredTypes.length || c.dataType == declaredTypes(i)
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
   * Whether every argument -- columns AND literals -- matches its declared input type, at exactly
   * the declared arity. Required before directly evaluating the transform (the
   * identity-vs-transform reducer in `KeyedShuffleSpec`), which feeds every child through the
   * function's `SpecificInternalRow(inputTypes())`: a child whose type differs would raise a
   * `ClassCastException`, and a child *beyond* the declared arity would raise an
   * `ArrayIndexOutOfBoundsException` (the row is sized to `inputTypes().length`). The exact-arity
   * requirement is specific to this eval path -- the transform-vs-transform path passes literals to
   * the connector's reducer (no eval) and deliberately allows mixed arity, so it uses
   * [[literalParamsMatchInputTypes]], which keeps the beyond-arity short-circuit. The check is one
   * level and reads each child's `dataType`: the non-literal child is a column reference -- an
   * [[Attribute]] or [[GetStructField]] chain, never a nested transform (rejected by the scan gate
   * `supportsExpressions`/`isColumnRef`) -- so there is no inner-transform column to recurse into,
   * and a gate-admitted child always has a resolvable `dataType`, so the eager read is safe.
   * Stronger than [[literalParamsMatchInputTypes]].
   */
  lazy val argsMatchInputTypes: Boolean =
    children.length == function.inputTypes().length && inputTypesMatch(_ => true)

  /**
   * Reducer precondition: positionally-aligned argument structure with `other` -- at each zipped
   * position a literal aligns with a literal, and any other slot is a column reference on both
   * sides. Only literal *values* may differ. Arity is NOT required to match: children are zipped (a
   * shorter side truncates), so a zero-vs-one parameter pair is admitted and left to the connector
   * reducer. Unlike [[isSameFunction]] the function name is not compared.
   */
  private def sameArgumentLayout(other: TransformExpression): Boolean =
    children.zip(other.children).forall {
      case (_: Literal, _: Literal) => true
      case (c1, c2) => TransformExpression.isColumnRef(c1) && TransformExpression.isColumnRef(c2)
    }

  /**
   * Whether no literal parameter has a complex type. A literal is rejected if its [[DataType]] is
   * [[ArrayType]] / [[MapType]] / [[StructType]] / [[UserDefinedType]]. Such params (whose value is
   * a Catalyst-internal container, or -- for a UDT -- whatever its `sqlType` serializes to) must
   * not cross the public reducer boundary, so the transform is treated as not reducible. Keying off
   * the type (not the value) also rejects a null-valued complex literal, and rejecting all UDTs is
   * a safe over-approximation (a UDT transform parameter is exotic; the cost is a shuffle). Scalar
   * types such as `CalendarIntervalType` are admitted (the connector interprets them via the type).
   */
  private def noComplexLiteralParams: Boolean =
    literalChildren.forall(_.dataType match {
      case _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] => false
      case _ => true
    })

  /**
   * Return a Reducer for a reducible function on another reducible function
   * Handles both parameterized (bucket, truncate) and non-parameterized (days, hours) functions.
   */
  private def reducer(
      thisFunction: ReducibleFunction[_, _],
      thisExpr: TransformExpression,
      otherFunction: ReducibleFunction[_, _],
      otherExpr: TransformExpression): Option[Reducer[_, _]] = {
    import TransformExpression._
    if (!thisExpr.sameArgumentLayout(otherExpr) ||
        !thisExpr.literalParamsMatchInputTypes || !otherExpr.literalParamsMatchInputTypes ||
        !thisExpr.noComplexLiteralParams || !otherExpr.noComplexLiteralParams) {
      return None
    }

    val thisParams = thisExpr.extractParameters
    val otherParams = otherExpr.extractParameters
    val thisName = thisExpr.function.canonicalName()

    // A single non-null IntegerType param on each side is the shape the deprecated
    // reducer(int, ..., int) fallback accepts. Gate on the DataType, not the boxed runtime class
    // (DateType / YearMonthInterval also box to Int). A typed null (Literal(null, IntegerType)) is
    // excluded: null.asInstanceOf[Int] would fabricate a 0 a legacy reducer might accept, so a
    // typed null must not reach the deprecated fallback (the generalized overload sees the real
    // null).
    def isSingleInt(p: Array[V2Literal[_]]): Boolean = {
      p.length == 1 && p(0).dataType == IntegerType && p(0).value() != null
    }

    // Probe one reducer overload into an Outcome. Pure -- logging is decided once, below.
    def probe(call: => Reducer[_, _]): Outcome = Try(Option(call)) match {
      case Success(Some(r)) => Reducible(r)
      case Success(None) => NotReducible
      case Failure(_: UnsupportedOperationException) => Unimplemented
      case Failure(e) => Threw(e)
    }

    // Prefer the generalized Literal[] overload; fall back to the deprecated int overload only for
    // a single-int pair, and only when the generalized one is not implemented.
    // Modern connectors never touch the deprecated path; deprecated-only connectors still reduce.
    val outcome =
      if (thisParams.isEmpty && otherParams.isEmpty) {
        probe(thisFunction.reducer(otherFunction))
      } else {
        probe(thisFunction.reducer(thisParams, otherFunction, otherParams)) match {
          // Generalized overload not implemented: fall back to the deprecated int overload, but
          // only for a single-int pair. Any other generalized outcome (reducible, deliberately not
          // reducible, or a thrown bug) is authoritative.
          case Unimplemented if isSingleInt(thisParams) && isSingleInt(otherParams) =>
            probe(thisFunction.reducer(
              thisParams(0).value().asInstanceOf[Int], otherFunction,
              otherParams(0).value().asInstanceOf[Int]))
          case other => other
        }
      }

    outcome match {
      case Reducible(r) => Some(r)
      case NotReducible => None
      case Threw(e) =>
        logWarning(log"V2 function ${MDC(FUNCTION_NAME, thisName)} reducer threw an exception; " +
          log"treating as not reducible.", e)
        None
      case Unimplemented =>
        logWarning(log"V2 function ${MDC(FUNCTION_NAME, thisName)} implements no reducer; " +
          log"treating as not reducible. Override " +
          log"reducer(Literal[], ReducibleFunction, Literal[]) to enable SPJ.")
        None
    }
  }

  /**
   * The unordered pair of transforms whose reduce produced this expression's keys, which is what
   * identifies the key space they landed in. Reducing is symmetric, so the two sides of one reduce
   * carry the same pair.
   */
  private def reducedKeySpace: Option[Set[TransformFunctionId]] =
    reducedWith.map(partner => Set(functionId, partner))

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
    reducedWith.isDefined && reducedKeySpace == other.reducedKeySpace

  /** Records that this expression's keys were reduced together with `other`'s. */
  def reducedTogetherWith(other: TransformExpression): TransformExpression =
    copy(reducedWith = Some(other.functionId))

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
   * Whether `e` is a bare column reference: an [[Attribute]] or a [[GetStructField]] chain
   * (struct-field access on a column). Shared by [[TransformExpression.isSameFunction]] and by
   * `KeyedPartitioning.supportsExpressions`, which both decide whether a transform's single
   * non-literal argument is a plain column.
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
}
