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
 * It carries no exprIds (a column argument is [[TransformFunctionId.ArgumentShape.Column]], not the
 * attribute), so an expression can hold one in a plain field and expression equality still answers
 * the same after canonicalization.
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
     * A plain column reference: an [[Attribute]] or a [[GetStructField]] chain. Which column is not
     * part of the identity -- the two sides of a join reference different columns by construction,
     * and `KeyedShuffleSpec.keyPositions` reconciles them separately.
     */
    case object Column extends ArgumentShape
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
   * This transform's identity, or None if some argument is not a literal, a nested transform or a
   * column reference (e.g. `c + 1`); such a transform is not the same as any other.
   */
  lazy val functionId: Option[TransformFunctionId] = {
    import TransformFunctionId.ArgumentShape
    val shapes = children.map {
      case l: Literal => Some(ArgumentShape.Param(l))
      case t: TransformExpression => t.functionId.map(ArgumentShape.Nested(_))
      case c if TransformExpression.isColumnRef(c) => Some(ArgumentShape.Column)
      case _ => None
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
   *   - everything else must be a plain column reference on both sides. Column identity is
   *     intentionally ignored (it is reconciled separately, via `keyPositions`), but a
   *     non-reference slot such as `c + 1` or `cast(c)`, or a literal-vs-reference mismatch, is
   *     treated as not the same.
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
    isSameFunction(other) || reducers(other).isDefined || other.reducers(this).isDefined

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
   * Rewrites this transform's column arguments with `rewriteColumn`, leaving its literal parameters
   * untouched. Unlike [[withReference]], which swaps the attribute *inside* a column argument (so a
   * `GetStructField` path above it is kept), this replaces the whole argument.
   *
   * Since literal parameters (the bucket count, the truncate width) live in `children` rather than
   * in a field of their own, any path that rewrites a transform's children has to skip them. A
   * parameter is not an aliasable value: substituting it changes what the transform computes, drops
   * it out of `functionId`, adds a second entry to `references` (tripping
   * `KeyedShuffleSpec.keyPositions`' single-reference assert), and makes
   * `KeyedPartitioning.supportsExpressions` reject the partitioning outright -- so SPJ is silently
   * lost. Every such path should go through here rather than reimplement the skip.
   *
   * Callers: `KeyedShuffleSpec.createPartitioning`, which retargets a transform at the other side's
   * clustering key, and `PartitioningPreservingUnaryExecNode`, which retargets it at an aliased
   * output attribute.
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
   * Reading `dataType` can throw rather than answer. [[withReference]] retargets a transform at
   * another key, and a [[GetStructField]] column slot retargeted at a non-struct attribute -- an
   * `identity` column on the other side of a join, say -- leaves `GetStructField(intCol, 0)`, whose
   * `dataType` raises `INTERNAL_ERROR: GetStructField requires a StructType child`. Both callers
   * are gates that must answer "not reducible" rather than fail the query, so a child whose type
   * cannot be read is treated as not matching.
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
   * Whether each position holds a literal on both sides or a column reference on both sides.
   * Literal values and arity may differ; the connector reducer decides those.
   */
  private def sameArgumentLayout(other: TransformExpression): Boolean =
    children.zip(other.children).forall {
      case (_: Literal, _: Literal) => true
      case (c1, c2) => TransformExpression.isColumnRef(c1) && TransformExpression.isColumnRef(c2)
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
   */
  def reducers(other: TransformExpression): Option[Reducer[_, _]] = {
    import TransformExpression._
    val thisFunction: ReducibleFunction[_, _] = function match {
      case f: ReducibleFunction[_, _] => f
      case _ => return None
    }
    val otherFunction: ReducibleFunction[_, _] = other.function match {
      case o: ReducibleFunction[_, _] => o
      case _ => return None
    }
    if (!sameArgumentLayout(other) ||
        !literalParamsMatchInputTypes || !other.literalParamsMatchInputTypes ||
        !noComplexLiteralParams || !other.noComplexLiteralParams) {
      return None
    }

    val thisParams = extractParameters
    val otherParams = other.extractParameters
    val thisName = function.canonicalName()

    // Only a single non-null IntegerType parameter per side may use the deprecated int overload; a
    // typed null would otherwise be read as 0.
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

    // Prefer the generalized overload. Use the deprecated one only if the generalized one is not
    // implemented; any other outcome is final.
    val outcome =
      if (thisParams.isEmpty && otherParams.isEmpty) {
        probe(thisFunction.reducer(otherFunction))
      } else {
        probe(thisFunction.reducer(thisParams, otherFunction, otherParams)) match {
          // The deprecated overload is documented for bucket against bucket, so only offer it a
          // pair of the same function.
          case Unimplemented if isSingleInt(thisParams) && isSingleInt(otherParams) &&
              function.name() == other.function.name() =>
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
