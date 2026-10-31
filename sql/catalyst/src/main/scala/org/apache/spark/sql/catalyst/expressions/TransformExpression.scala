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

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{ARGS, CLASS_NAME, ERROR, FUNCTION_NAME, FUNCTION_PARAM}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, UninitializedPhysicalType}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, IntegralType, MapType, StructType, UpCastRule, UserDefinedType}

/**
 * The identity of a partition transform function. It holds the two values
 * [[TransformExpression]] compares in `isSameFunction`, and it carries no exprIds, so an
 * expression can hold one in a plain field and expression equality still answers the same after
 * canonicalization.
 *
 * @param canonicalName the transform function's canonical name
 * @param numBucketsOpt the number of buckets if the transform is `bucket`. Unset otherwise.
 */
case class TransformFunctionId(canonicalName: String, numBucketsOpt: Option[Int])

/**
 * Represents a partition transform expression, for instance, `bucket`, `days`, `years`, etc.
 *
 * @param function the transform function itself. Spark will use it to decide whether two
 *                 partition transform expressions are compatible.
 * @param numBucketsOpt the number of buckets if the transform is `bucket`. Unset otherwise.
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
    numBucketsOpt: Option[Int] = None,
    reducedWith: Option[TransformFunctionId] = None) extends Expression {

  override def nullable: Boolean = true

  /** Drops the trailing `reducedWith` when it is unset, so the ordinary form is unchanged. */
  override protected def stringArgs: Iterator[Any] =
    if (reducedWith.isEmpty) super.stringArgs.take(productArity - 1) else super.stringArgs

  /** The identity of this expression's transform function. */
  lazy val functionId: TransformFunctionId =
    TransformFunctionId(function.canonicalName(), numBucketsOpt)

  /**
   * Whether this [[TransformExpression]] has the same semantics as `other`.
   * For instance, `bucket(32, c)` is equal to `bucket(32, d)`, but not to `bucket(16, d)` or
   * `year(c)`.
   *
   * This will be used, for instance, by Spark to determine whether storage-partitioned join can
   * be triggered, by comparing partition transforms from both sides of the join and checking
   * whether they are compatible.
   *
   * It compares the transforms only. A caller that compares partition keys has to consult
   * `reducedWith` as well, since a reduced key space is not the one its transform names.
   *
   * @param other the transform expression to compare to
   * @return true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean = functionId == other.functionId

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
          val thisReducer = reducer(f, numBucketsOpt, o, other.numBucketsOpt)
          val otherReducer = reducer(o, other.numBucketsOpt, f, numBucketsOpt)
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
      case(e1: ReducibleFunction[_, _], e2: ReducibleFunction[_, _]) =>
        reducer(e1, numBucketsOpt, e2, other.numBucketsOpt)
      case _ => None
    }
  }

  // Return a Reducer for a reducible function on another reducible function
  private def reducer(
      thisFunction: ReducibleFunction[_, _],
      thisNumBucketsOpt: Option[Int],
      otherFunction: ReducibleFunction[_, _],
      otherNumBucketsOpt: Option[Int]): Option[Reducer[_, _]] = {
    val res = (thisNumBucketsOpt, otherNumBucketsOpt) match {
      case (Some(numBuckets), Some(otherNumBuckets)) =>
        thisFunction.reducer(numBuckets, otherFunction, otherNumBuckets)
      case _ => thisFunction.reducer(otherFunction)
    }
    Option(res)
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
   * The bound function, when Spark could call it for this transform: a [[ScalarFunction]], over
   * keys no join reduced. See [[resolvedFunction]].
   */
  private def scalarFunction: Option[ScalarFunction[_]] = function match {
    case scalarFunc: ScalarFunction[_] if reducedWith.isEmpty => Some(scalarFunc)
    case _ => None
  }

  /** The arguments of the call: the bucket count, if any, as a literal, then the children. */
  private def callArguments: Seq[Expression] =
    numBucketsOpt.fold(children)(n => Literal(n) +: children)

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
   * declares for it in `inputTypes()`. Nothing casts the arguments after analysis, so the call
   * gets the values as the argument holds them. Most arguments go as they are, when they are held
   * the way the declared type is, which is what a function read before this check. One kind is
   * cast: an integral argument narrower than declared is widened with a [[Cast]], which keeps its
   * value and needs no time zone. A function bound to a `SMALLINT` column but declaring `INT`
   * stays evaluable that way. Any other mismatch, or a different number of arguments, gives
   * `None`, as does everything `resolvedFunction` is `None` for. See `atDeclaredType`. A mismatch
   * is logged at info level once per function class and argument types, since it makes a
   * storage-partitioned join shuffle where it would not otherwise.
   *
   * Resolving the call can throw, e.g. when Spark finds no method to call on a `ScalarFunction`
   * that inherits `produceResult` from a base class, or when connector code it calls throws. That
   * gives `None` too, and `eval` rethrows it. So does a `LinkageError`, e.g. a connector class
   * whose dependency is missing, which the planner would otherwise hit for a layout it does not
   * pick. The planner asks this of such layouts on every query, so a failure is logged as a
   * warning once per function class and kind of failure, and with its stack trace at debug level.
   * Not serialized, so an executor resolves it again rather than receiving the call (or the
   * failure) the driver resolved while planning.
   */
  def evaluableFunction: Option[Expression] = evaluableFunctionAttempt.toOption.flatten

  /**
   * The call, or why it failed to resolve, computed and logged once, since a `lazy val` whose
   * initializer throws runs it again on each access.
   */
  @transient private lazy val evaluableFunctionAttempt: Try[Option[Expression]] = {
    val attempt = try Success(resolveEvaluableFunction()) catch {
      case NonFatal(e) => Failure(e)
      case e: LinkageError => Failure(e)
    }
    attempt.failed.foreach(TransformExpression.logCannotResolve(function, _))
    attempt
  }

  private def resolveEvaluableFunction(): Option[Expression] =
    scalarFunction.flatMap { scalarFunc =>
      val arguments = callArguments
      val declaredTypes = scalarFunc.inputTypes().toSeq
      val typed = if (arguments.length == declaredTypes.length) {
        arguments.zip(declaredTypes).map { case (argument, declaredType) =>
          TransformExpression.atDeclaredType(argument, declaredType)
        }
      } else {
        Seq(None)
      }
      if (typed.forall(_.isDefined)) {
        Some(V2ExpressionUtils.resolveScalarFunction(scalarFunc, typed.flatten))
      } else {
        TransformExpression.logArgumentsMismatch(scalarFunc, arguments.map(_.dataType),
          declaredTypes)
        None
      }
    }

  override def eval(input: InternalRow): Any = evaluableFunction match {
    case Some(fn) => fn.eval(input)
    case None =>
      evaluableFunctionAttempt.failed.foreach(e => throw e)
      throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

object TransformExpression extends Logging {
  /**
   * The function classes and kinds of failure a failure to resolve a call was logged as a warning
   * for, as `functionClass:failureClass`. A function class that fails in another way is warned
   * about again.
   */
  private val failuresWarnedAbout: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()

  /**
   * The function classes and argument types a mismatch with the function's input types was
   * logged for, as `functionClass:argumentTypes`.
   */
  private val mismatchesLogged: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()

  /**
   * `text`, or `fallback` when it throws. `name()` and a failure's message are connector code,
   * which can throw as the call did, e.g. a `NoClassDefFoundError` for a missing dependency. A log
   * line must not fail planning.
   */
  private def orElse(text: => String, fallback: String): String =
    try text catch { case NonFatal(_) | _: LinkageError => fallback }

  /**
   * Logs that `function`'s input types do not take `argumentTypes`, see `evaluableFunction`.
   */
  private def logArgumentsMismatch(
      function: BoundFunction,
      argumentTypes: Seq[DataType],
      declaredTypes: Seq[DataType]): Unit = {
    def typeList(types: Seq[DataType]): String = types.map(_.catalogString).mkString("(", ", ", ")")
    val functionClass = function.getClass.getName
    val arguments = typeList(argumentTypes)
    if (mismatchesLogged.add(s"$functionClass:$arguments")) {
      val functionName = orElse(function.name(), functionClass)
      logInfo(log"The partition transform ${MDC(FUNCTION_NAME, functionName)} " +
        log"(${MDC(CLASS_NAME, functionClass)}) declares the input types " +
        log"${MDC(FUNCTION_PARAM, typeList(declaredTypes))}, which do not take arguments of " +
        log"types ${MDC(ARGS, arguments)}, so a storage-partitioned join does not evaluate it")
    }
  }

  /** Logs that a call of `function` failed to resolve, see `evaluableFunction`. */
  private def logCannotResolve(function: BoundFunction, e: Throwable): Unit = {
    val functionClass = function.getClass.getName
    val failureClass = e.getClass.getName
    val functionName = orElse(function.name(), functionClass)
    if (failuresWarnedAbout.add(s"$functionClass:$failureClass")) {
      logWarning(log"Cannot resolve a call of the partition transform " +
        log"${MDC(FUNCTION_NAME, functionName)} " +
        log"(${MDC(CLASS_NAME, functionClass)}), so a storage-partitioned join " +
        log"does not evaluate it: ${MDC(ERROR, orElse(e.toString, failureClass))}")
    }
    logDebug(log"Cannot resolve a call of the partition transform " +
      log"${MDC(FUNCTION_NAME, functionName)}", e)
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

  /**
   * `argument` at `declaredType`, or `None` when the function could not take it. See
   * [[TransformExpression.evaluableFunction]].
   *
   * An argument goes as it is when it is stored the way the declared type is (`storedAlike`),
   * which is how a function read it before this check. A narrower integral argument is held as
   * another JVM type, so it is cast. Anything else is refused, a `DATE` against a `TIMESTAMP` in
   * particular, whose cast takes the session time zone.
   */
  private def atDeclaredType(argument: Expression, declaredType: DataType): Option[Expression] =
    (argument.dataType, declaredType) match {
      case (argumentType, _) if storedAlike(argumentType, declaredType, nested = false) =>
        Some(argument)
      case (argumentType: IntegralType, declared: IntegralType)
          if UpCastRule.legalNumericPrecedence(argumentType, declared) =>
        Some(Cast(argument, declaredType))
      case _ =>
        None
    }

  /**
   * Whether a value of type `from` is stored the way one of type `to` is, so that a function
   * declaring `to` reads it as it is:
   *  - two types with one physical type, e.g. a `VARCHAR` and a string of the same collation, a
   *    `DATE` and an `INT`, or a `TIMESTAMP` and a `BIGINT`. A user-defined type is stored as its
   *    `sqlType`;
   *  - two structs, arrays or maps whose fields, elements, keys and values are stored alike, by
   *    position. Field names and nullability do not change how a value is stored. A function
   *    declaring a non-nullable field gets the values a nullable one holds;
   *  - two decimals of one scale, `to` with no less precision, so that the unscaled digits a
   *    function reads are the same. A decimal is read as an object at the top level, but nested in
   *    an unsafe row it is stored as a `long` up to `Decimal.MAX_LONG_DIGITS` digits and as bytes
   *    above, so there both have to be on one side of that.
   */
  private def storedAlike(from: DataType, to: DataType, nested: Boolean): Boolean =
    (from, to) match {
      case _ if from == to => true
      case (udt: UserDefinedType[_], _) => storedAlike(udt.sqlType, to, nested)
      case (_, udt: UserDefinedType[_]) => storedAlike(from, udt.sqlType, nested)
      case (f: StructType, t: StructType) =>
        f.length == t.length && f.fields.zip(t.fields).forall { case (ff, tf) =>
          storedAlike(ff.dataType, tf.dataType, nested = true)
        }
      case (f: ArrayType, t: ArrayType) => storedAlike(f.elementType, t.elementType, nested = true)
      case (f: MapType, t: MapType) =>
        storedAlike(f.keyType, t.keyType, nested = true) &&
          storedAlike(f.valueType, t.valueType, nested = true)
      case (f: DecimalType, t: DecimalType) =>
        f.scale == t.scale && f.precision <= t.precision &&
          (!nested ||
            DecimalType.isByteArrayDecimalType(f) == DecimalType.isByteArrayDecimalType(t))
      case _ =>
        val physical = PhysicalDataType(from)
        physical != UninitializedPhysicalType && physical == PhysicalDataType(to)
    }
}
