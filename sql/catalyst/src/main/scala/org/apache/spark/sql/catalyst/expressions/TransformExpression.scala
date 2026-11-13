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

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SparkThrowable, SparkThrowableHelper}
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{ARGS, CLASS_NAME, ERROR, FUNCTION_NAME, FUNCTION_PARAM}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, UninitializedPhysicalType}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StructType, UpCastRule, UserDefinedType}

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
