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

import com.clearspring.analytics.stream.cardinality.HyperLogLog

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashSet


trait AggregateExpression extends Expression with Unevaluable

trait AggregateExpression1 extends AggregateExpression {

  /**
   * Aggregate expressions should not be foldable.
   */
  override def foldable: Boolean = false

  /**
   * Creates a new instance that can be used to compute this aggregate expression for a group
   * of input rows/
   */
  def newInstance(): AggregateFunction1
}

/**
 * Represents an aggregation that has been rewritten to be performed in two steps.
 *
 * @param finalEvaluation an aggregate expression that evaluates to same final result as the
 *                        original aggregation.
 * @param partialEvaluations A sequence of [[NamedExpression]]s that can be computed on partial
 *                           data sets and are required to compute the `finalEvaluation`.
 */
case class SplitEvaluation(
    finalEvaluation: Expression,
    partialEvaluations: Seq[NamedExpression])

/**
 * An [[AggregateExpression1]] that can be partially computed without seeing all relevant tuples.
 * These partial evaluations can then be combined to compute the actual answer.
 */
trait PartialAggregate1 extends AggregateExpression1 {

  /**
   * Returns a [[SplitEvaluation]] that computes this aggregation using partial aggregation.
   */
  def asPartial: SplitEvaluation
}

/**
 * A specific implementation of an aggregate function. Used to wrap a generic
 * [[AggregateExpression1]] with an algorithm that will be used to compute one specific result.
 */
abstract class AggregateFunction1 extends LeafExpression with Serializable {

  /** Base should return the generic aggregate expression that this function is computing */
  val base: AggregateExpression1

  override def nullable: Boolean = base.nullable
  override def dataType: DataType = base.dataType

  def update(input: InternalRow): Unit

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    throw new UnsupportedOperationException(
      "AggregateFunction1 should not be used for generated aggregates")
  }
}

case class Min(child: Expression) extends UnaryExpression with PartialAggregate1 {

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType

  override def asPartial: SplitEvaluation = {
    val partialMin = Alias(Min(child), "PartialMin")()
    SplitEvaluation(Min(partialMin.toAttribute), partialMin :: Nil)
  }

  override def newInstance(): MinFunction = new MinFunction(child, this)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function min")
}

case class MinFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  val currentMin: MutableLiteral = MutableLiteral(null, expr.dataType)
  val cmp = GreaterThan(currentMin, expr)

  override def update(input: InternalRow): Unit = {
    if (currentMin.value == null) {
      currentMin.value = expr.eval(input)
    } else if (cmp.eval(input) == true) {
      currentMin.value = expr.eval(input)
    }
  }

  override def eval(input: InternalRow): Any = currentMin.value
}

case class Max(child: Expression) extends UnaryExpression with PartialAggregate1 {

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType

  override def asPartial: SplitEvaluation = {
    val partialMax = Alias(Max(child), "PartialMax")()
    SplitEvaluation(Max(partialMax.toAttribute), partialMax :: Nil)
  }

  override def newInstance(): MaxFunction = new MaxFunction(child, this)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function max")
}

case class MaxFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  val currentMax: MutableLiteral = MutableLiteral(null, expr.dataType)
  val cmp = LessThan(currentMax, expr)

  override def update(input: InternalRow): Unit = {
    if (currentMax.value == null) {
      currentMax.value = expr.eval(input)
    } else if (cmp.eval(input) == true) {
      currentMax.value = expr.eval(input)
    }
  }

  override def eval(input: InternalRow): Any = currentMax.value
}

case class Count(child: Expression) extends UnaryExpression with PartialAggregate1 {

  override def nullable: Boolean = false
  override def dataType: LongType.type = LongType

  override def asPartial: SplitEvaluation = {
    val partialCount = Alias(Count(child), "PartialCount")()
    SplitEvaluation(Coalesce(Seq(Sum(partialCount.toAttribute), Literal(0L))), partialCount :: Nil)
  }

  override def newInstance(): CountFunction = new CountFunction(child, this)
}

case class CountFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  var count: Long = _

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      count += 1L
    }
  }

  override def eval(input: InternalRow): Any = count
}

case class CountDistinct(expressions: Seq[Expression]) extends PartialAggregate1 {
  def this() = this(null)

  override def children: Seq[Expression] = expressions

  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def toString: String = s"COUNT(DISTINCT ${expressions.mkString(",")})"
  override def newInstance(): CountDistinctFunction = new CountDistinctFunction(expressions, this)

  override def asPartial: SplitEvaluation = {
    val partialSet = Alias(CollectHashSet(expressions), "partialSets")()
    SplitEvaluation(
      CombineSetsAndCount(partialSet.toAttribute),
      partialSet :: Nil)
  }
}

case class CountDistinctFunction(
    @transient expr: Seq[Expression],
    @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  @transient
  val distinctValue = new InterpretedProjection(expr)

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = distinctValue(input)
    if (!evaluatedExpr.anyNull) {
      seen.add(evaluatedExpr)
    }
  }

  override def eval(input: InternalRow): Any = seen.size.toLong
}

case class CollectHashSet(expressions: Seq[Expression]) extends AggregateExpression1 {
  def this() = this(null)

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: OpenHashSetUDT = new OpenHashSetUDT(expressions.head.dataType)
  override def toString: String = s"AddToHashSet(${expressions.mkString(",")})"
  override def newInstance(): CollectHashSetFunction =
    new CollectHashSetFunction(expressions, this)
}

case class CollectHashSetFunction(
    @transient expr: Seq[Expression],
    @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  @transient
  val distinctValue = new InterpretedProjection(expr)

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = distinctValue(input)
    if (!evaluatedExpr.anyNull) {
      seen.add(evaluatedExpr)
    }
  }

  override def eval(input: InternalRow): Any = {
    seen
  }
}

case class CombineSetsAndCount(inputSet: Expression) extends AggregateExpression1 {
  def this() = this(null)

  override def children: Seq[Expression] = inputSet :: Nil
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def toString: String = s"CombineAndCount($inputSet)"
  override def newInstance(): CombineSetsAndCountFunction = {
    new CombineSetsAndCountFunction(inputSet, this)
  }
}

case class CombineSetsAndCountFunction(
    @transient inputSet: Expression,
    @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  override def update(input: InternalRow): Unit = {
    val inputSetEval = inputSet.eval(input).asInstanceOf[OpenHashSet[Any]]
    val inputIterator = inputSetEval.iterator
    while (inputIterator.hasNext) {
      seen.add(inputIterator.next)
    }
  }

  override def eval(input: InternalRow): Any = seen.size.toLong
}

/** The data type of ApproxCountDistinctPartition since its output is a HyperLogLog object. */
private[sql] case object HyperLogLogUDT extends UserDefinedType[HyperLogLog] {

  override def sqlType: DataType = BinaryType

  /** Since we are using HyperLogLog internally, usually it will not be called. */
  override def serialize(obj: Any): Array[Byte] =
    obj.asInstanceOf[HyperLogLog].getBytes


  /** Since we are using HyperLogLog internally, usually it will not be called. */
  override def deserialize(datum: Any): HyperLogLog =
    HyperLogLog.Builder.build(datum.asInstanceOf[Array[Byte]])

  override def userClass: Class[HyperLogLog] = classOf[HyperLogLog]
}

case class ApproxCountDistinctPartition(child: Expression, relativeSD: Double)
  extends UnaryExpression with AggregateExpression1 {

  override def nullable: Boolean = false
  override def dataType: DataType = HyperLogLogUDT
  override def toString: String = s"APPROXIMATE COUNT(DISTINCT $child)"
  override def newInstance(): ApproxCountDistinctPartitionFunction = {
    new ApproxCountDistinctPartitionFunction(child, this, relativeSD)
  }
}

case class ApproxCountDistinctPartitionFunction(
    expr: Expression,
    base: AggregateExpression1,
    relativeSD: Double)
  extends AggregateFunction1 {
  def this() = this(null, null, 0) // Required for serialization.

  private val hyperLogLog = new HyperLogLog(relativeSD)

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      hyperLogLog.offer(evaluatedExpr)
    }
  }

  override def eval(input: InternalRow): Any = hyperLogLog
}

case class ApproxCountDistinctMerge(child: Expression, relativeSD: Double)
  extends UnaryExpression with AggregateExpression1 {

  override def nullable: Boolean = false
  override def dataType: LongType.type = LongType
  override def toString: String = s"APPROXIMATE COUNT(DISTINCT $child)"
  override def newInstance(): ApproxCountDistinctMergeFunction = {
    new ApproxCountDistinctMergeFunction(child, this, relativeSD)
  }
}

case class ApproxCountDistinctMergeFunction(
    expr: Expression,
    base: AggregateExpression1,
    relativeSD: Double)
  extends AggregateFunction1 {
  def this() = this(null, null, 0) // Required for serialization.

  private val hyperLogLog = new HyperLogLog(relativeSD)

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    hyperLogLog.addAll(evaluatedExpr.asInstanceOf[HyperLogLog])
  }

  override def eval(input: InternalRow): Any = hyperLogLog.cardinality()
}

case class ApproxCountDistinct(child: Expression, relativeSD: Double = 0.05)
  extends UnaryExpression with PartialAggregate1 {

  override def nullable: Boolean = false
  override def dataType: LongType.type = LongType
  override def toString: String = s"APPROXIMATE COUNT(DISTINCT $child)"

  override def asPartial: SplitEvaluation = {
    val partialCount =
      Alias(ApproxCountDistinctPartition(child, relativeSD), "PartialApproxCountDistinct")()

    SplitEvaluation(
      ApproxCountDistinctMerge(partialCount.toAttribute, relativeSD),
      partialCount :: Nil)
  }

  override def newInstance(): CountDistinctFunction = new CountDistinctFunction(child :: Nil, this)
}

case class Average(child: Expression) extends UnaryExpression with PartialAggregate1 {

  override def prettyName: String = "avg"

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      // Add 4 digits after decimal point, like Hive
      DecimalType.bounded(precision + 4, scale + 4)
    case _ =>
      DoubleType
  }

  override def asPartial: SplitEvaluation = {
    child.dataType match {
      case DecimalType.Fixed(precision, scale) =>
        val partialSum = Alias(Sum(child), "PartialSum")()
        val partialCount = Alias(Count(child), "PartialCount")()

        // partialSum already increase the precision by 10
        val castedSum = Cast(Sum(partialSum.toAttribute), partialSum.dataType)
        val castedCount = Cast(Sum(partialCount.toAttribute), partialSum.dataType)
        SplitEvaluation(
          Cast(Divide(castedSum, castedCount), dataType),
          partialCount :: partialSum :: Nil)

      case _ =>
        val partialSum = Alias(Sum(child), "PartialSum")()
        val partialCount = Alias(Count(child), "PartialCount")()

        val castedSum = Cast(Sum(partialSum.toAttribute), dataType)
        val castedCount = Cast(Sum(partialCount.toAttribute), dataType)
        SplitEvaluation(
          Divide(castedSum, castedCount),
          partialCount :: partialSum :: Nil)
    }
  }

  override def newInstance(): AverageFunction = new AverageFunction(child, this)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function average")
}

case class AverageFunction(expr: Expression, base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  private val calcType =
    expr.dataType match {
      case DecimalType.Fixed(precision, scale) =>
        DecimalType.bounded(precision + 10, scale)
      case _ =>
        expr.dataType
    }

  private val zero = Cast(Literal(0), calcType)

  private var count: Long = _
  private val sum = MutableLiteral(zero.eval(null), calcType)

  private def addFunction(value: Any) = Add(sum,
    Cast(Literal.create(value, expr.dataType), calcType))

  override def eval(input: InternalRow): Any = {
    if (count == 0L) {
      null
    } else {
      expr.dataType match {
        case DecimalType.Fixed(precision, scale) =>
          val dt = DecimalType.bounded(precision + 14, scale + 4)
          Cast(Divide(Cast(sum, dt), Cast(Literal(count), dt)), dataType).eval(null)
        case _ =>
          Divide(
            Cast(sum, dataType),
            Cast(Literal(count), dataType)).eval(null)
      }
    }
  }

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      count += 1
      sum.update(addFunction(evaluatedExpr), input)
    }
  }
}

case class Sum(child: Expression) extends UnaryExpression with PartialAggregate1 {

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      // Add 10 digits left of decimal point, like Hive
      DecimalType.bounded(precision + 10, scale)
    case _ =>
      child.dataType
  }

  override def asPartial: SplitEvaluation = {
    child.dataType match {
      case DecimalType.Fixed(_, _) =>
        val partialSum = Alias(Sum(child), "PartialSum")()
        SplitEvaluation(
          Cast(Sum(partialSum.toAttribute), dataType),
          partialSum :: Nil)

      case _ =>
        val partialSum = Alias(Sum(child), "PartialSum")()
        SplitEvaluation(
          Sum(partialSum.toAttribute),
          partialSum :: Nil)
    }
  }

  override def newInstance(): SumFunction = new SumFunction(child, this)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")
}

case class SumFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  private val calcType =
    expr.dataType match {
      case DecimalType.Fixed(precision, scale) =>
        DecimalType.bounded(precision + 10, scale)
      case _ =>
        expr.dataType
    }

  private val zero = Cast(Literal(0), calcType)

  private val sum = MutableLiteral(null, calcType)

  private val addFunction = Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(expr, calcType)), sum))

  override def update(input: InternalRow): Unit = {
    sum.update(addFunction, input)
  }

  override def eval(input: InternalRow): Any = {
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        Cast(sum, dataType).eval(null)
      case _ => sum.eval(null)
    }
  }
}

case class SumDistinct(child: Expression) extends UnaryExpression with PartialAggregate1 {

  def this() = this(null)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      // Add 10 digits left of decimal point, like Hive
      DecimalType.bounded(precision + 10, scale)
    case _ =>
      child.dataType
  }
  override def toString: String = s"SUM(DISTINCT $child)"
  override def newInstance(): SumDistinctFunction = new SumDistinctFunction(child, this)

  override def asPartial: SplitEvaluation = {
    val partialSet = Alias(CollectHashSet(child :: Nil), "partialSets")()
    SplitEvaluation(
      CombineSetsAndSum(partialSet.toAttribute, this),
      partialSet :: Nil)
  }

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sumDistinct")
}

case class SumDistinctFunction(expr: Expression, base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  private val seen = new scala.collection.mutable.HashSet[Any]()

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      seen += evaluatedExpr
    }
  }

  override def eval(input: InternalRow): Any = {
    if (seen.size == 0) {
      null
    } else {
      Cast(Literal(
        seen.reduceLeft(
          dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].plus)),
        dataType).eval(null)
    }
  }
}

case class CombineSetsAndSum(inputSet: Expression, base: Expression) extends AggregateExpression1 {
  def this() = this(null, null)

  override def children: Seq[Expression] = inputSet :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = base.dataType
  override def toString: String = s"CombineAndSum($inputSet)"
  override def newInstance(): CombineSetsAndSumFunction = {
    new CombineSetsAndSumFunction(inputSet, this)
  }
}

case class CombineSetsAndSumFunction(
    @transient inputSet: Expression,
    @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  override def update(input: InternalRow): Unit = {
    val inputSetEval = inputSet.eval(input).asInstanceOf[OpenHashSet[Any]]
    val inputIterator = inputSetEval.iterator
    while (inputIterator.hasNext) {
      seen.add(inputIterator.next())
    }
  }

  override def eval(input: InternalRow): Any = {
    val casted = seen.asInstanceOf[OpenHashSet[InternalRow]]
    if (casted.size == 0) {
      null
    } else {
      Cast(Literal(
        casted.iterator.map(f => f.get(0, null)).reduceLeft(
          base.dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].plus)),
        base.dataType).eval(null)
    }
  }
}

case class First(child: Expression) extends UnaryExpression with PartialAggregate1 {
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"FIRST($child)"

  override def asPartial: SplitEvaluation = {
    val partialFirst = Alias(First(child), "PartialFirst")()
    SplitEvaluation(
      First(partialFirst.toAttribute),
      partialFirst :: Nil)
  }
  override def newInstance(): FirstFunction = new FirstFunction(child, this)
}

case class FirstFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  override def update(input: InternalRow): Unit = {
    // We ignore null values.
    if (result == null) {
      result = expr.eval(input)
    }
  }

  override def eval(input: InternalRow): Any = result
}

case class Last(child: Expression) extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"LAST($child)"

  override def asPartial: SplitEvaluation = {
    val partialLast = Alias(Last(child), "PartialLast")()
    SplitEvaluation(
      Last(partialLast.toAttribute),
      partialLast :: Nil)
  }
  override def newInstance(): LastFunction = new LastFunction(child, this)
}

case class LastFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  override def update(input: InternalRow): Unit = {
    val value = expr.eval(input)
    // We ignore null values.
    if (value != null) {
      result = value
    }
  }

  override def eval(input: InternalRow): Any = {
    result
  }
}
