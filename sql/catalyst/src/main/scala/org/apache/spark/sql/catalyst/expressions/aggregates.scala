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

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.util.collection.OpenHashSet

abstract class AggregateExpression extends Expression {
  self: Product =>

  /**
   * Creates a new instance that can be used to compute this aggregate expression for a group
   * of input rows/
   */
  def newInstance(): AggregateFunction

  /**
   * [[AggregateExpression.eval]] should never be invoked because [[AggregateExpression]]'s are
   * replaced with a physical aggregate operator at runtime.
   */
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
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
 * An [[AggregateExpression]] that can be partially computed without seeing all relevant tuples.
 * These partial evaluations can then be combined to compute the actual answer.
 */
abstract class PartialAggregate extends AggregateExpression {
  self: Product =>

  /**
   * Returns a [[SplitEvaluation]] that computes this aggregation using partial aggregation.
   */
  def asPartial: SplitEvaluation
}

/**
 * A specific implementation of an aggregate function. Used to wrap a generic
 * [[AggregateExpression]] with an algorithm that will be used to compute one specific result.
 */
abstract class AggregateFunction
  extends AggregateExpression with Serializable with trees.LeafNode[Expression] {
  self: Product =>

  override type EvaluatedType = Any

  /** Base should return the generic aggregate expression that this function is computing */
  val base: AggregateExpression

  override def nullable = base.nullable
  override def dataType = base.dataType

  def update(input: Row): Unit

  // Do we really need this?
  override def newInstance() = makeCopy(productIterator.map { case a: AnyRef => a }.toArray)
}

case class Min(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"MIN($child)"

  override def asPartial: SplitEvaluation = {
    val partialMin = Alias(Min(child), "PartialMin")()
    SplitEvaluation(Min(partialMin.toAttribute), partialMin :: Nil)
  }

  override def newInstance() = new MinFunction(child, this)
}

case class MinFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  val currentMin: MutableLiteral = MutableLiteral(null, expr.dataType)
  val cmp = GreaterThan(currentMin, expr)

  override def update(input: Row): Unit = {
    if (currentMin.value == null) {
      currentMin.value = expr.eval(input)
    } else if(cmp.eval(input) == true) {
      currentMin.value = expr.eval(input)
    }
  }

  override def eval(input: Row): Any = currentMin.value
}

case class Max(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"MAX($child)"

  override def asPartial: SplitEvaluation = {
    val partialMax = Alias(Max(child), "PartialMax")()
    SplitEvaluation(Max(partialMax.toAttribute), partialMax :: Nil)
  }

  override def newInstance() = new MaxFunction(child, this)
}

case class MaxFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  val currentMax: MutableLiteral = MutableLiteral(null, expr.dataType)
  val cmp = LessThan(currentMax, expr)

  override def update(input: Row): Unit = {
    if (currentMax.value == null) {
      currentMax.value = expr.eval(input)
    } else if(cmp.eval(input) == true) {
      currentMax.value = expr.eval(input)
    }
  }

  override def eval(input: Row): Any = currentMax.value
}

case class Count(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = false
  override def dataType = LongType
  override def toString = s"COUNT($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount = Alias(Count(child), "PartialCount")()
    SplitEvaluation(Coalesce(Seq(Sum(partialCount.toAttribute), Literal(0L))), partialCount :: Nil)
  }

  override def newInstance() = new CountFunction(child, this)
}

case class CountDistinct(expressions: Seq[Expression]) extends PartialAggregate {
  def this() = this(null)

  override def children = expressions

  override def nullable = false
  override def dataType = LongType
  override def toString = s"COUNT(DISTINCT ${expressions.mkString(",")})"
  override def newInstance() = new CountDistinctFunction(expressions, this)

  override def asPartial = {
    val partialSet = Alias(CollectHashSet(expressions), "partialSets")()
    SplitEvaluation(
      CombineSetsAndCount(partialSet.toAttribute),
      partialSet :: Nil)
  }
}

case class CollectHashSet(expressions: Seq[Expression]) extends AggregateExpression {
  def this() = this(null)

  override def children = expressions
  override def nullable = false
  override def dataType = ArrayType(expressions.head.dataType)
  override def toString = s"AddToHashSet(${expressions.mkString(",")})"
  override def newInstance() = new CollectHashSetFunction(expressions, this)
}

case class CollectHashSetFunction(
    @transient expr: Seq[Expression],
    @transient base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  @transient
  val distinctValue = new InterpretedProjection(expr)

  override def update(input: Row): Unit = {
    val evaluatedExpr = distinctValue(input)
    if (!evaluatedExpr.anyNull) {
      seen.add(evaluatedExpr)
    }
  }

  override def eval(input: Row): Any = {
    seen
  }
}

case class CombineSetsAndCount(inputSet: Expression) extends AggregateExpression {
  def this() = this(null)

  override def children = inputSet :: Nil
  override def nullable = false
  override def dataType = LongType
  override def toString = s"CombineAndCount($inputSet)"
  override def newInstance() = new CombineSetsAndCountFunction(inputSet, this)
}

case class CombineSetsAndCountFunction(
    @transient inputSet: Expression,
    @transient base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  override def update(input: Row): Unit = {
    val inputSetEval = inputSet.eval(input).asInstanceOf[OpenHashSet[Any]]
    val inputIterator = inputSetEval.iterator
    while (inputIterator.hasNext) {
      seen.add(inputIterator.next)
    }
  }

  override def eval(input: Row): Any = seen.size.toLong
}

case class ApproxCountDistinctPartition(child: Expression, relativeSD: Double)
  extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable = false
  override def dataType = child.dataType
  override def toString = s"APPROXIMATE COUNT(DISTINCT $child)"
  override def newInstance() = new ApproxCountDistinctPartitionFunction(child, this, relativeSD)
}

case class ApproxCountDistinctMerge(child: Expression, relativeSD: Double)
  extends AggregateExpression with trees.UnaryNode[Expression] {

  override def nullable = false
  override def dataType = LongType
  override def toString = s"APPROXIMATE COUNT(DISTINCT $child)"
  override def newInstance() = new ApproxCountDistinctMergeFunction(child, this, relativeSD)
}

case class ApproxCountDistinct(child: Expression, relativeSD: Double = 0.05)
  extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = false
  override def dataType = LongType
  override def toString = s"APPROXIMATE COUNT(DISTINCT $child)"

  override def asPartial: SplitEvaluation = {
    val partialCount =
      Alias(ApproxCountDistinctPartition(child, relativeSD), "PartialApproxCountDistinct")()

    SplitEvaluation(
      ApproxCountDistinctMerge(partialCount.toAttribute, relativeSD),
      partialCount :: Nil)
  }

  override def newInstance() = new CountDistinctFunction(child :: Nil, this)
}

case class Average(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = true

  override def dataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 4, scale + 4)  // Add 4 digits after decimal point, like Hive
    case DecimalType.Unlimited =>
      DecimalType.Unlimited
    case _ =>
      DoubleType
  }

  override def toString = s"AVG($child)"

  override def asPartial: SplitEvaluation = {
    child.dataType match {
      case DecimalType.Fixed(_, _) =>
        // Turn the child to unlimited decimals for calculation, before going back to fixed
        val partialSum = Alias(Sum(Cast(child, DecimalType.Unlimited)), "PartialSum")()
        val partialCount = Alias(Count(child), "PartialCount")()

        val castedSum = Cast(Sum(partialSum.toAttribute), DecimalType.Unlimited)
        val castedCount = Cast(Sum(partialCount.toAttribute), DecimalType.Unlimited)
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

  override def newInstance() = new AverageFunction(child, this)
}

case class Sum(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {

  override def nullable = true

  override def dataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 10, scale)  // Add 10 digits left of decimal point, like Hive
    case DecimalType.Unlimited =>
      DecimalType.Unlimited
    case _ =>
      child.dataType
  }

  override def toString = s"SUM($child)"

  override def asPartial: SplitEvaluation = {
    child.dataType match {
      case DecimalType.Fixed(_, _) =>
        val partialSum = Alias(Sum(Cast(child, DecimalType.Unlimited)), "PartialSum")()
        SplitEvaluation(
          Cast(CombineSum(partialSum.toAttribute), dataType),
          partialSum :: Nil)

      case _ =>
        val partialSum = Alias(Sum(child), "PartialSum")()
        SplitEvaluation(
          CombineSum(partialSum.toAttribute),
          partialSum :: Nil)
    }
  }

  override def newInstance() = new SumFunction(child, this)
}

/**
 * Sum should satisfy 3 cases:
 * 1) sum of all null values = zero
 * 2) sum for table column with no data = null
 * 3) sum of column with null and not null values = sum of not null values
 * Require separate CombineSum Expression and function as it has to distinguish "No data" case
 * versus "data equals null" case, while aggregating results and at each partial expression.i.e.,
 * Combining    PartitionLevel   InputData
 *                           <-- null
 * Zero     <-- Zero         <-- null
 *                              
 *          <-- null         <-- no data
 * null     <-- null         <-- no data 
 */
case class CombineSum(child: Expression) extends AggregateExpression {
  def this() = this(null)
  
  override def children = child :: Nil
  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"CombineSum($child)"
  override def newInstance() = new CombineSumFunction(child, this)
}

case class SumDistinct(child: Expression)
  extends PartialAggregate with trees.UnaryNode[Expression] {

  def this() = this(null)
  override def nullable = true
  override def dataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 10, scale)  // Add 10 digits left of decimal point, like Hive
    case DecimalType.Unlimited =>
      DecimalType.Unlimited
    case _ =>
      child.dataType
  }
  override def toString = s"SUM(DISTINCT ${child})"
  override def newInstance() = new SumDistinctFunction(child, this)

  override def asPartial = {
    val partialSet = Alias(CollectHashSet(child :: Nil), "partialSets")()
    SplitEvaluation(
      CombineSetsAndSum(partialSet.toAttribute, this),
      partialSet :: Nil)
  }
}

case class CombineSetsAndSum(inputSet: Expression, base: Expression) extends AggregateExpression {
  def this() = this(null, null)

  override def children = inputSet :: Nil
  override def nullable = true
  override def dataType = base.dataType
  override def toString = s"CombineAndSum($inputSet)"
  override def newInstance() = new CombineSetsAndSumFunction(inputSet, this)
}

case class CombineSetsAndSumFunction(
    @transient inputSet: Expression,
    @transient base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  override def update(input: Row): Unit = {
    val inputSetEval = inputSet.eval(input).asInstanceOf[OpenHashSet[Any]]
    val inputIterator = inputSetEval.iterator
    while (inputIterator.hasNext) {
      seen.add(inputIterator.next)
    }
  }

  override def eval(input: Row): Any = {
    val casted = seen.asInstanceOf[OpenHashSet[Row]]
    if (casted.size == 0) {
      null
    } else {
      Cast(Literal(
        casted.iterator.map(f => f.apply(0)).reduceLeft(
          base.dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].plus)),
        base.dataType).eval(null)
    }
  }
}

case class First(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"FIRST($child)"

  override def asPartial: SplitEvaluation = {
    val partialFirst = Alias(First(child), "PartialFirst")()
    SplitEvaluation(
      First(partialFirst.toAttribute),
      partialFirst :: Nil)
  }
  override def newInstance() = new FirstFunction(child, this)
}

case class Last(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"LAST($child)"

  override def asPartial: SplitEvaluation = {
    val partialLast = Alias(Last(child), "PartialLast")()
    SplitEvaluation(
      Last(partialLast.toAttribute),
      partialLast :: Nil)
  }
  override def newInstance() = new LastFunction(child, this)
}

case class AverageFunction(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  private val calcType =
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        DecimalType.Unlimited
      case _ =>
        expr.dataType
    }

  private val zero = Cast(Literal(0), calcType)

  private var count: Long = _
  private val sum = MutableLiteral(zero.eval(null), calcType)

  private def addFunction(value: Any) = Add(sum, Cast(Literal(value, expr.dataType), calcType))

  override def eval(input: Row): Any = {
    if (count == 0L) {
      null
    } else {
      expr.dataType match {
        case DecimalType.Fixed(_, _) =>
          Cast(Divide(
            Cast(sum, DecimalType.Unlimited),
            Cast(Literal(count), DecimalType.Unlimited)), dataType).eval(null)
        case _ =>
          Divide(
            Cast(sum, dataType),
            Cast(Literal(count), dataType)).eval(null)
      }
    }
  }

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      count += 1
      sum.update(addFunction(evaluatedExpr), input)
    }
  }
}

case class CountFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var count: Long = _

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      count += 1L
    }
  }

  override def eval(input: Row): Any = count
}

case class ApproxCountDistinctPartitionFunction(
    expr: Expression,
    base: AggregateExpression,
    relativeSD: Double)
  extends AggregateFunction {
  def this() = this(null, null, 0) // Required for serialization.

  private val hyperLogLog = new HyperLogLog(relativeSD)

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      hyperLogLog.offer(evaluatedExpr)
    }
  }

  override def eval(input: Row): Any = hyperLogLog
}

case class ApproxCountDistinctMergeFunction(
    expr: Expression,
    base: AggregateExpression,
    relativeSD: Double)
  extends AggregateFunction {
  def this() = this(null, null, 0) // Required for serialization.

  private val hyperLogLog = new HyperLogLog(relativeSD)

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    hyperLogLog.addAll(evaluatedExpr.asInstanceOf[HyperLogLog])
  }

  override def eval(input: Row): Any = hyperLogLog.cardinality()
}

case class SumFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  private val calcType =
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        DecimalType.Unlimited
      case _ =>
        expr.dataType
    }

  private val zero = Cast(Literal(0), calcType)

  private val sum = MutableLiteral(null, calcType)

  private val addFunction = 
    Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(expr, calcType)), sum, zero))

  override def update(input: Row): Unit = {
    sum.update(addFunction, input)
  }

  override def eval(input: Row): Any = {
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        Cast(sum, dataType).eval(null)
      case _ => sum.eval(null)
    }
  }
}

case class CombineSumFunction(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {
  
  def this() = this(null, null) // Required for serialization.

  private val calcType =
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        DecimalType.Unlimited
      case _ =>
        expr.dataType
    }

  private val zero = Cast(Literal(0), calcType)

  private val sum = MutableLiteral(null, calcType)

  private val addFunction = 
    Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(expr, calcType)), sum, zero))
  
  override def update(input: Row): Unit = {
    val result = expr.eval(input)
    // partial sum result can be null only when no input rows present 
    if(result != null) {
      sum.update(addFunction, input)
    }
  }

  override def eval(input: Row): Any = {
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        Cast(sum, dataType).eval(null)
      case _ => sum.eval(null)
    }
  }
}

case class SumDistinctFunction(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  private val seen = new scala.collection.mutable.HashSet[Any]()

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      seen += evaluatedExpr
    }
  }

  override def eval(input: Row): Any = {
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

case class CountDistinctFunction(
    @transient expr: Seq[Expression],
    @transient base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val seen = new OpenHashSet[Any]()

  @transient
  val distinctValue = new InterpretedProjection(expr)

  override def update(input: Row): Unit = {
    val evaluatedExpr = distinctValue(input)
    if (!evaluatedExpr.anyNull) {
      seen.add(evaluatedExpr)
    }
  }

  override def eval(input: Row): Any = seen.size.toLong
}

case class FirstFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  override def update(input: Row): Unit = {
    if (result == null) {
      result = expr.eval(input)
    }
  }

  override def eval(input: Row): Any = result
}

case class LastFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  override def update(input: Row): Unit = {
    result = input
  }

  override def eval(input: Row): Any =  if (result != null) expr.eval(result.asInstanceOf[Row])
                                        else null
}
