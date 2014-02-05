package catalyst
package expressions

import catalyst.types._

abstract class AggregateExpression extends Expression {
  self: Product =>

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
 * An [[AggregateExpression]] that can be partially computed without seeing all relevent tuples.
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

  /** Base should return the generic aggregate expression that this function is computing */
  val base: AggregateExpression
  def references = base.references
  def nullable = base.nullable
  def dataType = base.dataType

  def apply(input: Seq[Row]): Unit
  def result: Any
}

case class Count(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = IntegerType
  override def toString = s"COUNT($child)"

  def asPartial: SplitEvaluation = {
    val partialCount = Alias(Count(child), "PartialCount")()
    SplitEvaluation(Sum(partialCount.toAttribute), partialCount :: Nil)
  }
}

case class CountDistinct(expressions: Seq[Expression]) extends AggregateExpression {
  def children = expressions
  def references = expressions.flatMap(_.references).toSet
  def nullable = false
  def dataType = IntegerType
  override def toString = s"COUNT(DISTINCT ${expressions.mkString(",")}})"
}

case class Average(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = DoubleType
  override def toString = s"AVG($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(Sum(child), "PartialSum")()
    val partialCount = Alias(Count(child), "PartialCount")()
    val castedSum = Cast(Sum(partialSum.toAttribute), dataType)
    val castedCount = Cast(Sum(partialCount.toAttribute), dataType)

    SplitEvaluation(
      Divide(castedSum, castedCount),
      partialCount :: partialSum :: Nil)
  }
}

case class Sum(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = false
  def dataType = child.dataType
  override def toString = s"SUM($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(Sum(child), "PartialSum")()
    SplitEvaluation(
      Sum(partialSum.toAttribute),
      partialSum :: Nil)
  }
}

case class First(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  def references = child.references
  def nullable = child.nullable
  def dataType = child.dataType
  override def toString = s"FIRST($child)"

  override def asPartial: SplitEvaluation = {
    val partialFirst = Alias(First(child), "PartialFirst")()
    SplitEvaluation(
      First(partialFirst.toAttribute),
      partialFirst :: Nil)
  }
}
