package catalyst
package execution

import catalyst.errors._
import catalyst.expressions._
import org.apache.spark.rdd.SharkPairRDDFunctions

/* Implicits */
import org.apache.spark.SparkContext._
import SharkPairRDDFunctions._

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SharkPlan)
  extends UnaryNode {

  val requiredPartitioning = ClusteredDistribution(groupingExpressions)
  override def requiredChildPartitioning = Seq(requiredPartitioning, requiredPartitioning)

  def output = aggregateExpressions.map(_.toAttribute)

  def execute() = attachTree(this, "execute") {
    // TODO: If the child of it is an [[catalyst.execution.Exchange]],
    // do not evaluate the groupingExpressions again since we have evaluated it
    // in the [[catalyst.execution.Exchange]].
    val grouped = child.execute().map { row =>
      (buildRow(groupingExpressions.map(Evaluate(_, Vector(row)))), row)
    }.groupByKeyLocally()

    grouped.map { case (group, rows) =>
      // Replace all aggregate expressions with spark functions that will compute the result.
      val aggImplementations = aggregateExpressions.map { agg =>
        val impl = agg transform {
          case base @ Average(expr) => new AverageFunction(expr, base)
          case base @ Sum(expr) => new SumFunction(expr, base)
          case base @ Count(expr) => new CountFunction(expr, base)
          case base @ CountDistinct(expr) => new CountDistinctFunction(expr, base)
          case base @ First(expr) => new FirstFunction(expr, base)
        }

        val remainingAttributes = impl.collect { case a: Attribute => a }
        // If any references exist that are not inside agg functions then the must be grouping exprs
        // in this case we must rebind them to the grouping tuple.
        // TODO: Is this right still? Do we need this?
        if (remainingAttributes.nonEmpty) {
          val unaliasedAggregateExpr = agg transform { case Alias(c, _) => c }

          // An exact match with a grouping expression
          val exactGroupingExpr = groupingExpressions.indexOf(unaliasedAggregateExpr) match {
            case -1 => None
            case ordinal => Some(BoundReference(0, ordinal, Alias(impl, "AGGEXPR")().toAttribute))
          }

          exactGroupingExpr.getOrElse(
            sys.error(s"$agg is not in grouping expressions: $groupingExpressions"))
        } else {
          impl
        }
      }

      // Pull out all the functions so we can feed each row into them.
      val aggFunctions = aggImplementations.flatMap(_ collect { case f: AggregateFunction => f })
      assert(aggFunctions.nonEmpty)

      rows.foreach { row =>
        val input = Vector(row)
        aggFunctions.foreach(_.apply(input))
      }
      buildRow(aggImplementations.map(Evaluate(_, Vector(group))))
    }
  }
}

case class AverageFunction(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  var count: Long = _
  var sum: Long = _

  def result: Any = sum.toDouble / count.toDouble

  def apply(input: Seq[Row]): Unit = {
    count += 1
    // TODO: Support all types here...
    sum += Evaluate(expr, input).asInstanceOf[Int]
  }
}

case class CountFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var count: Int = _

  def apply(input: Seq[Row]): Unit = {
    val evaluatedExpr = expr.map(Evaluate(_, input))
    if (evaluatedExpr.map(_ != null).reduceLeft(_ || _)) {
      count += 1
    }
  }

  def result: Any = count
}

case class SumFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var sum = Evaluate(Cast(Literal(0), expr.dataType), Nil)

  def apply(input: Seq[Row]): Unit =
    sum = Evaluate(Add(Literal(sum), expr), input)

  def result: Any = sum
}

case class CountDistinctFunction(expr: Seq[Expression], base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val seen = new scala.collection.mutable.HashSet[Any]()

  def apply(input: Seq[Row]): Unit = {
    val evaluatedExpr = expr.map(Evaluate(_, input))
    if (evaluatedExpr.map(_ != null).reduceLeft(_ && _))
      seen += evaluatedExpr
  }

  def result: Any = seen.size
}

case class FirstFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  def apply(input: Seq[Row]): Unit = {
    if (result == null)
      result = Evaluate(expr, input)
  }
}

/**
 * Uses spark Accumulators to perform global aggregation.
 *
 * Currently supports only COUNT().
 */
case class SparkAggregate(aggregateExprs: Seq[NamedExpression], child: SharkPlan)
                         (@transient sc: SharkContext) extends UnaryNode {
  def output = aggregateExprs.map(_.toAttribute)
  override def otherCopyArgs = Seq(sc)

  case class AverageFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    val count  = sc.accumulable(0)
    val sum = sc.accumulable(0)
    def result: Any = sum.value.toDouble / count.value.toDouble

    def apply(input: Seq[Row]): Unit = {
      count += 1
      // TODO: Support all types here...
      sum += Evaluate(expr, input).asInstanceOf[Int]
    }
  }

  case class CountFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    val count = sc.accumulable(0)

    def apply(input: Seq[Row]): Unit =
      if (Evaluate(expr, input) != null)
        count += 1

    def result: Any = count.value.toLong
  }

  override def executeCollect() = attachTree(this, "SparkAggregate") {
    // Replace all aggregate expressions with spark functions that will compute the result.
    val aggImplementations = aggregateExprs.map { _ transform {
      case base @ Average(expr) => new AverageFunction(expr, base)
      case base @ Count(expr) => new CountFunction(expr, base)
    }}

    // Pull out all the functions so we can feed each row into them.
    val aggFunctions = aggImplementations.flatMap(_ collect { case f: AggregateFunction => f })
    assert(aggFunctions.nonEmpty)

    logger.debug(s"Running aggregates: $aggFunctions")
    child.execute().foreach { row =>
      val input = Vector(row)
      aggFunctions.foreach(_.apply(input))
    }
    Array(buildRow(aggImplementations.map(Evaluate(_, Nil))))
  }

  def execute() = sc.makeRDD(executeCollect(), 1)
}
