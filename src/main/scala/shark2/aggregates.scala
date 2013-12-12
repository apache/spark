package catalyst
package shark2

import catalyst.errors._
import catalyst.expressions._
import catalyst.types.IntegerType
import shark.SharkContext

/* Implicits */
import org.apache.spark.SparkContext._

case class Aggregate(groupingExpressions: Seq[Expression],
                     aggregateExpressions: Seq[NamedExpression],
                     child: SharkPlan) extends UnaryNode {

  case class AverageFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    var count: Long = _
    var sum: Long = _

    def result: Any = sum.toDouble / count.toDouble

    def apply(input: Seq[Seq[Any]]): Unit = {
      count += 1
      // TODO: Support all types here...
      sum += Evaluate(expr, input).asInstanceOf[Int]
    }
  }

  case class CountFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    var count: Long = _

    def apply(input: Seq[Seq[Any]]): Unit = {
      val evaluatedExpr = expr.map(Evaluate(_, input))
      if(evaluatedExpr.map(_ != null).reduceLeft(_ || _))
          count += 1
    }

    def result: Any = count
  }

  case class SumFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    // TODO: Support other types.
    require(expr.dataType == IntegerType)
    var sum: Int = _

    def apply(input: Seq[Seq[Any]]): Unit =
      sum += Evaluate(expr, input).asInstanceOf[Int]

    def result: Any = sum
  }

  case class CountDistinctFunction(expr: Seq[Expression], base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    val seen = new scala.collection.mutable.HashSet[Any]()

    def apply(input: Seq[Seq[Any]]): Unit = {
      val evaluatedExpr = expr.map(Evaluate(_, input))
      if(evaluatedExpr.map(_ != null).reduceLeft(_ && _))
        seen += evaluatedExpr
    }

    def result: Any = seen.size
  }

  def output = aggregateExpressions.map(_.toAttribute)



  def execute() = attachTree(this, "execute") {
    val grouped = child.execute().map(row => (groupingExpressions.map(Evaluate(_, Vector(row))), row)).groupByKey()
    grouped.map {
      case (group, rows) =>
        // Replace all aggregate expressions with spark functions that will compute the result.
        val aggImplementations = aggregateExpressions.map { agg =>
          val impl = agg transform {
            case base @ Average(expr) => new AverageFunction(expr, base)
            case base @ Sum(expr) => new SumFunction(expr, base)
            case base @ Count(expr) => new CountFunction(expr, base)
            case base @ CountDistinct(expr) => new CountDistinctFunction(expr, base)
          }

          val remainingAttributes = impl.collect { case a: Attribute => a }
          // If any references exist that are not inside agg functions then the must be grouping exprs in this case
          // we must rebind them to the grouping tuple.
          if(remainingAttributes.nonEmpty) {
            val ordinal = groupingExpressions.indexOf(agg)
            if(ordinal == -1) sys.error(s"$agg is not in grouping expressions: $groupingExpressions")
            BoundReference(0, ordinal, Alias(impl, "AGGEXPR")().toAttribute)
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
        // IS THIS RIGHT?
        aggImplementations.map(Evaluate(_, Vector(group))).toIndexedSeq
    }
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

    def apply(input: Seq[Seq[Any]]): Unit = {
      count += 1
      // TODO: Support all types here...
      sum += Evaluate(expr, input).asInstanceOf[Int]
    }
  }

  case class CountFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
    def this() = this(null, null) // Required for serialization.

    val count = sc.accumulable(0)

    def apply(input: Seq[Seq[Any]]): Unit =
      if(Evaluate(expr, input) != null)
        count += 1

    def result: Any = count.value.toLong
  }

  def execute() = attachTree(this, "SparkAggregate") {
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
    sc.makeRDD(Seq(aggImplementations.map(Evaluate(_, Nil)).toIndexedSeq), 1)
  }
}