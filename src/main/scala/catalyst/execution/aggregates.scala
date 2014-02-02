package catalyst
package execution

import org.apache.hadoop.hive.ql.udf.generic.{GenericUDAFEvaluator, AbstractGenericUDAFResolver}

import catalyst.errors._
import catalyst.expressions._
import catalyst.plans.physical.{ClusteredDistribution, AllTuples}

/* Implicits */
import org.apache.spark.rdd.SharkPairRDDFunctions._

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SharkPlan)(@transient sc: SharkContext)
  extends UnaryNode {

  override def requiredChildDistribution =
    if (groupingExpressions == Nil) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }

  override def otherCopyArgs = sc :: Nil

  case class HiveUdafFunction(
    exprs: Seq[Expression],
    base: AggregateExpression,
    functionName: String)
    extends AggregateFunction
    with HiveInspectors
    with HiveFunctionFactory {

    def this() = this(null, null, null)

    val resolver = createFunction[AbstractGenericUDAFResolver](functionName)

    val function = {
      val evaluator = resolver.getEvaluator(exprs.map(_.dataType.toTypeInfo).toArray)
      evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, toInspectors(exprs).toArray)
      evaluator
    }

    // Cast required to avoid type inference selecting a deprecated Hive API.
    val buffer = function.getNewAggregationBuffer.asInstanceOf[GenericUDAFEvaluator.AbstractAggregationBuffer]

    def result: Any = unwrap(function.evaluate(buffer))

    def apply(input: Seq[Row]): Unit = {
      val inputs = exprs.map(Evaluate(_, input).asInstanceOf[AnyRef]).toArray
      function.iterate(buffer, inputs)
    }
  }

  def output = aggregateExpressions.map(_.toAttribute)

  /* Replace all aggregate expressions with spark functions that will compute the result. */
  def createAggregateImplementations() = aggregateExpressions.map { agg =>
    val impl = agg transform {
      case base @ Average(expr) => new AverageFunction(expr, base)
      case base @ Sum(expr) => new SumFunction(expr, base)
      case base @ Count(expr) => new CountFunction(expr, base)
      // TODO: Create custom query plan node that calculates distinct values efficiently.
      case base @ CountDistinct(expr) => new CountDistinctFunction(expr, base)
      case base @ First(expr) => new FirstFunction(expr, base)
      case base @ HiveGenericUdaf(resolver, expr) => new HiveUdafFunction(expr, base, resolver)
    }

    val remainingAttributes = impl.collect { case a: Attribute => a }
    // If any references exist that are not inside agg functions then the must be grouping exprs
    // in this case we must rebind them to the grouping tuple.
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

  def execute() = attachTree(this, "execute") {
    // TODO: If the child of it is an [[catalyst.execution.Exchange]],
    // do not evaluate the groupingExpressions again since we have evaluated it
    // in the [[catalyst.execution.Exchange]].
    val grouped = child.execute().map { row =>
      (buildRow(groupingExpressions.map(Evaluate(_, Vector(row)))), row)
    }.groupByKeyLocally()

    val result = grouped.map { case (group, rows) =>
      val aggImplementations = createAggregateImplementations()

      // Pull out all the functions so we can feed each row into them.
      val aggFunctions = aggImplementations.flatMap(_ collect { case f: AggregateFunction => f })
      assert(aggFunctions.nonEmpty)

      rows.foreach { row =>
        val input = Vector(row)
        aggFunctions.foreach(_.apply(input))
      }
      buildRow(aggImplementations.map(Evaluate(_, Vector(group))))
    }

    // TODO: THIS DOES NOT PRESERVE LINEAGE AND BREAKS PIPELINING.
    if(groupingExpressions.isEmpty && result.count == 0) {
      // When there there is no output to the Aggregate operator, we still output an empty row.
      val aggImplementations = createAggregateImplementations()
      sc.makeRDD(buildRow(aggImplementations.map(Evaluate(_, Nil))) :: Nil)
    } else {
      result
    }
  }
}

// TODO: Move these default functions back to expressions. Build framework for instantiating them.
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
    if (evaluatedExpr.map(_ != null).reduceLeft(_ && _)) {
      seen += evaluatedExpr
    }
  }

  def result: Any = seen.size
}

case class FirstFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  def apply(input: Seq[Row]): Unit = {
    if (result == null) {
      result = Evaluate(expr, input)
    }
  }
}
