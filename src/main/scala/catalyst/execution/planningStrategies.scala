package catalyst
package execution

import expressions._
import planning._
import plans._
import plans.logical.LogicalPlan

trait PlanningStrategies {
  self: QueryPlanner[SharkPlan] =>

  val sc: SharkContext

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.InsertIntoTable(table: MetastoreRelation, partition, child) =>
        InsertIntoHiveTable(table, partition, planLater(child))(sc) :: Nil
      case _ => Nil
    }
  }

  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      // Push attributes into table scan when possible.
      case p @ logical.Project(projectList, m: MetastoreRelation) if isSimpleProject(projectList) =>
        execution.HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m) :: Nil
      case m: MetastoreRelation =>
        execution.HiveTableScan(m.output, m) :: Nil
      case _ => Nil
    }

    /**
     * Returns true if `projectList` only performs column pruning and does not evaluate other
     * complex expressions.
     */
    def isSimpleProject(projectList: Seq[NamedExpression]) = {
      projectList.map {
        case a: Attribute => true
        case _ => false
      }.reduceLeft(_ && _)
    }
  }

  /**
   * Aggregate functions that use sparks accumulator functionality.
   */
  object SparkAggregates extends Strategy {
    val allowedAggregates = Set[Class[_]](
      classOf[Count],
      classOf[Average])

    /**
     * Returns true if `exprs` only contains aggregates that can be computed using Accumulators.
     */
    def onlyAllowedAggregates(exprs: Seq[Expression]): Boolean = {
      val aggs = exprs.flatMap(_.collect { case a: AggregateExpression => a}).map(_.getClass)
      aggs.map(allowedAggregates contains _).reduceLeft(_ && _)
    }

    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.Aggregate(Nil, agg, child) if onlyAllowedAggregates(agg) =>
        execution.SparkAggregate(agg, planLater(child))(sc) :: Nil
      case _ => Nil
    }
  }

  object SparkEquiInnerJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case FilteredOperation(predicates, logical.Join(left, right, Inner, condition)) =>
        logger.debug(s"Considering join: ${predicates ++ condition}")
        // Find equi-join predicates that can be evaluated before the join, and thus can be used
        // as join keys. Note we can only mix in the conditions with other predicates because the
        // match above ensures that this is and Inner join.
        val (joinPredicates, otherPredicates) = (predicates ++ condition).partition {
          case Equals(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
                               (canEvaluate(l, right) && canEvaluate(r, left)) => true
          case _ => false
        }

        val joinKeys = joinPredicates.map {
          case Equals(l,r) if (canEvaluate(l, left) && canEvaluate(r, right)) => (l, r)
          case Equals(l,r) if (canEvaluate(l, right) && canEvaluate(r, left)) => (r, l)
        }

        // Do not consider this strategy if there are no join keys.
        if (joinKeys.nonEmpty) {
          val leftKeys = joinKeys.map(_._1)
          val rightKeys = joinKeys.map(_._2)

          val joinOp = execution.SparkEquiInnerJoin(
            leftKeys, rightKeys, planLater(left), planLater(right))

          // Make sure other conditions are met if present.
          if (otherPredicates.nonEmpty) {
            execution.Filter(combineConjunctivePredicates(otherPredicates), joinOp) :: Nil
          } else {
            joinOp :: Nil
          }
        } else {
          logger.debug(s"Avoiding spark join with no join keys.")
          Nil
        }
      case _ => Nil
    }

    private def combineConjunctivePredicates(predicates: Seq[Expression]) =
      predicates.reduceLeft(And(_, _))

    /** Returns true if `expr` can be evaluated using only the output of `plan`. */
    protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
      expr.references subsetOf plan.outputSet
  }

  object BroadcastNestedLoopJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.Join(left, right, joinType, condition) =>
        execution.BroadcastNestedLoopJoin(
          planLater(left), planLater(right), joinType, condition)(sc) :: Nil
      case _ => Nil
    }
  }

  object CartesianProduct extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.Join(left, right, _, None) =>
        execution.CartesianProduct(planLater(left), planLater(right)) :: Nil
      case logical.Join(left, right, Inner, Some(condition)) =>
        execution.Filter(condition,
          execution.CartesianProduct(planLater(left), planLater(right))) :: Nil
      case _ => Nil
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.Sort(sortExprs, child) =>
        execution.Sort(sortExprs, planLater(child)) :: Nil
      // TODO: It is correct, but overkill to do a global sorting here.
      case logical.SortPartitions(sortExprs, child) =>
        execution.Sort(sortExprs, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.Filter(condition, planLater(child)) :: Nil
      case logical.Aggregate(group, agg, child) =>
        execution.Aggregate(group, agg, planLater(child)) :: Nil
      case logical.Sample(fraction, withReplacement, child) =>
        execution.Sample(fraction, withReplacement, planLater(child)) :: Nil
      case logical.LocalRelation(output, data) =>
        execution.LocalRelation(output, data.map(_.productIterator.toVector))(sc) :: Nil
      case logical.StopAfter(limit, child) =>
        execution.StopAfter(Evaluate(limit, Nil).asInstanceOf[Int], planLater(child))(sc) :: Nil
      case logical.Union(left, right) =>
        execution.Union(planLater(left), planLater(right))(sc) :: Nil
      case logical.Transform(input, script, output, child) =>
        execution.Transform(input, script, output, planLater(child))(sc) :: Nil
      case _ => Nil
    }
  }

}