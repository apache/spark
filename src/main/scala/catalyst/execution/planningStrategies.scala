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
        execution.HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m, None) :: Nil
      case m: MetastoreRelation =>
        execution.HiveTableScan(m.output, m, None) :: Nil
      case _ => Nil
    }
  }

  /**
   * A strategy used to detect filtering predicates on top of a partitioned relation to help
   * partition pruning.
   *
   * This strategy itself doesn't perform partition pruning, it just collects and combines all the
   * partition pruning predicates and pass them down to the underlying [[HiveTableScan]] operator,
   * which does the actual pruning work.
   */
  object PartitionPrunings extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case p @ FilteredOperation(predicates, relation: MetastoreRelation)
          if relation.hiveQlTable.isPartitioned =>

        val partitionKeyIds = relation.partitionKeys.map(_.id).toSet

        // Filter out all predicates that only deal with partition keys
        val (pruningPredicates, otherPredicates) = predicates.partition {
          _.references.map(_.id).subsetOf(partitionKeyIds)
        }

        val scan = execution.HiveTableScan(
          relation.output, relation, pruningPredicates.reduceOption(And))

        otherPredicates
          .reduceLeftOption(And)
          .map(execution.Filter(_, scan))
          .getOrElse(scan) :: Nil

      case _ =>
        Nil
    }
  }

  /**
   * A strategy that detects projection over filtered operation and applies column pruning if
   * possible.
   */
  object ColumnPrunings extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.Project(projectList, child @ FilteredOperation(predicates, m: MetastoreRelation))
          if isSimpleProject(projectList) =>

        val projectAttributes = projectList.asInstanceOf[Seq[Attribute]]
        val predicatesReferences = predicates.flatMap(_.references).toSet
        val prunedAttributes = projectAttributes ++ (predicatesReferences -- projectAttributes)

        if (m.hiveQlTable.isPartitioned) {
          // Applies partition pruning first for partitioned table
          PartitionPrunings(child).view.map { sharkPlan =>
            execution.Project(
              projectList,
              sharkPlan.transform {
                case scan@execution.HiveTableScan(attributes, _, _) =>
                  scan.copy(attributes = prunedAttributes)
              })
          }
        } else {
          val scan = execution.HiveTableScan(prunedAttributes, m, None)
          val conjunctionOpt = predicates.reduceOption(And)

          execution.Project(
            projectList,
            conjunctionOpt
              .map(execution.Filter(_, scan))
              .getOrElse(scan)) :: Nil
        }

      case _ =>
        Nil
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
          case Equals(l, r) => (canEvaluate(l, left) && canEvaluate(r, right)) ||
                               (canEvaluate(l, right) && canEvaluate(r, left))
          case _ => false
        }

        val joinKeys = joinPredicates.map {
          case Equals(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => (l, r)
          case Equals(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => (r, l)
        }

        // Do not consider this strategy if there are no join keys.
        if (joinKeys.nonEmpty) {
          val (leftKeys, rightKeys) = joinKeys.unzip
          val joinOp = execution.SparkEquiInnerJoin(
            leftKeys, rightKeys, planLater(left), planLater(right))

          otherPredicates
            .reduceOption(And)
            .map(execution.Filter(_, joinOp))
            .getOrElse(joinOp) :: Nil
        } else {
          logger.debug(s"Avoiding spark join with no join keys.")
          Nil
        }
      case _ => Nil
    }

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
        execution.Aggregate(group, agg, planLater(child))(sc) :: Nil
      case logical.Sample(fraction, withReplacement, seed, child) =>
        execution.Sample(fraction, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data) =>
        execution.LocalRelation(output, data.map(_.productIterator.toVector))(sc) :: Nil
      case logical.StopAfter(limit, child) =>
        execution.StopAfter(Evaluate(limit, Nil).asInstanceOf[Int], planLater(child))(sc) :: Nil
      case Unions(unionChildren) =>
        execution.Union(unionChildren.map(planLater))(sc) :: Nil
      case logical.Transform(input, script, output, child) =>
        execution.Transform(input, script, output, planLater(child))(sc) :: Nil
      case _ => Nil
    }
  }

  /**
   * Returns true if `projectList` only performs column pruning and does not evaluate other
   * complex expressions.
   */
  private def isSimpleProject(projectList: Seq[NamedExpression]) = {
    projectList.forall(_.isInstanceOf[Attribute])
  }
}