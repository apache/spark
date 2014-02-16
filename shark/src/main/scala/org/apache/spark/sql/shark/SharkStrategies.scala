package org.apache.spark.sql
package shark

import catalyst.expressions._
import catalyst.planning._
import catalyst.plans._
import catalyst.plans.logical.LogicalPlan

import execution.SparkPlan

trait SharkStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkSqlContext#SparkPlanner =>

  val sharkContext: SharkContext

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child) =>
        ScriptTransformation(input, script, output, planLater(child))(sharkContext) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(table: MetastoreRelation, partition, child) =>
        InsertIntoHiveTable(table, partition, planLater(child))(sharkContext) :: Nil
      case _ => Nil
    }
  }

  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Push attributes into table scan when possible.
      case p @ logical.Project(projectList, m: MetastoreRelation) if isSimpleProject(projectList) =>
        HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m, None)(sharkContext) :: Nil
      case m: MetastoreRelation =>
        HiveTableScan(m.output, m, None)(sharkContext) :: Nil
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
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case p @ FilteredOperation(predicates, relation: MetastoreRelation)
        if relation.hiveQlTable.isPartitioned =>

        val partitionKeyIds = relation.partitionKeys.map(_.id).toSet

        // Filter out all predicates that only deal with partition keys
        val (pruningPredicates, otherPredicates) = predicates.partition {
          _.references.map(_.id).subsetOf(partitionKeyIds)
        }

        val scan = HiveTableScan(
          relation.output, relation, pruningPredicates.reduceLeftOption(And))(sharkContext)

        otherPredicates
          .reduceLeftOption(And)
          .map(execution.Filter(_, scan))
          .getOrElse(scan) :: Nil

      case _ =>
        Nil
    }
  }

  /**
   * A strategy that detects projects and filters over some relation and applies column pruning if
   * possible.  Partition pruning is applied first if the relation is partitioned.
   */
  object ColumnPrunings extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        val predicateOpt = predicates.reduceOption(And)
        val predicateRefs = predicateOpt.map(_.references).getOrElse(Set.empty)
        val projectRefs = projectList.flatMap(_.references)

        // To figure out what columns to preserve after column pruning, we need to consider:
        //
        // 1. Columns referenced by the project list (order preserved)
        // 2. Columns referenced by filtering predicates but not by project list
        // 3. Relation output
        //
        // Then the final result is ((1 union 2) intersect 3)
        val prunedCols = (projectRefs ++ (predicateRefs -- projectRefs)).intersect(relation.output)

        val filteredScans =
          if (relation.hiveQlTable.isPartitioned) {
            // Applies partition pruning first for partitioned table
            val filteredRelation = predicateOpt.map(logical.Filter(_, relation)).getOrElse(relation)
            PartitionPrunings(filteredRelation).view.map(_.transform {
              case scan: HiveTableScan =>
                scan.copy(attributes = prunedCols)(sharkContext)
            })
          } else {
            val scan = HiveTableScan(prunedCols, relation, None)(sharkContext)
            predicateOpt.map(execution.Filter(_, scan)).getOrElse(scan) :: Nil
          }

        if (isSimpleProject(projectList) && prunedCols == projectRefs) {
          filteredScans
        } else {
          filteredScans.view.map(execution.Project(projectList, _))
        }

      case _ =>
        Nil
    }
  }

  /**
   * Returns true if `projectList` only performs column pruning and does not evaluate other
   * complex expressions.
   */
  def isSimpleProject(projectList: Seq[NamedExpression]) = {
    projectList.forall(_.isInstanceOf[Attribute])
  }
}
