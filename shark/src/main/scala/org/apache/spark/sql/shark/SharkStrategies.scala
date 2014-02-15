package org.apache.spark.sql
package shark

import catalyst.expressions._
import catalyst.planning._
import catalyst.plans._
import catalyst.plans.logical.LogicalPlan

import org.apache.spark.sql.execution._

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
        HiveTableScan(
          projectList.asInstanceOf[Seq[Attribute]], m, None)(sharkContext) :: Nil
      case m: MetastoreRelation =>
        HiveTableScan(m.output, m, None)(sharkContext) :: Nil
      case _ => Nil
    }

    /**
     * Returns true if `projectList` only performs column pruning and does not evaluate other
     * complex expressions.
     */
    def isSimpleProject(projectList: Seq[NamedExpression]) = {
      projectList.forall(_.isInstanceOf[Attribute])
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
          .map(Filter(_, scan))
          .getOrElse(scan) :: Nil

      case _ =>
        Nil
    }
  }
}
