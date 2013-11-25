package catalyst
package shark2

import shark.SharkContext

import expressions._
import planning._
import plans.logical
import plans.logical.LogicalPlan

abstract trait PlanningStrategies {
  self: QueryPlanner[SharkPlan] =>

  val sc: SharkContext

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.InsertIntoHiveTable(tableName, child) =>
        InsertIntoHiveTable(tableName, planLater(child))(sc) :: Nil
      case _ => Nil
    }
  }

  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      // Push attributes into table scan when possible.
      case p @ logical.Project(projectList, m: MetastoreRelation) if isSimpleProject(projectList) =>
        shark2.HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m) :: Nil
      case m: MetastoreRelation =>
        shark2.HiveTableScan(m.output, m) :: Nil
      case _ => Nil
    }

    /**
     * Returns true if [[projectList]] only performs column pruning and
     * does not evaluate other complex expressions.
     */
    def isSimpleProject(projectList: Seq[NamedExpression]) = {
      projectList.map {
        case a: Attribute => true
        case _ => false
      }.reduceLeft(_ && _)
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SharkPlan] = plan match {
      case logical.Sort(sortExprs, child) =>
        shark2.Sort(sortExprs, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        shark2.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        shark2.Filter(condition, planLater(child)) :: Nil
      case logical.Aggregate(Nil, agg, child) =>
        shark2.SparkAggregate(agg, planLater(child))(sc) :: Nil
      case logical.LocalRelation(output, data) =>
        shark2.LocalRelation(output, data.map(_.productIterator.toVector))(sc) :: Nil
      case logical.StopAfter(limit, child) =>
        shark2.StopAfter(Evaluate(limit, Nil).asInstanceOf[Int], planLater(child))(sc) :: Nil
      case logical.Union(left, right) =>
        shark2.Union(planLater(left), planLater(right))(sc) :: Nil
      case _ => Nil
    }
  }

}