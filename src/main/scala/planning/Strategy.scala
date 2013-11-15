package catalyst
package planning

import catalyst.analysis.MetastoreRelation
import expressions._
import plans.logical._
import plans.physical
import plans.physical.PhysicalPlan

abstract class Strategy {
  def apply(plan: LogicalPlan): Seq[PhysicalPlan]

  // TODO: Actually plan later.
  def planLater(plan: LogicalPlan): PhysicalPlan = TrivalPlanner(plan).next
}

object DataSinks extends Strategy {
  def apply(plan: LogicalPlan): Seq[PhysicalPlan] = plan match {
    case InsertIntoHiveTable(tableName, child) =>
      physical.InsertIntoHiveTable(tableName, planLater(child)) :: Nil
    case _ => Nil
  }
}

object HiveTableScans extends Strategy {
  def apply(plan: LogicalPlan): Seq[PhysicalPlan] = plan match {
    case p @ Project(projectList, m: MetastoreRelation) if isSimpleProject(projectList) =>
      physical.HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m) :: Nil
    case m: MetastoreRelation =>
      physical.HiveTableScan(m.output, m) :: Nil
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