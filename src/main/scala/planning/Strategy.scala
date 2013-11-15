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

  //def planLater(plan: LogicalPlan): PhysicalPlan = TrivalPlanner(plan).next
}