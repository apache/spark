package catalyst
package analysis

import plans.logical._
import rules._

class Analyzer(catalog: Catalog) extends RuleExecutor[LogicalPlan] {
  val batches = Seq(
    Batch("Resolution",
      ResolveRelations)
  )

  object ResolveRelations extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case UnresolvedRelation(name, alias) => catalog.lookupRelation(name, alias)
    }
  }
}