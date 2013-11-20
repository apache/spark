package catalyst
package analysis

import plans.logical._
import rules._

/**
 * A trivial analyze with an [[EmptyCatalog]]. Used for testing when all relations are
 * already filled in and the analyser needs only to resolve attribute references.
 */
object SimpleAnalyzer extends Analyzer(EmptyCatalog)

class Analyzer(catalog: Catalog) extends RuleExecutor[LogicalPlan] {
  val fixedPoint = FixedPoint(100)

  val batches = Seq(
    Batch("Resolution", fixedPoint,
      ResolveReferences,
      ResolveRelations)
  )

  object ResolveRelations extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case UnresolvedRelation(name, alias) => catalog.lookupRelation(name, alias)
    }
  }

  object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case q: LogicalPlan =>
        // logger.fine(s"resolving ${plan.simpleString}")
        q transformExpressions {
        case u @ UnresolvedAttribute(name) =>
          // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
          q.resolve(name).getOrElse(u)
      }
    }
  }
}