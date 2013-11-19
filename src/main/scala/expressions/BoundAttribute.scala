package catalyst
package expressions

import plans.physical.PhysicalPlan
import rules._
import errors._

case class BoundReference(inputTuple: Int, ordinal: Int, baseReference: AttributeReference) extends LeafExpression {
  def references = baseReference.references
  def nullable = baseReference.nullable
  def dataType = baseReference.dataType
}

// TODO: Should be against any query plan...
object BindReferences extends Rule[PhysicalPlan] {
  def apply(plan: PhysicalPlan): PhysicalPlan = {
    plan.transform {
      case leafNode: PhysicalPlan if leafNode.children.isEmpty => leafNode
      case nonLeaf: PhysicalPlan => attachTree(nonLeaf, "Binding references in operator") {
        nonLeaf.transformExpressions {
          case a: AttributeReference => attachTree(a, "Binding attribute") {
            val inputTuple = nonLeaf.children.indexWhere(_.output contains a)
            val ordinal = nonLeaf.children(inputTuple).output.indexWhere(_ == a)
            assert(ordinal != -1, "Reference not found in child plan")
            BoundReference(inputTuple, ordinal, a)
          }
        }
      }
    }
  }
}