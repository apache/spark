package catalyst
package expressions

import rules._
import errors._

import shark2.SharkPlan

case class BoundReference(inputTuple: Int, ordinal: Int, baseReference: AttributeReference) extends LeafExpression {
  def references = baseReference.references
  def nullable = baseReference.nullable
  def dataType = baseReference.dataType
}

// TODO: Should be against any query plan...
object BindReferences extends Rule[SharkPlan] {
  def apply(plan: SharkPlan): SharkPlan = {
    plan.transform {
      case leafNode: SharkPlan if leafNode.children.isEmpty => leafNode
      case nonLeaf: SharkPlan => attachTree(nonLeaf, "Binding references in operator") {
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