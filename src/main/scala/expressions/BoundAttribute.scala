package catalyst
package expressions

import rules._
import errors._

import shark2.SharkPlan

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value to be retrieved more
 * efficiently.  However, since operations like column pruning can change the layout of intermediate tuples,
 * BindReferences should be run after all such transformations.
 */
case class BoundReference(inputTuple: Int, ordinal: Int, baseReference: Attribute)
  extends Attribute with trees.LeafNode[Expression] {

  def nullable = baseReference.nullable
  def dataType = baseReference.dataType
  def exprId = baseReference.exprId
  def qualifiers = baseReference.qualifiers
  def name = baseReference.name
  def resolved = true

  def withQualifiers(newQualifiers: Seq[String]) =
    BoundReference(inputTuple, ordinal, baseReference.withQualifiers(newQualifiers))

  override def toString = s"$baseReference:$inputTuple.$ordinal"
}

// TODO: Should run against any query plan, not just SharkPlans
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