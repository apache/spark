package catalyst
package expressions

import rules._
import errors._

import catalyst.plans.QueryPlan

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(inputTuple: Int, ordinal: Int, baseReference: Attribute)
  extends Attribute with trees.LeafNode[Expression] {

  def nullable = baseReference.nullable
  def dataType = baseReference.dataType
  def exprId = baseReference.exprId
  def qualifiers = baseReference.qualifiers
  def name = baseReference.name

  def newInstance = BoundReference(inputTuple, ordinal, baseReference.newInstance)
  def withQualifiers(newQualifiers: Seq[String]) =
    BoundReference(inputTuple, ordinal, baseReference.withQualifiers(newQualifiers))

  override def toString = s"$baseReference:$inputTuple.$ordinal"
}

class BindReferences[TreeNode <: QueryPlan[TreeNode]] extends Rule[TreeNode] {
  import BindReferences._

  def apply(plan: TreeNode): TreeNode = {
    plan.transform {
      case leafNode if leafNode.children.isEmpty => leafNode
      case nonLeaf => nonLeaf.transformExpressions { case e =>
        bindReference(e, nonLeaf.children.map(_.output))
      }
    }
  }
}

object BindReferences extends Logging {
  def bindReference(expression: Expression, input: Seq[Seq[Attribute]]): Expression = {
    expression.transform { case a: AttributeReference =>
      attachTree(a, "Binding attribute") {
        def inputAsString = input.map(_.mkString("{", ",", "}")).mkString(",")

        for {
          (tuple, inputTuple) <- input.zipWithIndex
          (attr, ordinal) <- tuple.zipWithIndex
          if attr == a
        } {
          logger.debug(s"Binding $attr to $inputTuple.$ordinal given input $inputAsString")
          return BoundReference(inputTuple, ordinal, a)
        }

        logger.debug(s"No binding found for $a given input $inputAsString")
        a
      }
    }
  }
}
