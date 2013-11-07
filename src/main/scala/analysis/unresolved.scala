package catalyst
package analysis

import expressions._
import plans.logical._

case class UnresolvedRelation(name: String, alias: Option[String]) extends LeafNode

case class UnresolvedAttribute(name: String) extends Attribute with trees.LeafNode[Expression]