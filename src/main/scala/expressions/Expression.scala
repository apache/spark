package catalyst
package expressions

import trees._

abstract class Expression extends TreeNode[Expression] {
  self: Product =>
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}
