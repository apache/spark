package catalyst
package trees

abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  self: BaseType with Product =>

  def children: Seq[BaseType]

  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = rule.applyOrElse(this, identity[BaseType])
    if(this == afterRule)
      transformChildren(rule)
    else
      afterRule.transformChildren(rule)
  }

  def transformChildren(rule: PartialFunction[BaseType, BaseType]): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: AnyRef if(children contains arg) =>
          val newChild = arg.asInstanceOf[BaseType].transform(rule)
          if(newChild != arg) {
            changed = true
            newChild
          } else {
            arg
          }
      case nonChild: AnyRef => nonChild
    }.toArray
    if(changed) makeCopy(newArgs) else this
  }

  def makeCopy(newArgs: Array[AnyRef]): this.type =
    getClass.getConstructors.head.newInstance(newArgs: _*).asInstanceOf[this.type]
}

trait BinaryNode[BaseType <: TreeNode[BaseType]] {
  def left: BaseType
  def right: BaseType

  def children = Seq(left, right)
}

trait LeafNode[BaseType <: TreeNode[BaseType]] {
  def children = Nil
}

trait UnaryNode[BaseType <: TreeNode[BaseType]] {
  def child: BaseType
  def children = child :: Nil
}
