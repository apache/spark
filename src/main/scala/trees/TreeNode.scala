package catalyst
package trees

class TreeNode[BaseType <: TreeNode[BaseType]] {
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = ???
}
