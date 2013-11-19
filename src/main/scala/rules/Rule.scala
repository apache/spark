package catalyst
package rules

import trees._

abstract class Rule[TreeType <: TreeNode[_]] {
  val name = {
    val className = getClass.getName
    if(className endsWith "$")
      className.dropRight(1)
    else
      className
  }

  def apply(plan: TreeType): TreeType
}