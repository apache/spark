package catalyst

import trees._

package object errors {

  class OptimizationException[TreeType <: TreeNode[_]]
    (tree: TreeType, msg: String, cause: Throwable = null) extends Exception(msg, cause) {

    override def getMessage: String = {
      s"${super.getMessage}, tree:\n$tree"
    }
  }

  def attachTree[TreeType <: TreeNode[_], A](tree: TreeType, msg: String = "")(f: => A): A = {
    try f catch {
      case e: Exception => throw new OptimizationException(tree, msg, e)
    }
  }

}