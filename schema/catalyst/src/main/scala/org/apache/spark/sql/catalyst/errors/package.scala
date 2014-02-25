package org.apache.spark.sql
package catalyst

import trees._

/**
 * Functions for attaching and retrieving trees that are associated with errors.
 */
package object errors {

  class TreeNodeException[TreeType <: TreeNode[_]]
    (tree: TreeType, msg: String, cause: Throwable) extends Exception(msg, cause) {

    // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
    // external project dependencies for some reason.
    def this(tree: TreeType, msg: String) = this(tree, msg, null)

    override def getMessage: String = {
      val treeString = tree.toString
      s"${super.getMessage}, tree:${if (treeString contains "\n") "\n" else " "}$tree"
    }
  }

  /**
   *  Wraps any exceptions that are thrown while executing `f` in a
   *  [[catalyst.errors.TreeNodeException TreeNodeException]], attaching the provided `tree`.
   */
  def attachTree[TreeType <: TreeNode[_], A](tree: TreeType, msg: String = "")(f: => A): A = {
    try f catch {
      case e: Exception => throw new TreeNodeException(tree, msg, e)
    }
  }

  /**
   * Executes `f` which is expected to throw a
   * [[catalyst.errors.TreeNodeException TreeNodeException]]. The first tree encountered in
   * the stack of exceptions of type `TreeType` is returned.
   */
  def getTree[TreeType <: TreeNode[_]](f: => Unit): TreeType = ??? // TODO: Implement
}
