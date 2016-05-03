/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Functions for attaching and retrieving trees that are associated with errors.
 */
package object errors {

  class TreeNodeException[TreeType <: TreeNode[_]](
      @transient val tree: TreeType,
      msg: String,
      cause: Throwable)
    extends Exception(msg, cause) {

    val treeString = tree.toString

    // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
    // external project dependencies for some reason.
    def this(tree: TreeType, msg: String) = this(tree, msg, null)

    override def getMessage: String = {
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
}
