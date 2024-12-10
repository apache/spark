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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Wrapper class used to compare [[TreeNode]] instances by means of shallow comparison on
 * references only, rather than deep comparison covering all the descendant nodes.
 */
class TreeNodeComparatorByReference[TreeNodeType <: TreeNode[TreeNodeType]](
    val node: TreeNodeType) {
  override def equals(other: Any): Boolean = other match {
    // compare nodes by reference only in order to avoid looking into the tree
    case other: TreeNodeComparatorByReference[_] => node.eq(other.node)
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(node)
}
