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

import org.apache.spark.Logging

/**
 * A library for easily manipulating trees of operators.  Operators that extend TreeNode are
 * granted the following interface:
 * <ul>
 *   <li>Scala collection like methods (foreach, map, flatMap, collect, etc)</li>
 *   <li>
 *     transform - accepts a partial function that is used to generate a new tree.  When the
 *     partial function can be applied to a given tree segment, that segment is replaced with the
 *     result.  After attempting to apply the partial function to a given node, the transform
 *     function recursively attempts to apply the function to that node's children.
 *   </li>
 *   <li>debugging support - pretty printing, easy splicing of trees, etc.</li>
 * </ul>
 */
package object trees extends Logging {
  // Since we want tree nodes to be lightweight, we create one logger for all treenode instances.
  protected override def logName = "catalyst.trees"

  /**
   * A [[TreeNode]] companion for reference equality for Hash based Collection.
   */
  class TreeNodeRef(val obj: TreeNode[_]) {
    override def equals(o: Any): Boolean = o match {
      case that: TreeNodeRef => that.obj.eq(obj)
      case _ => false
    }

    override def hashCode: Int = if (obj == null) 0 else obj.hashCode
  }
}
