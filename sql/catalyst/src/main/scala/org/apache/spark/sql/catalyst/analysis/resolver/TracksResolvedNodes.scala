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

import java.util.IdentityHashMap

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.internal.SQLConf

/**
 * Trait for top-level resolvers that is used to keep track of resolved nodes and throw an error if
 * a node is resolved more than once. This is only used in tests because of the memory overhead of
 * using a set to track resolved nodes.
 */
trait TracksResolvedNodes[TreeNodeType <: TreeNode[TreeNodeType]] extends SQLConfHelper {
  // Using Map because IdentityHashSet is not available in Scala
  private val seenResolvedNodes = new IdentityHashMap[TreeNodeType, Unit]

  private val shouldTrackResolvedNodes =
    conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_TRACK_RESOLVED_NODES_ENABLED)

  protected def throwIfNodeWasResolvedEarlier(node: TreeNodeType): Unit =
    if (shouldTrackResolvedNodes && seenResolvedNodes.containsKey(node)) {
      throw SparkException.internalError(
        s"Single-pass resolver attempted to resolve the same node more than once: $node"
      )
    }

  protected def markNodeAsResolved(node: TreeNodeType): Unit = {
    if (shouldTrackResolvedNodes) {
      seenResolvedNodes.put(node, ())
    }
  }
}
