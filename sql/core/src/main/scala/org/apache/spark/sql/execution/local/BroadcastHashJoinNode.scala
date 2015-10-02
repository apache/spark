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

package org.apache.spark.sql.execution.local

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashedRelation}

/**
 * A wrapper of [[HashJoinNode]] for broadcast join. The actual work of this node will be
 * delegated to the [[HashJoinNode]] that is created in `open`.
 */
case class BroadcastHashJoinNode(
    conf: SQLConf,
    streamedKeys: Seq[Expression],
    streamedNode: LocalNode,
    buildSide: BuildSide,
    buildOutput: Seq[Attribute],
    hashedRelation: Broadcast[HashedRelation]) extends UnaryLocalNode(conf) {

  override val child = streamedNode

  private[this] var hashJoinNode: HashJoinNode = _

  // Because we do not pass in the buildNode, we take the output of buildNode to
  // create the inputSet properly.
  override def inputSet: AttributeSet = AttributeSet(child.output ++ buildOutput)

  override def output: Seq[Attribute] = buildSide match {
    case BuildRight => streamedNode.output ++ buildOutput
    case BuildLeft => buildOutput ++ streamedNode.output
  }

  override def open(): Unit = {
    // Call the open of streamedNode.
    streamedNode.open()
    // Create the HashJoinNode based on the streamedNode and HashedRelation.
    hashJoinNode =
      HashJoinNode(
        conf = conf,
        streamedKeys = streamedKeys,
        streamedNode = streamedNode,
        buildSide = buildSide,
        buildOutput = buildOutput,
        hashedRelation = hashedRelation.value,
        isWrapped = true)
    // Setup this HashJoinNode. We still call these in case there is any setup work
    // that needs to be done in this HashJoinNode. Because isWrapped is true,
    // prepare and open will not propagate to the child of streamedNode.
    hashJoinNode.prepare()
    hashJoinNode.open()
  }

  override def next(): Boolean = {
    hashJoinNode.next()
  }

  override def fetch(): InternalRow = {
    hashJoinNode.fetch()
  }

  override def close(): Unit = {
    // Close the internal HashJoinNode.  We still call this in case there is any teardown work
    // that needs to be done in this HashJoinNode. Because isWrapped is true,
    // prepare and open will not propagate to the child of streamedNode.
    hashJoinNode.close()
    // Now, close the streamedNode.
    streamedNode.close()
  }
}
