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
 * A [[HashJoinNode]] for broadcast join. It takes a streamedNode and a broadcast
 * [[HashedRelation]]. The actual work of this node is defined in [[HashJoinNode]].
 */
case class BroadcastHashJoinNode(
    conf: SQLConf,
    streamedKeys: Seq[Expression],
    streamedNode: LocalNode,
    buildSide: BuildSide,
    buildOutput: Seq[Attribute],
    hashedRelation: Broadcast[HashedRelation])
  extends UnaryLocalNode(conf) with HashJoinNode {

  override val child = streamedNode

  // Because we do not pass in the buildNode, we take the output of buildNode to
  // create the inputSet properly.
  override def inputSet: AttributeSet = AttributeSet(child.output ++ buildOutput)

  override def output: Seq[Attribute] = buildSide match {
    case BuildRight => streamedNode.output ++ buildOutput
    case BuildLeft => buildOutput ++ streamedNode.output
  }

  protected override def doOpen(): Unit = {
    streamedNode.open()
    // Set the HashedRelation used by the HashJoinNode.
    withHashedRelation(hashedRelation.value)
  }

  override def close(): Unit = {
    streamedNode.close()
  }
}
