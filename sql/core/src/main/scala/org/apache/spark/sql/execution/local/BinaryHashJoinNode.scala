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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.joins.{HashedRelation, BuildLeft, BuildRight, BuildSide}

/**
 * A [[HashJoinNode]] that builds the [[HashedRelation]] according to the value of
 * `buildSide`. The actual work of this node is defined in [[HashJoinNode]].
 */
case class BinaryHashJoinNode(
    conf: SQLConf,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: LocalNode,
    right: LocalNode)
  extends BinaryLocalNode(conf) with HashJoinNode {

  protected override val (streamedNode, streamedKeys) = buildSide match {
    case BuildLeft => (right, rightKeys)
    case BuildRight => (left, leftKeys)
  }

  private val (buildNode, buildKeys) = buildSide match {
    case BuildLeft => (left, leftKeys)
    case BuildRight => (right, rightKeys)
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  private def buildSideKeyGenerator: Projection = {
    // We are expecting the data types of buildKeys and streamedKeys are the same.
    assert(buildKeys.map(_.dataType) == streamedKeys.map(_.dataType))
    UnsafeProjection.create(buildKeys, buildNode.output)
  }

  protected override def doOpen(): Unit = {
    buildNode.open()
    val hashedRelation = HashedRelation(buildNode, buildSideKeyGenerator)
    // We have built the HashedRelation. So, close buildNode.
    buildNode.close()

    streamedNode.open()
    // Set the HashedRelation used by the HashJoinNode.
    withHashedRelation(hashedRelation)
  }

  override def close(): Unit = {
    // Please note that we do not need to call the close method of our buildNode because
    // it has been called in this.open.
    streamedNode.close()
  }
}
