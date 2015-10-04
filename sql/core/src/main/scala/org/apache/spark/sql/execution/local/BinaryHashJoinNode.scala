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
 * A wrapper of [[HashJoinNode]]. It will build the [[HashedRelation]] according to the value of
 * `buildSide`. The actual work of this node will be delegated to the [[HashJoinNode]]
 * that is created in `open`.
 */
case class BinaryHashJoinNode(
    conf: SQLConf,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: LocalNode,
    right: LocalNode) extends BinaryLocalNode(conf) {

  private[this] lazy val (buildNode, buildKeys, streamedNode, streamedKeys) = buildSide match {
    case BuildLeft => (left, leftKeys, right, rightKeys)
    case BuildRight => (right, rightKeys, left, leftKeys)
  }

  private[this] val hashJoinNode: HashJoinNode = {
    HashJoinNode(
      conf = conf,
      streamedKeys = streamedKeys,
      streamedNode = streamedNode,
      buildSide = buildSide,
      buildOutput = buildNode.output,
      isWrapped = true)
  }
  override def output: Seq[Attribute] = left.output ++ right.output

  private[this] def isUnsafeMode: Boolean = {
    (codegenEnabled && unsafeEnabled && UnsafeProjection.canSupport(buildKeys))
  }

  private[this] def buildSideKeyGenerator: Projection = {
    if (isUnsafeMode) {
      UnsafeProjection.create(buildKeys, buildNode.output)
    } else {
      newMutableProjection(buildKeys, buildNode.output)()
    }
  }

  override def open(): Unit = {
    // buildNode's prepare has been called in this.prepare.
    buildNode.open()
    val hashedRelation = HashedRelation(buildNode, buildSideKeyGenerator)
    // We have built the HashedRelation. So, close buildNode.
    buildNode.close()

    // Call the open of streamedNode.
    streamedNode.open()
    // Set the HashedRelation used by the HashJoinNode.
    hashJoinNode.withHashedRelation(hashedRelation)
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
    // Please note that we do not need to call the close method of our buildNode because
    // it has been called in this.open.
  }
}
