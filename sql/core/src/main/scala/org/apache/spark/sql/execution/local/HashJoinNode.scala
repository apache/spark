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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.joins._

/**
 * An abstract node for sharing common functionality among different implementations of
 * inner hash equi-join, notably [[BinaryHashJoinNode]] and [[BroadcastHashJoinNode]].
 *
 * Much of this code is similar to [[org.apache.spark.sql.execution.joins.HashJoin]].
 */
trait HashJoinNode {

  self: LocalNode =>

  protected def streamedKeys: Seq[Expression]
  protected def streamedNode: LocalNode
  protected def buildSide: BuildSide

  private[this] var currentStreamedRow: InternalRow = _
  private[this] var currentHashMatches: Seq[InternalRow] = _
  private[this] var currentMatchPosition: Int = -1

  private[this] var joinRow: JoinedRow = _
  private[this] var resultProjection: (InternalRow) => InternalRow = _

  private[this] var hashed: HashedRelation = _
  private[this] var joinKeys: Projection = _

  private def streamSideKeyGenerator: Projection =
    UnsafeProjection.create(streamedKeys, streamedNode.output)

  /**
   * Sets the HashedRelation used by this node. This method needs to be called after
   * before the first `next` gets called.
   */
  protected def withHashedRelation(hashedRelation: HashedRelation): Unit = {
    hashed = hashedRelation
  }

  /**
   * Custom open implementation to be overridden by subclasses.
   */
  protected def doOpen(): Unit

  override def open(): Unit = {
    doOpen()
    joinRow = new JoinedRow
    resultProjection = UnsafeProjection.create(schema)
    joinKeys = streamSideKeyGenerator
  }

  override def next(): Boolean = {
    currentMatchPosition += 1
    if (currentHashMatches == null || currentMatchPosition >= currentHashMatches.size) {
      fetchNextMatch()
    } else {
      true
    }
  }

  /**
   * Populate `currentHashMatches` with build-side rows matching the next streamed row.
   * @return whether matches are found such that subsequent calls to `fetch` are valid.
   */
  private def fetchNextMatch(): Boolean = {
    currentHashMatches = null
    currentMatchPosition = -1

    while (currentHashMatches == null && streamedNode.next()) {
      currentStreamedRow = streamedNode.fetch()
      val key = joinKeys(currentStreamedRow)
      if (!key.anyNull) {
        currentHashMatches = hashed.get(key)
      }
    }

    if (currentHashMatches == null) {
      false
    } else {
      currentMatchPosition = 0
      true
    }
  }

  override def fetch(): InternalRow = {
    val ret = buildSide match {
      case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
      case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
    }
    resultProjection(ret)
  }
}
