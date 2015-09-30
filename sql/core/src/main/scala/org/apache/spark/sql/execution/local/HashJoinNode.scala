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
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Much of this code is similar to [[org.apache.spark.sql.execution.joins.HashJoin]].
 */
case class HashJoinNode(
    conf: SQLConf,
    streamedKeys: Seq[Expression],
    streamedNode: LocalNode,
    buildSide: BuildSide,
    buildOutput: Seq[Attribute],
    hashedRelation: HashedRelation) extends UnaryLocalNode(conf) {

  override val child = streamedNode

  // Because we do not pass in the buildNode, we take the output of buildNode to
  // create the inputSet properly.
  override def inputSet: AttributeSet = AttributeSet(child.output ++ buildOutput)

  override def output: Seq[Attribute] = buildSide match {
    case BuildRight => streamedNode.output ++ buildOutput
    case BuildLeft => buildOutput ++ streamedNode.output
  }

  private[this] var currentStreamedRow: InternalRow = _
  private[this] var currentHashMatches: Seq[InternalRow] = _
  private[this] var currentMatchPosition: Int = -1

  private[this] var joinRow: JoinedRow = _
  private[this] var resultProjection: (InternalRow) => InternalRow = _

  private[this] val hashed: HashedRelation = hashedRelation
  private[this] var joinKeys: Projection = _


  private[this] def isUnsafeMode: Boolean = {
    (codegenEnabled && unsafeEnabled && UnsafeProjection.canSupport(schema))
  }

  private[this] def streamSideKeyGenerator: Projection = {
    if (isUnsafeMode) {
      UnsafeProjection.create(streamedKeys, streamedNode.output)
    } else {
      newMutableProjection(streamedKeys, streamedNode.output)()
    }
  }

  override def open(): Unit = {
    streamedNode.open()
    joinRow = new JoinedRow
    resultProjection = {
      if (isUnsafeMode) {
        UnsafeProjection.create(schema)
      } else {
        identity[InternalRow]
      }
    }
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

  override def close(): Unit = {
    streamedNode.close()
  }
}
