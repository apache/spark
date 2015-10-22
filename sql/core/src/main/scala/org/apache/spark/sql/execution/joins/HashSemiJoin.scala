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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.LongSQLMetric


trait HashSemiJoin {
  self: SparkPlan =>
  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val left: SparkPlan
  val right: SparkPlan
  val condition: Option[Expression]

  override def output: Seq[Attribute] = left.output

  protected[this] def supportUnsafe: Boolean = {
    (self.codegenEnabled && self.unsafeEnabled
      && UnsafeProjection.canSupport(leftKeys)
      && UnsafeProjection.canSupport(rightKeys)
      && UnsafeProjection.canSupport(left.schema)
      && UnsafeProjection.canSupport(right.schema))
  }

  override def outputsUnsafeRows: Boolean = supportUnsafe
  override def canProcessUnsafeRows: Boolean = supportUnsafe
  override def canProcessSafeRows: Boolean = !supportUnsafe

  protected def leftKeyGenerator: Projection =
    if (supportUnsafe) {
      UnsafeProjection.create(leftKeys, left.output)
    } else {
      newMutableProjection(leftKeys, left.output)()
    }

  protected def rightKeyGenerator: Projection =
    if (supportUnsafe) {
      UnsafeProjection.create(rightKeys, right.output)
    } else {
      newMutableProjection(rightKeys, right.output)()
    }

  @transient private lazy val boundCondition =
    newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)

  protected def buildKeyHashSet(
      buildIter: Iterator[InternalRow], numBuildRows: LongSQLMetric): java.util.Set[InternalRow] = {
    val hashSet = new java.util.HashSet[InternalRow]()

    // Create a Hash set of buildKeys
    val rightKey = rightKeyGenerator
    while (buildIter.hasNext) {
      val currentRow = buildIter.next()
      numBuildRows += 1
      val rowKey = rightKey(currentRow)
      if (!rowKey.anyNull) {
        val keyExists = hashSet.contains(rowKey)
        if (!keyExists) {
          hashSet.add(rowKey.copy())
        }
      }
    }

    hashSet
  }

  protected def hashSemiJoin(
    streamIter: Iterator[InternalRow],
    numStreamRows: LongSQLMetric,
    hashSet: java.util.Set[InternalRow],
    numOutputRows: LongSQLMetric): Iterator[InternalRow] = {
    val joinKeys = leftKeyGenerator
    streamIter.filter(current => {
      numStreamRows += 1
      val key = joinKeys(current)
      val r = !key.anyNull && hashSet.contains(key)
      if (r) numOutputRows += 1
      r
    })
  }

  protected def hashSemiJoin(
      streamIter: Iterator[InternalRow],
      numStreamRows: LongSQLMetric,
      hashedRelation: HashedRelation,
      numOutputRows: LongSQLMetric): Iterator[InternalRow] = {
    val joinKeys = leftKeyGenerator
    val joinedRow = new JoinedRow
    streamIter.filter { current =>
      numStreamRows += 1
      val key = joinKeys(current)
      lazy val rowBuffer = hashedRelation.get(key)
      val r = !key.anyNull && rowBuffer != null && rowBuffer.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      }
      if (r) numOutputRows += 1
      r
    }
  }
}
