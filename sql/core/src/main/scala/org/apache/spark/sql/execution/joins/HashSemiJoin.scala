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


trait HashSemiJoin {
  self: SparkPlan =>
  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val left: SparkPlan
  val right: SparkPlan
  val condition: Option[Expression]

  override def output: Seq[Attribute] = left.output

  @transient protected lazy val rightKeyGenerator: Projection =
    newProjection(rightKeys, right.output)

  @transient protected lazy val leftKeyGenerator: () => MutableProjection =
    newMutableProjection(leftKeys, left.output)

  @transient private lazy val boundCondition =
    newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)

  protected def buildKeyHashSet(
      buildIter: Iterator[InternalRow],
      copy: Boolean): java.util.Set[InternalRow] = {
    val hashSet = new java.util.HashSet[InternalRow]()
    var currentRow: InternalRow = null

    // Create a Hash set of buildKeys
    while (buildIter.hasNext) {
      currentRow = buildIter.next()
      val rowKey = rightKeyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val keyExists = hashSet.contains(rowKey)
        if (!keyExists) {
          if (copy) {
            hashSet.add(rowKey.copy())
          } else {
            // rowKey may be not serializable (from codegen)
            hashSet.add(rowKey)
          }
        }
      }
    }
    hashSet
  }

  protected def hashSemiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    val joinKeys = leftKeyGenerator()
    val joinedRow = new JoinedRow
    streamIter.filter(current => {
      lazy val rowBuffer = hashedRelation.get(joinKeys.currentValue)
      !joinKeys(current).anyNull && rowBuffer != null && rowBuffer.exists {
        (build: InternalRow) => boundCondition(joinedRow(current, build))
      }
    })
  }

  protected def hashSemiJoin(
      streamIter: Iterator[InternalRow],
      hashSet: java.util.Set[InternalRow]): Iterator[InternalRow] = {
    val joinKeys = leftKeyGenerator()
    val joinedRow = new JoinedRow
    streamIter.filter(current => {
      !joinKeys(current.copy()).anyNull && hashSet.contains(joinKeys.currentValue)
    })
  }
}
