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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseOrdering, Murmur3HashFunction, RowOrdering, UnsafeRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.map.BytesToBytesMap

/** Hashing and equality operations for rows whose schema may contain collated strings. */
final class UnsafeRowKeyOperations(val schema: StructType)
    extends BytesToBytesMap.KeyOperationsFactory {

  private def createOrdering(): BaseOrdering = {
    RowOrdering.createNaturalAscendingOrdering(schema.map(_.dataType).toSeq)
  }

  val ordering: BaseOrdering =
    createOrdering()

  def hash(row: InternalRow): Int = {
    Murmur3HashFunction.hash(
      row,
      schema,
      42L,
      isCollationAware = true,
      // This flag only affects hashing when isCollationAware is false.
      legacyCollationAwareHashing = false).toInt
  }

  def areEqual(left: InternalRow, right: InternalRow): Boolean = {
    ordering.compare(left, right) == 0
  }

  override def create(): BytesToBytesMap.KeyOperations = new BytesToBytesMap.KeyOperations {
    private val localOrdering = createOrdering()
    private val left = new UnsafeRow(schema.length)
    private val right = new UnsafeRow(schema.length)

    override def hash(base: AnyRef, offset: Long, length: Int): Int = {
      left.pointTo(base, offset, length)
      UnsafeRowKeyOperations.this.hash(left)
    }

    override def equals(
        leftBase: AnyRef,
        leftOffset: Long,
        leftLength: Int,
        rightBase: AnyRef,
        rightOffset: Long,
        rightLength: Int): Boolean = {
      left.pointTo(leftBase, leftOffset, leftLength)
      right.pointTo(rightBase, rightOffset, rightLength)
      localOrdering.compare(left, right) == 0
    }
  }
}
