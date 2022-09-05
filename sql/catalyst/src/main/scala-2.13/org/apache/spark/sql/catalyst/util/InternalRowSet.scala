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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Murmur3HashFunction, RowOrdering}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * A mutable Set with [[InternalRow]] as its element type. It uses Spark's internal murmur hash to
 * compute hash code from an row, and uses [[RowOrdering]] to perform equality checks.
 *
 * @param dataTypes the data types for the row keys this set holds
 */
class InternalRowSet(val dataTypes: Seq[DataType]) extends mutable.Set[InternalRow] {
  private val baseSet = new mutable.HashSet[InternalRowContainer]

  private val structType = StructType(dataTypes.map(t => StructField("f", t)))
  private val ordering = RowOrdering.createNaturalAscendingOrdering(dataTypes)

  override def contains(row: InternalRow): Boolean =
    baseSet.contains(new InternalRowContainer(row))

  private class InternalRowContainer(val row: InternalRow) {
    override def hashCode(): Int = Murmur3HashFunction.hash(row, structType, 42L).toInt

    override def equals(other: Any): Boolean = other match {
      case r: InternalRowContainer => ordering.compare(row, r.row) == 0
      case r => this == r
    }
  }

  override def addOne(row: InternalRow): InternalRowSet.this.type = {
    val rowKey = new InternalRowContainer(row)
    baseSet += rowKey
    this
  }

  override def subtractOne(row: InternalRow): InternalRowSet.this.type = {
    val rowKey = new InternalRowContainer(row)
    baseSet -= rowKey
    this
  }

  override def clear(): Unit = {
    baseSet.clear()
  }

  override def iterator: Iterator[InternalRow] = {
    baseSet.iterator.map(_.row)
  }
}
