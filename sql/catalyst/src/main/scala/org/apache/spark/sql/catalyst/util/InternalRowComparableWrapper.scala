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
import org.apache.spark.sql.catalyst.expressions.{Expression, Murmur3HashFunction, RowOrdering}
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Wraps the [[InternalRow]] with the corresponding [[DataType]] to make it comparable with
 * the values in [[InternalRow]].
 * It uses Spark's internal murmur hash to compute hash code from an row, and uses [[RowOrdering]]
 * to perform equality checks.
 *
 * @param dataTypes the data types for the row
 */
class InternalRowComparableWrapper(val row: InternalRow, val dataTypes: Seq[DataType]) {

  private val structType = StructType(dataTypes.map(t => StructField("f", t)))
  private val ordering = RowOrdering.createNaturalAscendingOrdering(dataTypes)

  override def hashCode(): Int = Murmur3HashFunction.hash(row, structType, 42L).toInt

  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[InternalRowComparableWrapper]) {
      return false
    }
    val otherWrapper = other.asInstanceOf[InternalRowComparableWrapper]
    if (!otherWrapper.dataTypes.equals(this.dataTypes)) {
      return false
    }
    ordering.compare(row, otherWrapper.row) == 0
  }
}

object InternalRowComparableWrapper {

  def apply(
      partition: InputPartition with HasPartitionKey,
      partitionExpression: Seq[Expression]): InternalRowComparableWrapper = {
    new InternalRowComparableWrapper(
      partition.asInstanceOf[HasPartitionKey].partitionKey(), partitionExpression.map(_.dataType))
  }

  def apply(
      partitionRow: InternalRow,
      partitionExpression: Seq[Expression]): InternalRowComparableWrapper = {
    new InternalRowComparableWrapper(partitionRow, partitionExpression.map(_.dataType))
  }

  def mergePartitions(
      leftPartitioning: KeyGroupedPartitioning,
      rightPartitioning: KeyGroupedPartitioning,
      partitionExpression: Seq[Expression]): Seq[InternalRow] = {
    val partitionDataTypes = partitionExpression.map(_.dataType)
    val partitionsSet = new mutable.HashSet[InternalRowComparableWrapper]
    leftPartitioning.partitionValues
      .map(new InternalRowComparableWrapper(_, partitionDataTypes))
      .foreach(partition => partitionsSet.add(partition))
    rightPartitioning.partitionValues
      .map(new InternalRowComparableWrapper(_, partitionDataTypes))
      .foreach(partition => partitionsSet.add(partition))
    partitionsSet.map(_.row).toSeq
  }
}
