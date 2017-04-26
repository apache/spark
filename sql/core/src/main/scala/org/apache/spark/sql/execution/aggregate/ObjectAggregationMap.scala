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

package org.apache.spark.sql.execution.aggregate

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, TypedImperativeAggregate}
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

/**
 * An aggregation map that supports using safe `SpecificInternalRow`s aggregation buffers, so that
 * we can support storing arbitrary Java objects as aggregate function states in the aggregation
 * buffers. This class is only used together with [[ObjectHashAggregateExec]].
 */
class ObjectAggregationMap(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager) {

  // A hash map using grouping row as key, the value is a Tuple3, which consists of buffer row, its
  // size, and updating count.
  private[this] val hashMap = new ju.LinkedHashMap[UnsafeRow, (InternalRow, Long, Int)]

  def getAggregationBuffer(groupingKey: UnsafeRow): (InternalRow, Long, Int) = {
    if (hashMap.containsKey(groupingKey)) {
      val currentBuffer = hashMap.get(groupingKey)
      val newBuffer = currentBuffer.copy(_3 = currentBuffer._3 + 1)
      hashMap.replace(groupingKey, newBuffer)
      newBuffer
    } else {
      null
    }
  }

  def updateSize(groupingKey: UnsafeRow, newSize: Long): Boolean = {
    val buffer = hashMap.get(groupingKey)
    assert(buffer != null)

    val oldSize = buffer._2
    if (newSize == oldSize) {
      true
    } else if (newSize > oldSize) {
      val required = newSize - oldSize
      val granted = acquireMemory(required)
      if (granted < required) {
        freeMemory(granted)
        false
      } else {
        hashMap.replace(groupingKey, (buffer._1, newSize, 0))
        true
      }
    } else {
      freeMemory(oldSize - newSize)
      hashMap.replace(groupingKey, (buffer._1, newSize, 0))
      true
    }
  }

  def putAggregationBuffer(groupingKey: UnsafeRow, aggBuffer: (InternalRow, Long, Int)): Boolean = {
    val granted = acquireMemory(aggBuffer._2)
    if (granted < aggBuffer._2) {
      freeMemory(granted)
      false
    } else {
      hashMap.put(groupingKey, aggBuffer)
      true
    }
  }

  def iterator: Iterator[AggregationBufferEntry] = {
    val iter = hashMap.asScala.mapValues(_._1).iterator
    new Iterator[AggregationBufferEntry] {

      override def hasNext: Boolean = {
        iter.hasNext
      }
      override def next(): AggregationBufferEntry = {
        val entry = iter.next()
        new AggregationBufferEntry(entry._1, entry._2)
      }
    }
  }

  /**
   * Dumps all entries into a newly created external sorter, clears the hash map, and returns the
   * external sorter.
   */
  def dumpToExternalSorter(
      groupingAttributes: Seq[Attribute],
      aggregateFunctions: Seq[AggregateFunction]): UnsafeKVExternalSorter = {
    val aggBufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val sorter = new UnsafeKVExternalSorter(
      StructType.fromAttributes(groupingAttributes),
      StructType.fromAttributes(aggBufferAttributes),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      SparkEnv.get.conf.getLong(
        "spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD),
      null
    )

    val mapIterator = iterator
    val unsafeAggBufferProjection =
      UnsafeProjection.create(aggBufferAttributes.map(_.dataType).toArray)

    while (mapIterator.hasNext) {
      val entry = mapIterator.next()
      aggregateFunctions.foreach {
        case agg: TypedImperativeAggregate[_] =>
          agg.serializeAggregateBufferInPlace(entry.aggregationBuffer)
        case _ =>
      }

      sorter.insertKV(
        entry.groupingKey,
        unsafeAggBufferProjection(entry.aggregationBuffer)
      )
    }

    clear()
    sorter
  }

  private def clear(): Unit = {
    val totalSize = hashMap.asScala.mapValues(_._2).values.sum
    hashMap.clear()
    freeMemory(totalSize)
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
}

// Stores the grouping key and aggregation buffer
class AggregationBufferEntry(var groupingKey: UnsafeRow, var aggregationBuffer: InternalRow)
