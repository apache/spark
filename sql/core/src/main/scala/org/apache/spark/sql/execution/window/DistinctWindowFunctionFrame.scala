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

package org.apache.spark.sql.execution.window

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, UnsafeKVExternalSorter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.unsafe.map.BytesToBytesMap

/**
 * Computes one or more equivalent DISTINCT aggregate expressions for a frame whose lower bound is
 * UNBOUNDED PRECEDING.
 *
 * Such a frame only grows as output advances. For each qualifying input row this frame finds the
 * first output row whose upper bound contains it. A bounded BytesToBytesMap removes duplicates
 * up to the hash fallback threshold. If another new key arrives, the frame permanently falls back
 * to an external sorter for the rest of the window partition. That sorter finds the earliest event
 * for every key, then a second external sorter orders those events by index. `write` feeds each
 * unique, normalized DISTINCT input to the aggregate processor when its event becomes visible.
 */
private[window] final class DistinctWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    distinctExpressions: Seq[Expression],
    filter: Option[Expression],
    inputSchema: Seq[Attribute],
    upperBound: Option[BoundOrdering],
    spillSize: SQLMetric,
    hashFallbackThreshold: Int,
    spillThreshold: Int,
    spillSizeThreshold: Long)
  extends WindowFunctionFrame with AutoCloseable {

  private val distinctFields = distinctExpressions.zipWithIndex.map { case (expression, index) =>
    StructField(s"key$index", expression.dataType, expression.nullable)
  }
  private val distinctKeySchema = StructType(distinctFields)
  private val distinctTypes = distinctKeySchema.map(_.dataType)
  private val eventValueSchema = StructType(distinctFields.map { field =>
    field.copy(name = s"value${field.name.stripPrefix("key")}")
  })
  private val positionSchema = StructType(Seq(
    StructField("firstVisibleIndex", IntegerType, nullable = false),
    StructField("inputIndex", IntegerType, nullable = false)))

  private val distinctProjection = UnsafeProjection.create(distinctExpressions, inputSchema)
  private val distinctOrdering = RowOrdering.createNaturalAscendingOrdering(distinctTypes)
  private val canUseHashDedup = distinctTypes.forall(UnsafeRowUtils.isBinaryStable)
  private val boundFilter = filter.map(Predicate.create(_, inputSchema))
  private val positionInput =
    new SpecificInternalRow(Seq(IntegerType, IntegerType))
  private val positionProjection =
    UnsafeProjection.create(Array[DataType](IntegerType, IntegerType))

  private var eventSorter: UnsafeKVExternalSorter = _
  private var eventIterator: UnsafeKVExternalSorter#KVSorterIterator = _
  private var boundaryIterator: Iterator[UnsafeRow] = Iterator.empty
  private var nextBoundaryRow: UnsafeRow = _
  private var boundaryInputIndex = 0
  private var partitionSize = 0
  private var nextEventIndex = Int.MaxValue

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    closeEventResources()
    nextEventIndex = Int.MaxValue
    partitionSize = rows.length
    boundaryInputIndex = 0
    processor.initialize(partitionSize)
    val partitionIndex = Option(TaskContext.get()).map(_.partitionId()).getOrElse(0)
    boundFilter.foreach(_.initialize(partitionIndex))

    val firstVisibleRows = new FirstVisibleRowsBuilder
    var newEventSorter: UnsafeKVExternalSorter = null
    try {
      populateFirstVisibleRows(rows, firstVisibleRows)
      newEventSorter = firstVisibleRows.buildEvents()
      spillSize.add(firstVisibleRows.getSpillSize)
      spillSize.add(newEventSorter.getSpillSize)

      eventSorter = newEventSorter
      newEventSorter = null
      eventIterator = eventSorter.sortedIterator()
      loadNextEvent()
    } finally {
      firstVisibleRows.close()
      if (newEventSorter != null) {
        newEventSorter.cleanupResources()
      }
    }

    upperBound match {
      case Some(_) =>
        boundaryIterator = rows.generateIterator()
        nextBoundaryRow = WindowFunctionFrame.getNextOrNull(boundaryIterator)
      case None =>
        boundaryIterator = Iterator.empty
        nextBoundaryRow = null
        boundaryInputIndex = partitionSize
    }
  }

  private def populateFirstVisibleRows(
      rows: ExternalAppendOnlyUnsafeRowArray,
      firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    upperBound match {
      case None =>
        val iterator = rows.generateIterator()
        var inputIndex = 0
        while (iterator.hasNext) {
          addCandidate(iterator.next(), 0, inputIndex, firstVisibleRows)
          inputIndex += 1
        }

      case Some(bound) =>
        val inputIterator = rows.generateIterator()
        val outputIterator = rows.generateIterator()
        var nextInput = WindowFunctionFrame.getNextOrNull(inputIterator)
        var inputIndex = 0
        var outputIndex = 0

        while (outputIterator.hasNext && nextInput != null) {
          val currentOutput = outputIterator.next()
          while (nextInput != null &&
              bound.compare(nextInput, inputIndex, currentOutput, outputIndex) <= 0) {
            addCandidate(nextInput, outputIndex, inputIndex, firstVisibleRows)
            inputIndex += 1
            nextInput = WindowFunctionFrame.getNextOrNull(inputIterator)
          }
          outputIndex += 1
        }
    }
  }

  private def addCandidate(
      row: InternalRow,
      firstVisibleIndex: Int,
      inputIndex: Int,
      firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    if (boundFilter.forall(_.eval(row))) {
      positionInput.setInt(0, firstVisibleIndex)
      positionInput.setInt(1, inputIndex)
      val distinctRow = distinctProjection(row)
      firstVisibleRows.add(distinctRow, positionProjection(positionInput))
    }
  }

  /**
   * Uses a bounded BytesToBytesMap to remove duplicates before they reach the first external
   * sorter. The map contains only the earliest row for each distinct key because candidates arrive
   * in non-decreasing (firstVisibleIndex, inputIndex) order.
   *
   * BytesToBytesMap compares raw UnsafeRow bytes, so binary-unstable keys use the sorter directly.
   * Once the map is at the hash fallback threshold and another new key arrives, or it cannot append
   * a record, all map entries are transferred to one external sorter. The rest of this window
   * partition goes directly to that sorter and never returns to hash-based deduplication.
   */
  private final class FirstVisibleRowsBuilder extends AutoCloseable {
    private val map = if (canUseHashDedup) {
      val taskMemoryManager = TaskContext.get().taskMemoryManager()
      new BytesToBytesMap(taskMemoryManager, 64, taskMemoryManager.pageSizeBytes())
    } else {
      null
    }
    private var usingMap = map != null
    private var sorter: UnsafeKVExternalSorter = _

    def add(key: UnsafeRow, value: UnsafeRow): Unit = {
      if (!usingMap) {
        insertIntoSorter(key, value)
        return
      }

      val location = map.lookup(
        key.getBaseObject,
        key.getBaseOffset,
        key.getSizeInBytes)
      if (location.isDefined) {
        return
      }

      if (map.numKeys() >= hashFallbackThreshold ||
          map.getTotalMemoryConsumption >= spillSizeThreshold) {
        switchToSorter()
        insertIntoSorter(key, value)
      } else if (!location.append(
          key.getBaseObject,
          key.getBaseOffset,
          key.getSizeInBytes,
          value.getBaseObject,
          value.getBaseOffset,
          value.getSizeInBytes)) {
        switchToSorter()
        insertIntoSorter(key, value)
      }
    }

    def buildEvents(): UnsafeKVExternalSorter = {
      var events: UnsafeKVExternalSorter = null
      try {
        if (usingMap) {
          // Make the map spillable before allocating the event sorter. The destructive iterator
          // releases the hash array immediately and lets memory pressure spill remaining pages.
          val iterator = destructiveMapIterator()
          events = newSorter(positionSchema, eventValueSchema)
          drainMap(iterator, (key, position) => emitEvent(events, position, key))
        } else {
          events = newSorter(positionSchema, eventValueSchema)
          if (sorter != null) {
            DistinctWindowFunctionFrame.this.buildEvents(sorter, events)
          }
        }
        val result = events
        events = null
        result
      } finally {
        if (events != null) {
          events.cleanupResources()
        }
      }
    }

    def getSpillSize: Long = if (sorter == null) 0L else sorter.getSpillSize

    private def insertIntoSorter(key: UnsafeRow, value: UnsafeRow): Unit = {
      if (sorter == null) {
        sorter = newSorter(distinctKeySchema, positionSchema)
      }
      sorter.insertKV(key, value)
    }

    private def switchToSorter(): Unit = {
      assert(sorter == null)
      val iterator = destructiveMapIterator()
      sorter = newSorter(distinctKeySchema, positionSchema)
      drainMap(iterator, (key, position) => sorter.insertKV(key, position))
    }

    private def destructiveMapIterator(): BytesToBytesMap#MapIterator = {
      assert(usingMap)
      val iterator = map.destructiveIterator()
      usingMap = false
      iterator
    }

    private def drainMap(
        iterator: BytesToBytesMap#MapIterator,
        consume: (UnsafeRow, UnsafeRow) => Unit): Unit = {
      val key = new UnsafeRow(distinctKeySchema.length)
      val position = new UnsafeRow(positionSchema.length)
      while (iterator.hasNext) {
        val location = iterator.next()
        key.pointTo(
          location.getKeyBase,
          location.getKeyOffset,
          location.getKeyLength)
        position.pointTo(
          location.getValueBase,
          location.getValueOffset,
          location.getValueLength)
        consume(key, position)
      }
    }

    override def close(): Unit = {
      if (sorter != null) {
        sorter.cleanupResources()
        sorter = null
      }
      if (map != null) {
        map.free()
        usingMap = false
      }
    }
  }

  private def buildEvents(
      firstSorter: UnsafeKVExternalSorter,
      events: UnsafeKVExternalSorter): Unit = {
    val iterator = firstSorter.sortedIterator()
    var groupKey: UnsafeRow = null
    var selectedKey: UnsafeRow = null
    var selectedPosition: UnsafeRow = null
    try {
      while (iterator.next()) {
        val key = iterator.getKey
        val position = iterator.getValue
        if (groupKey == null) {
          groupKey = key.copy()
          selectedKey = groupKey
          selectedPosition = position.copy()
        } else if (distinctOrdering.compare(groupKey, key) == 0) {
          if (isEarlier(position, selectedPosition)) {
            selectedKey = key.copy()
            selectedPosition = position.copy()
          }
        } else {
          emitEvent(events, selectedPosition, selectedKey)
          groupKey = key.copy()
          selectedKey = groupKey
          selectedPosition = position.copy()
        }
      }
      if (groupKey != null) {
        emitEvent(events, selectedPosition, selectedKey)
      }
    } finally {
      iterator.close()
    }
  }

  private def isEarlier(left: InternalRow, right: InternalRow): Boolean = {
    val leftVisibleIndex = left.getInt(0)
    val rightVisibleIndex = right.getInt(0)
    leftVisibleIndex < rightVisibleIndex ||
      leftVisibleIndex == rightVisibleIndex && left.getInt(1) < right.getInt(1)
  }

  private def emitEvent(
      events: UnsafeKVExternalSorter,
      position: UnsafeRow,
      distinctValue: UnsafeRow): Unit = {
    events.insertKV(position, distinctValue)
  }

  private def newSorter(
      keySchema: StructType,
      valueSchema: StructType): UnsafeKVExternalSorter = {
    val taskContext = TaskContext.get()
    // The frame owns all of its sorters and registers one task completion listener at construction.
    UnsafeKVExternalSorter.createWithCallerOwnedLifecycle(
      keySchema,
      valueSchema,
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      taskContext.taskMemoryManager().pageSizeBytes,
      spillThreshold,
      spillSizeThreshold)
  }

  private def loadNextEvent(): Unit = {
    if (eventIterator != null && eventIterator.next()) {
      nextEventIndex = eventIterator.getKey.getInt(0)
    } else {
      nextEventIndex = Int.MaxValue
    }
  }

  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0
    while (nextEventIndex <= index) {
      processor.update(eventIterator.getValue)
      bufferUpdated = true
      loadNextEvent()
    }
    if (bufferUpdated) {
      processor.evaluate(target)
    }

    upperBound.foreach { bound =>
      while (nextBoundaryRow != null &&
          bound.compare(nextBoundaryRow, boundaryInputIndex, current, index) <= 0) {
        boundaryInputIndex += 1
        nextBoundaryRow = WindowFunctionFrame.getNextOrNull(boundaryIterator)
      }
    }
  }

  override def currentLowerBound(): Int = 0

  override def currentUpperBound(): Int = boundaryInputIndex

  private def closeEventResources(): Unit = {
    if (eventIterator != null) {
      eventIterator.close()
      eventIterator = null
    }
    if (eventSorter != null) {
      eventSorter.cleanupResources()
      eventSorter = null
    }
  }

  override def close(): Unit = closeEventResources()
}
