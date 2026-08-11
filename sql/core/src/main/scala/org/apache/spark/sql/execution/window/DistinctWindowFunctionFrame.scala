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
import org.apache.spark.memory.SparkOutOfMemoryError
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
 * Such a frame never removes rows: it either covers the entire partition or only grows as output
 * advances. For each qualifying input row this frame finds the first output row whose upper bound
 * contains it. A BytesToBytesMap removes duplicates while it remains below configurable key-count
 * and memory-size soft limits. If the map cannot be allocated, or if another new key arrives after
 * either limit has been reached or an append fails, the frame permanently falls back to an
 * external sorter for the rest of the window partition. For a growing frame, that sorter finds the
 * earliest event for every key, then a second external sorter orders those events by
 * (firstVisibleIndex, inputIndex). The frame feeds each unique, normalized DISTINCT input to the
 * aggregate processor when its event becomes visible. A frame covering the entire partition
 * consumes unique inputs directly because their order is undefined.
 */
private[window] abstract class DistinctWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    distinctExpressions: Seq[Expression],
    filter: Option[Expression],
    inputSchema: Seq[Attribute],
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
  private var nextEventIndex = Int.MaxValue

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override final def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    closeEventResources()
    nextEventIndex = Int.MaxValue
    processor.initialize(rows.length)
    val partitionIndex = Option(TaskContext.get()).map(_.partitionId()).getOrElse(0)
    boundFilter.foreach(_.initialize(partitionIndex))

    val firstVisibleRows = new FirstVisibleRowsBuilder
    try {
      populateFirstVisibleRows(rows, firstVisibleRows)
      processDistinctRows(firstVisibleRows)
      spillSize.add(firstVisibleRows.getSpillSize)
    } finally {
      firstVisibleRows.close()
    }
    prepareFrame(rows)
  }

  protected def populateFirstVisibleRows(
      rows: ExternalAppendOnlyUnsafeRowArray,
      firstVisibleRows: FirstVisibleRowsBuilder): Unit

  protected def processDistinctRows(firstVisibleRows: FirstVisibleRowsBuilder): Unit

  protected def prepareFrame(rows: ExternalAppendOnlyUnsafeRowArray): Unit

  protected final def prepareOrderedEvents(firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    var newEventSorter: UnsafeKVExternalSorter = null
    try {
      newEventSorter = firstVisibleRows.buildEvents()
      eventSorter = newEventSorter
      newEventSorter = null
      eventIterator = eventSorter.sortedIterator()
      loadNextEvent()
    } finally {
      if (newEventSorter != null) {
        newEventSorter.cleanupResources()
      }
    }
  }

  protected final def addCandidate(
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
   * If map construction fails, inputs go directly to the sorter. Once the map reaches either its
   * key-count or memory-size soft limit and another new key arrives, or it cannot append a record,
   * all map entries are transferred to one external sorter. The rest of this window partition goes
   * directly to that sorter and never returns to hash-based deduplication.
   */
  protected final class FirstVisibleRowsBuilder extends AutoCloseable {
    private val map = if (canUseHashDedup) {
      val taskMemoryManager = TaskContext.get().taskMemoryManager()
      try {
        new BytesToBytesMap(taskMemoryManager, 64, taskMemoryManager.pageSizeBytes())
      } catch {
        case _: SparkOutOfMemoryError => null
      }
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
          events = newSorter(positionSchema, distinctKeySchema)
          drainMap(iterator, (key, position) => emitEvent(events, position, key))
        } else {
          events = newSorter(positionSchema, distinctKeySchema)
          if (sorter != null) {
            consumeDistinctRows(
              sorter, (key, position) => emitEvent(events, position, key))
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

    def foreachDistinctRow(consume: (UnsafeRow, UnsafeRow) => Unit): Unit = {
      if (usingMap) {
        drainMap(destructiveMapIterator(), consume)
      } else if (sorter != null) {
        consumeDistinctRows(sorter, consume)
      }
    }

    /**
     * Returns bytes spilled by the first, distinct-key sorter. The frame accounts for the second,
     * event-order sorter separately when closing its event resources. Spill files created while a
     * destructive BytesToBytesMap iterator releases the map's data pages are not included in the
     * window spill metric.
     */
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

  private def consumeDistinctRows(
      firstSorter: UnsafeKVExternalSorter,
      consume: (UnsafeRow, UnsafeRow) => Unit): Unit = {
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
          consume(selectedKey, selectedPosition)
          groupKey = key.copy()
          selectedKey = groupKey
          selectedPosition = position.copy()
        }
      }
      if (groupKey != null) {
        consume(selectedKey, selectedPosition)
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

  protected final def updateProcessor(index: Int): Unit = {
    var bufferUpdated = index == 0
    while (nextEventIndex <= index) {
      processor.update(eventIterator.getValue)
      bufferUpdated = true
      loadNextEvent()
    }
    if (bufferUpdated) {
      processor.evaluate(target)
    }
  }

  override final def currentLowerBound(): Int = 0

  private def closeEventResources(): Unit = {
    if (eventSorter != null) {
      spillSize.add(eventSorter.getSpillSize)
    }
    if (eventIterator != null) {
      eventIterator.close()
      eventIterator = null
    }
    if (eventSorter != null) {
      eventSorter.cleanupResources()
      eventSorter = null
    }
  }

  override final def close(): Unit = closeEventResources()
}

/**
 * Computes DISTINCT aggregates over the entire window partition. Since every output row has the
 * same set of unique inputs, their consumption order is undefined and is not restored after hash
 * deduplication or sorting by the DISTINCT key.
 */
private[window] final class UnboundedDistinctWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    distinctExpressions: Seq[Expression],
    filter: Option[Expression],
    inputSchema: Seq[Attribute],
    spillSize: SQLMetric,
    hashFallbackThreshold: Int,
    spillThreshold: Int,
    spillSizeThreshold: Long)
  extends DistinctWindowFunctionFrame(
    target,
    processor,
    distinctExpressions,
    filter,
    inputSchema,
    spillSize,
    hashFallbackThreshold,
    spillThreshold,
    spillSizeThreshold) {

  private var partitionSize = 0

  override protected def populateFirstVisibleRows(
      rows: ExternalAppendOnlyUnsafeRowArray,
      firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    val iterator = rows.generateIterator()
    var inputIndex = 0
    while (iterator.hasNext) {
      addCandidate(iterator.next(), 0, inputIndex, firstVisibleRows)
      inputIndex += 1
    }
  }

  override protected def processDistinctRows(
      firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    firstVisibleRows.foreachDistinctRow((key, _) => processor.update(key))
  }

  override protected def prepareFrame(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    partitionSize = rows.length
    processor.evaluate(target)
  }

  override def write(index: Int, current: InternalRow): Unit = {}

  override def currentUpperBound(): Int = partitionSize
}

/**
 * Computes DISTINCT aggregates for a growing frame with an UNBOUNDED PRECEDING lower bound.
 */
private[window] final class UnboundedPrecedingDistinctWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    distinctExpressions: Seq[Expression],
    filter: Option[Expression],
    inputSchema: Seq[Attribute],
    upperBound: BoundOrdering,
    spillSize: SQLMetric,
    hashFallbackThreshold: Int,
    spillThreshold: Int,
    spillSizeThreshold: Long)
  extends DistinctWindowFunctionFrame(
    target,
    processor,
    distinctExpressions,
    filter,
    inputSchema,
    spillSize,
    hashFallbackThreshold,
    spillThreshold,
    spillSizeThreshold) {

  private val rowOffset = upperBound match {
    case RowBoundOrdering(offset) => Some(offset)
    case _ => None
  }

  private var partitionSize = 0
  private var boundaryRows: ExternalAppendOnlyUnsafeRowArray = _
  private var boundaryIterator: Iterator[UnsafeRow] = Iterator.empty
  private var nextBoundaryRow: UnsafeRow = _
  private var boundaryInputIndex = 0
  private var currentOutput: InternalRow = _
  private var currentOutputIndex = 0

  override protected def populateFirstVisibleRows(
      rows: ExternalAppendOnlyUnsafeRowArray,
      firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    rowOffset match {
      case Some(offset) =>
        // A ROWS bound depends only on row indexes. Calculate the first visible output row
        // directly instead of scanning the partition again as output rows advance.
        val inputIterator = rows.generateIterator()
        var inputIndex = 0
        while (inputIterator.hasNext) {
          val input = inputIterator.next()
          val firstVisibleIndex = inputIndex.toLong - offset.toLong
          if (firstVisibleIndex < rows.length) {
            addCandidate(
              input,
              math.max(firstVisibleIndex, 0L).toInt,
              inputIndex,
              firstVisibleRows)
          }
          inputIndex += 1
        }

      case None =>
        val inputIterator = rows.generateIterator()
        val outputIterator = rows.generateIterator()
        var nextInput = WindowFunctionFrame.getNextOrNull(inputIterator)
        var inputIndex = 0
        var outputIndex = 0

        while (outputIterator.hasNext && nextInput != null) {
          val currentOutput = outputIterator.next()
          while (nextInput != null &&
              upperBound.compare(nextInput, inputIndex, currentOutput, outputIndex) <= 0) {
            addCandidate(nextInput, outputIndex, inputIndex, firstVisibleRows)
            inputIndex += 1
            nextInput = WindowFunctionFrame.getNextOrNull(inputIterator)
          }
          outputIndex += 1
        }
    }
  }

  override protected def processDistinctRows(
      firstVisibleRows: FirstVisibleRowsBuilder): Unit = {
    prepareOrderedEvents(firstVisibleRows)
  }

  override protected def prepareFrame(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    partitionSize = rows.length
    boundaryInputIndex = 0
    currentOutput = null
    currentOutputIndex = 0
    if (rowOffset.isEmpty) {
      boundaryRows = rows
      boundaryIterator = Iterator.empty
      nextBoundaryRow = null
    } else {
      boundaryRows = null
      boundaryIterator = Iterator.empty
      nextBoundaryRow = null
    }
  }

  override def write(index: Int, current: InternalRow): Unit = {
    updateProcessor(index)
    rowOffset match {
      case Some(offset) =>
        // The upper bound is exclusive, hence the extra one after applying the ROWS offset.
        val upperBoundIndex = index.toLong + offset.toLong + 1L
        boundaryInputIndex = math.max(0L, math.min(upperBoundIndex, partitionSize.toLong)).toInt

      case None =>
        currentOutput = current
        currentOutputIndex = index
    }
  }

  override def currentUpperBound(): Int = {
    if (rowOffset.isEmpty && currentOutput != null) {
      // Only the Arrow Python evaluator reads frame bounds. Avoid this extra partition scan for
      // ordinary SQL evaluation, where DISTINCT aggregate frames never need the bounds.
      if (boundaryRows != null) {
        boundaryIterator = boundaryRows.generateIterator()
        nextBoundaryRow = WindowFunctionFrame.getNextOrNull(boundaryIterator)
        boundaryRows = null
      }
      while (nextBoundaryRow != null && upperBound.compare(
          nextBoundaryRow,
          boundaryInputIndex,
          currentOutput,
          currentOutputIndex) <= 0) {
        boundaryInputIndex += 1
        nextBoundaryRow = WindowFunctionFrame.getNextOrNull(boundaryIterator)
      }
    }
    boundaryInputIndex
  }
}
