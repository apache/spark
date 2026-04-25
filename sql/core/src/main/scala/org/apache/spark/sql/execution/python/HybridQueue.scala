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

package org.apache.spark.sql.execution.python

import java.io._

import scala.Enumeration

import org.apache.spark.memory.{MemoryConsumer, SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.memory.MemoryBlock

/**
 * Enum to represent the storage mode for hybrid queues.
 */
object QueueMode extends Enumeration {
  type QueueMode = Value
  val IN_MEMORY, DISK = Value
}

trait Queue[T] {
  def add(item: T): Boolean
  def remove(): T
  def close(): Unit
}

/**
 * A generic base class for hybrid queues that can store data either in memory or on disk.
 * This class contains common logic for queue management, spilling, and memory management.
 */
abstract class HybridQueue[T, Q <: Queue[T]](
    memManager: TaskMemoryManager,
    tempDir: File,
    serMgr: SerializerManager)
  extends MemoryConsumer(memManager, memManager.getTungstenMemoryMode) {

  // Each buffer should have at least one element.
  protected var queues = new java.util.LinkedList[Q]()

  private var writing: Q = _
  protected var reading: Q = _

  protected var numElementsQueuedOnDisk: Long = 0L
  protected var numElementsQueued: Long = 0L

  // exposed for testing
  private[python] def numQueues(): Int = queues.size()

  protected def createDiskQueue(): Q
  protected def createInMemoryQueue(page: MemoryBlock): Q
  protected def getRequiredSize(item: T): Long
  protected def getPageSize(queue: Q): Long
  protected def isInMemoryQueue(queue: Q): Boolean
  protected def isReadingFromDiskQueue: Boolean = !isInMemoryQueue(reading)

  def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger == this) {
      // When it's triggered by itself, it should write upcoming elements into disk instead of
      // copying the elements already in the queue.
      return 0L
    }
    var released = 0L
    synchronized {
      // poll out all the buffers and add them back in the same order to make sure that the elements
      // are in correct order.
      val newQueues = new java.util.LinkedList[Q]()
      while (!queues.isEmpty) {
        val queue = queues.remove()
        val newQueue = if (!queues.isEmpty && isInMemoryQueue(queue)) {
          val diskQueue = createDiskQueue()
          var item = queue.remove()
          while (item != null) {
            diskQueue.add(item)
            item = queue.remove()
          }
          released += getPageSize(queue)
          queue.close()
          diskQueue
        } else {
          queue
        }
        newQueues.add(newQueue)
      }
      queues = newQueues
    }
    released
  }

  private def createNewQueue(required: Long): Q = {
    // Tests may attempt to force spills.
    val page = try {
      allocatePage(required)
    } catch {
      case _: SparkOutOfMemoryError =>
        null
    }
    val buffer = if (page != null) {
      createInMemoryQueue(page)
    } else {
      createDiskQueue()
    }

    synchronized {
      queues.add(buffer)
    }
    buffer
  }

  def add(item: T): QueueMode.Value = {
    if (writing == null || !writing.add(item)) {
      writing = createNewQueue(getRequiredSize(item))
      if (!writing.add(item)) {
        throw QueryExecutionErrors.failedToPushRowIntoRowQueueError(writing.toString)
      }
    }
    numElementsQueued += 1
    if (isInMemoryQueue(writing)) {
      QueueMode.IN_MEMORY
    } else {
      numElementsQueuedOnDisk += 1
      QueueMode.DISK
    }
  }

  def remove(): T = {
    var item: T = null.asInstanceOf[T]
    if (reading != null) {
      item = reading.remove()
    }
    if (item == null) {
      if (reading != null) {
        reading.close()
      }
      synchronized {
        reading = queues.remove()
      }
      assert(reading != null, s"queue should not be empty")
      item = reading.remove()
      assert(item != null, s"$reading should have at least one element")
    }
    if (!isInMemoryQueue(reading)) {
      numElementsQueuedOnDisk -= 1
    }
    numElementsQueued -= 1
    item
  }

  def close(): Unit = {
    if (reading != null) {
      reading.close()
      reading = null.asInstanceOf[Q]
    }
    synchronized {
      while (!queues.isEmpty) {
        queues.remove().close()
      }
    }
  }

  def getNumElementsQueuedOnDisk(): Long = numElementsQueuedOnDisk
  def getNumElementsQueued(): Long = numElementsQueued
}
