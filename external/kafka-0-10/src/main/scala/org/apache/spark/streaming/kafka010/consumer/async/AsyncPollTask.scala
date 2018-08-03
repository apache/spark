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

package org.apache.spark.streaming.kafka010.consumer.async

import java.{util => ju}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.concurrent.{ExecutionContext, Future}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging

class AsyncPollTask[K, V](
                           topicPartition: TopicPartition,
                           executionContext: ExecutionContext,
                           kafkaConsumer: KafkaConsumer[K, V],
                           maintainBufferSize: Long,
                           pollTimeout: Long,
                           startOffset: Long
                         ) extends Logging {

  var bufferList: ThreadSafeEfficientConcurrentLinkedQueue[ConsumerRecord[K, V]] =
    new ThreadSafeEfficientConcurrentLinkedQueue[ConsumerRecord[K, V]]()
  private val maintainBufferMin = Math.max(maintainBufferSize, 1)
  private val currentSeekOffset: AtomicLong = new AtomicLong(
    startOffset
  )
  private val lastRequestedAt: AtomicLong = new AtomicLong(System.nanoTime())
  private val shouldClose: AtomicBoolean = new AtomicBoolean(false)
  private val error: AtomicReference[Throwable] = new AtomicReference[Throwable](null);

  private val future = Future {
    var listSize: Int = 0
    while (!shouldClose.get()) {
      val lastRequestedAtBefore = lastRequestedAt.get()
      listSize = bufferList.size()
      logDebug(s"$topicPartition Buffersize is $listSize and maintain buffer is $maintainBufferMin")
      if (listSize < maintainBufferMin) {
        var records: ju.List[ConsumerRecord[K, V]] = null
        try {
          records = fetch(currentSeekOffset.get())
          if(error.get() != null) {
            error.set(null)
          }
        }
        catch {
          case x: Throwable =>
            logError(s"$topicPartition Exception in fetch", x)
            error.set(x)
        }
        var count = if (records == null) 0 else records.size()
        if (count == 0) {
          waitForReactivation(lastRequestedAtBefore)
        } else {
          logDebug(s"$topicPartition Adding $count records to buffer")
          currentSeekOffset.set(records.get(count - 1).offset() + 1)
          bufferList.addAll(records)
        }
      } else {
        logDebug(s"$topicPartition Since buffer is sufficient")
        waitForReactivation(lastRequestedAtBefore)
      }
    }
    logDebug(s"$topicPartition Closing consumer")
    kafkaConsumer.close()
  }(executionContext)

  private def waitForReactivation(lastRequestedAtBefore: Long) = {
    lastRequestedAt.synchronized {
      if (lastRequestedAtBefore == lastRequestedAt.get()) {
        logDebug(s"$topicPartition Future going to sleep")
        lastRequestedAt.wait()
        logDebug(s"$topicPartition Future is awake")
      } else {
        logDebug(s"$topicPartition Last requested value mismatched Before : $lastRequestedAtBefore"
          + " Current : " + lastRequestedAt.get() + ", Skipping wait.")
      }
    }
  }

  private def fetch(seekOffset: Long): ju.List[ConsumerRecord[K, V]] = {
    var result: ju.List[ConsumerRecord[K, V]] = new ju.ArrayList[ConsumerRecord[K, V]]()
    var count = 0
    if (seekOffset != AsyncSparkKafkaConsumer.UNKNOWN_OFFSET) {
      logDebug(s"$topicPartition Polling from offset $seekOffset")
      kafkaConsumer.seek(topicPartition, seekOffset)
      val records = kafkaConsumer.poll(pollTimeout)
      count = records.count()
      logDebug(s"$topicPartition Received records $count offset $seekOffset " + this.toString)
      result = records.records(topicPartition)
    }
    result
  }

  def ensureAsyncTaskRunning(): Unit = {
    lastRequestedAt.synchronized {
      lastRequestedAt.set(System.nanoTime())
      lastRequestedAt.notify()
    }
  }

  def resetOffset(offset: Long): Unit = {
    logDebug(s"$topicPartition Clearing buffer and resetting offset from old value "
      + currentSeekOffset.get() + " to new value " + offset + s"for partition $topicPartition"
      + this.toString)
    bufferList.clear()
    currentSeekOffset.set(offset)
    logDebug(s"$topicPartition Notifying sleeping poll task for partition $topicPartition")
    ensureAsyncTaskRunning()
  }

  def getNextOffset(): Long =
    if (bufferList.size() == 0) currentSeekOffset.get else bufferList.peekFirst().offset

  def getNextRecord(timeout: Long): ConsumerRecord[K, V] = {
    ensureAsyncTaskRunning
    if(error.get() != null) {
      throw error.get()
    }
    bufferList.pollFirst(timeout, TimeUnit.MILLISECONDS)
  }

  def revertLastRecord(record: ConsumerRecord[K, V]): Unit = bufferList.addFirst(record)

  /**
   * Close async kafka consumer. It uses future to close the consumer
   * since task could be running and throw exception if closed directly
   */
  def close(): Unit = {
    logDebug(s"$topicPartition Mark consumer for closure")
    shouldClose.set(true)
    resetOffset(AsyncSparkKafkaConsumer.UNKNOWN_OFFSET)
  }

}

private[kafka010] class ThreadSafeEfficientConcurrentLinkedQueue[T] {
  val queue: ju.concurrent.LinkedBlockingDeque[T] = new ju.concurrent.LinkedBlockingDeque[T]()

  def addAll(list: ju.List[T]): Unit = queue.synchronized {
    queue.addAll(list)
    queue.notifyAll()
  }

  def add(record: T): Unit = queue.synchronized {
    queue.add(record)
    queue.notifyAll()
  }

  def addFirst(record: T): Unit = queue.synchronized {
    queue.addFirst(record)
    queue.notifyAll()
  }

  def clear(): Unit = queue.synchronized {
    queue.clear()
    queue.notifyAll()
  }

  def pollFirst(timeout: Long, timeunit: TimeUnit): T = {
    queue.synchronized {
      var rec = queue.peekFirst()
      if (rec == null) {
        queue.wait(timeunit.toMillis(timeout))
      }
    }
    queue.pollFirst()
  }

  def peekFirst(): T = queue.synchronized {
    queue.peekFirst()
  }

  def size(): Int = queue.synchronized {
    queue.size()
  }

}
