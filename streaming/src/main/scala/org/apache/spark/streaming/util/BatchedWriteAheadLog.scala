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

package org.apache.spark.streaming.util

import java.nio.ByteBuffer
import java.util.concurrent.{LinkedBlockingQueue, TimeoutException}
import java.util.{Iterator => JIterator}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.util.{Utils, ThreadUtils}

/**
 * A wrapper for a WriteAheadLog that batches records before writing data. All other methods will
 * be passed on to the wrapped class.
 */
private[streaming] class BatchedWriteAheadLog(parent: WriteAheadLog)
  extends WriteAheadLog with Logging {

  /**
   * Wrapper class for representing the records that we will write to the WriteAheadLog. Coupled with
   * the timestamp for the write request of the record, and the promise that will block the write
   * request, while a separate thread is actually performing the write.
   */
  private[util] case class RecordBuffer(
      record: ByteBuffer,
      time: Long,
      promise: Promise[WriteAheadLogRecordHandle])

  /** A thread pool for fulfilling log write promises */
  private val batchWriterThreadPool = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("wal-batch-writer-thead-pool"))

  // exposed for tests
  protected val walWriteQueue = new LinkedBlockingQueue[RecordBuffer]()

  private val WAL_WRITE_STATUS_TIMEOUT = 5000 // 5 seconds

  // Whether the writer thread is active
  private var active: Boolean = true
  private val buffer = new ArrayBuffer[RecordBuffer]()

  startBatchedWriterThread()

  /**
   * Write a byte buffer to the log file. This method adds the byteBuffer to a queue and blocks
   * until the record is properly written by the parent.
   */
  override def write(byteBuffer: ByteBuffer, time: Long): WriteAheadLogRecordHandle = {
    val promise = Promise[WriteAheadLogRecordHandle]()
    walWriteQueue.offer(RecordBuffer(byteBuffer, time, promise))
    try {
      Await.result(promise.future.recover { case _ => null }(batchWriterThreadPool),
        WAL_WRITE_STATUS_TIMEOUT.milliseconds)
    } catch {
      case e: TimeoutException =>
        logWarning(s"Write to Write Ahead Log promise timed out after " +
          s"$WAL_WRITE_STATUS_TIMEOUT millis for record.")
        null
    }
  }

  /**
   * Read a segment from an existing Write Ahead Log. The data may be aggregated, and the user
   * should de-aggregate using [[BatchedWriteAheadLog.deaggregate]]
   *
   * This method is handled by the parent WriteAheadLog.
   */
  override def read(segment: WriteAheadLogRecordHandle): ByteBuffer = {
    parent.read(segment)
  }

  /**
   * Read all the existing logs from the log directory.
   *
   * This method is handled by the parent WriteAheadLog.
   */
  override def readAll(): JIterator[ByteBuffer] = {
    parent.readAll().asScala.flatMap(BatchedWriteAheadLog.deaggregate).asJava
  }

  /**
   * Delete the log files that are older than the threshold time.
   *
   * This method is handled by the parent WriteAheadLog.
   */
  override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
    parent.clean(threshTime, waitForCompletion)
  }


  /**
   * Stop the batched writer thread, fulfill promises with failures and close parent writer.
   */
  override def close(): Unit = {
    logInfo("BatchedWriteAheadLog shutting down.")
    active = false
    fulfillPromises()
    batchWriterThreadPool.shutdownNow()
    parent.close()
  }

  /**
   * Respond to any promises that may have been left in the queue, to unblock receivers during
   * shutdown.
   */
  private def fulfillPromises(): Unit = {
    while (!walWriteQueue.isEmpty) {
      val RecordBuffer(_, _, promise) = walWriteQueue.poll()
      promise.success(null)
    }
  }

  /** Start the actual log writer on a separate thread. Visible(protected) for testing. */
  protected def startBatchedWriterThread(): Unit = {
    ThreadUtils.runInNewThread("Batched WAL Writer", isDaemon = true) {
      while (active) {
        try {
          flushRecords()
        } catch {
          case NonFatal(e) =>
            logWarning("Encountered exception in Batched Writer Thread.", e)
        }
      }
      logInfo("Batched WAL Writer thread exiting.")
    }
  }

  /** Write all the records in the buffer to the write ahead log. Visible for testing. */
  protected def flushRecords(): Unit = {
    try {
      buffer.append(walWriteQueue.take())
      val numBatched = walWriteQueue.drainTo(buffer.asJava) + 1
      logDebug(s"Received $numBatched records from queue")
    } catch {
      case _: InterruptedException =>
        logWarning("Batch Write Ahead Log Writer queue interrupted.")
    }
    try {
      var segment: WriteAheadLogRecordHandle = null
      if (buffer.length > 0) {
        logDebug(s"Batched ${buffer.length} records for Write Ahead Log write")
        // we take the latest record for the time to ensure that we don't clean up files earlier
        // than the expiration date of the records
        val time = buffer.last.time
        segment = parent.write(BatchedWriteAheadLog.aggregate(buffer), time)
      }
      buffer.foreach(_.promise.success(segment))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Batch WAL Writer failed to write $buffer", e)
        buffer.foreach(_.promise.success(null))
    }
    buffer.clear()
  }
}

/** Static methods for aggregating and de-aggregating records. */
private[streaming] object BatchedWriteAheadLog {
  /** Aggregate multiple serialized ReceivedBlockTrackerLogEvents in a single ByteBuffer. */
  private[streaming] def aggregate(records: Seq[RecordBuffer]): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(records.map(_.record.array().toArray)))
  }

  /**
   * De-aggregate serialized ReceivedBlockTrackerLogEvents in a single ByteBuffer.
   * A stream may not have used batching initially, but started using it after a restart. This
   * method therefore needs to be backwards compatible.
   */
  private[streaming] def deaggregate(buffer: ByteBuffer): Array[ByteBuffer] = {
    try {
      Utils.deserialize[Array[Array[Byte]]](buffer.array()).map(ByteBuffer.wrap)
    } catch {
      case _: ClassCastException => // users may restart a stream with batching enabled
        Array(buffer)
    }
  }
}
