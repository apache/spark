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
import java.util.concurrent.LinkedBlockingQueue
import java.util.{Iterator => JIterator}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.util.Utils

/**
 * A wrapper for a WriteAheadLog that batches records before writing data. Handles aggregation
 * during writes, and de-aggregation in the `readAll` method. The end consumer has to handle
 * de-aggregation after the `read` method. In addition, the `WriteAheadLogRecordHandle` returned
 * after the write will contain the batch of records rather than individual records.
 *
 * When writing a batch of records, the `time` passed to the `wrappedLog` will be the timestamp
 * of the latest record in the batch. This is very important in achieving correctness. Consider the
 * following example:
 * We receive records with timestamps 1, 3, 5, 7. We use "log-1" as the filename. Once we receive
 * a clean up request for timestamp 3, we would clean up the file "log-1", and lose data regarding
 * 5 and 7.
 *
 * All other methods of the WriteAheadLog interface will be passed on to the wrapped WriteAheadLog.
 */
private[util] class BatchedWriteAheadLog(val wrappedLog: WriteAheadLog)
  extends WriteAheadLog with Logging {

  import BatchedWriteAheadLog._

  // exposed for tests
  private val walWriteQueue = new LinkedBlockingQueue[Record]()

  // Whether the writer thread is active
  @volatile private var active: Boolean = true
  private val buffer = new ArrayBuffer[Record]()

  private val batchedWriterThread = startBatchedWriterThread()

  /**
   * Write a byte buffer to the log file. This method adds the byteBuffer to a queue and blocks
   * until the record is properly written by the parent.
   */
  override def write(byteBuffer: ByteBuffer, time: Long): WriteAheadLogRecordHandle = {
    val promise = Promise[WriteAheadLogRecordHandle]()
    walWriteQueue.offer(Record(byteBuffer, time, promise))
    Await.result(promise.future, WAL_WRITE_STATUS_TIMEOUT.milliseconds)
  }

  /**
   * Read a segment from an existing Write Ahead Log. The data may be aggregated, and the user
   * should de-aggregate using [[BatchedWriteAheadLog.deaggregate]]
   *
   * This method is handled by the parent WriteAheadLog.
   */
  override def read(segment: WriteAheadLogRecordHandle): ByteBuffer = {
    wrappedLog.read(segment)
  }

  /**
   * Read all the existing logs from the log directory. The output of the wrapped WriteAheadLog
   * will be de-aggregated.
   */
  override def readAll(): JIterator[ByteBuffer] = {
    wrappedLog.readAll().asScala.flatMap(deaggregate).asJava
  }

  /**
   * Delete the log files that are older than the threshold time.
   *
   * This method is handled by the parent WriteAheadLog.
   */
  override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
    wrappedLog.clean(threshTime, waitForCompletion)
  }


  /**
   * Stop the batched writer thread, fulfill promises with failures and close the wrapped WAL.
   */
  override def close(): Unit = {
    logInfo("BatchedWriteAheadLog shutting down.")
    active = false
    batchedWriterThread.interrupt()
    batchedWriterThread.join()
    while (!walWriteQueue.isEmpty) {
      val Record(_, _, promise) = walWriteQueue.poll()
      promise.failure(new SparkException("close() was called on BatchedWriteAheadLog."))
    }
    wrappedLog.close()
  }

  /** Start the actual log writer on a separate thread. */
  private def startBatchedWriterThread(): Thread = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
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
    }, "Batched WAL Writer")
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Write all the records in the buffer to the write ahead log. */
  private def flushRecords(): Unit = {
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
        // We take the latest record for the timestamp. Please refer to the class Javadoc for
        // detailed explanation
        val time = buffer.last.time
        segment = wrappedLog.write(aggregate(buffer), time)
      }
      buffer.foreach(_.promise.success(segment))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Batch WAL Writer failed to write $buffer", e)
        buffer.foreach(_.promise.failure(e))
    } finally {
      buffer.clear()
    }
  }

  /** Method for querying the queue length during tests. */
  private[util] def getQueueLength(): Int = walWriteQueue.size()
}

/** Static methods for aggregating and de-aggregating records. */
private[util] object BatchedWriteAheadLog {

  val WAL_WRITE_STATUS_TIMEOUT = 5000 // 5 seconds

  /**
   * Wrapper class for representing the records that we will write to the WriteAheadLog. Coupled
   * with the timestamp for the write request of the record, and the promise that will block the
   * write request, while a separate thread is actually performing the write.
   */
  case class Record(data: ByteBuffer, time: Long, promise: Promise[WriteAheadLogRecordHandle])

  /** Copies the byte array of a ByteBuffer. */
  private def getByteArray(buffer: ByteBuffer): Array[Byte] = {
    val byteArray = new Array[Byte](buffer.remaining())
    buffer.get(byteArray)
    byteArray
  }

  /** Aggregate multiple serialized ReceivedBlockTrackerLogEvents in a single ByteBuffer. */
  def aggregate(records: Seq[Record]): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize[Array[Array[Byte]]](
      records.map(record => getByteArray(record.data)).toArray))
  }

  /**
   * De-aggregate serialized ReceivedBlockTrackerLogEvents in a single ByteBuffer.
   * A stream may not have used batching initially, but started using it after a restart. This
   * method therefore needs to be backwards compatible.
   */
  def deaggregate(buffer: ByteBuffer): Array[ByteBuffer] = {
    try {
      Utils.deserialize[Array[Array[Byte]]](getByteArray(buffer)).map(ByteBuffer.wrap)
    } catch {
      case _: ClassCastException => // users may restart a stream with batching enabled
        Array(buffer)
    }
  }
}
