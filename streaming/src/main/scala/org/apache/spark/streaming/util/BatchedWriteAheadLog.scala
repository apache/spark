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
import org.apache.spark.streaming.scheduler.CombinedReceivedBlockTrackerLogEvent
import org.apache.spark.util.{Utils, ThreadUtils}

/**
 * Wrapper class for representing the records that we will write to the WriteAheadLog. Coupled with
 * the timestamp for the write request of the record, and the promise that will block the write
 * request, while a separate thread is actually performing the write.
 */
private[util] case class RecordBuffer(
    record: ByteBuffer,
    time: Long,
    promise: Promise[WriteAheadLogRecordHandle])

/**
 * A wrapper for a WriteAheadLog that batches records before writing data. All other methods will
 * be passed on to the wrapped class.
 */
private[streaming] class BatchedWriteAheadLog(parent: WriteAheadLog)
  extends WriteAheadLog with Logging {

  /** A thread pool for fulfilling log write promises */
  private val batchWriterThreadPool = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("wal-batch-writer-thead-pool"))

  // exposed for tests
  protected val walWriteQueue = new LinkedBlockingQueue[RecordBuffer]()

  private val WAL_WRITE_STATUS_TIMEOUT = 5000 // 5 seconds

  private val writeAheadLogBatchWriter: BatchedLogWriter = startBatchedWriterThread()

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
   * Read a segment from an existing Write Ahead Log.
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
    parent.readAll()
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
   * Stop the manager, close any open log writer.
   *
   * This method is handled by the parent WriteAheadLog.
   */
  override def close(): Unit = {
    writeAheadLogBatchWriter.stop()
    batchWriterThreadPool.shutdownNow()
    parent.close()
  }

  /** Start the actual log writer on a separate thread. */
  private def startBatchedWriterThread(): BatchedLogWriter = {
    val writer = new BatchedLogWriter()
    val thread = new Thread(writer, "Batched WAL Writer")
    thread.setDaemon(true)
    thread.start()
    writer
  }

  /** A helper class that writes LogEvents in a separate thread to allow for batching. */
  private[util] class BatchedLogWriter extends Runnable {

    private var active: Boolean = true
    private val buffer = new ArrayBuffer[RecordBuffer]()

    override def run(): Unit = {
      while (active) {
        try {
          flushRecords()
        } catch {
          case NonFatal(e) =>
            logError("Exception while flushing records in Batch Write Ahead Log writer.", e)
        }
      }
      logInfo("Batch Write Ahead Log writer shutting down.")
    }

    def stop(): Unit = {
      logInfo("Stopping Batch Write Ahead Log writer.")
      active = false
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
          // we take the latest record for the time to ensure that we don't clean up files earlier
          // than the expiration date of the records
          val time = buffer.last.time
          segment = parent.write(ByteBuffer.wrap(Utils.serialize(
            CombinedReceivedBlockTrackerLogEvent(buffer.map(_.record.array()).toArray))), time)
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
}
