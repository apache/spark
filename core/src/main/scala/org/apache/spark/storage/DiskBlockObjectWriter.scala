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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException, OutputStream}
import java.nio.channels.{ClosedByInterruptException, FileChannel}
import java.nio.file.Files
import java.util.zip.Checksum

import org.apache.spark.SparkException
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{ERROR, PATH}
import org.apache.spark.io.MutableCheckedOutputStream
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.PairsWriter

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class DiskBlockObjectWriter(
    val file: File,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetricsReporter,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging
  with PairsWriter {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      this.flush()
    }

    def manualClose(): Unit = {
      try {
        super.close()
      } catch {
        // The output stream may have been closed when the task thread is interrupted, then we
        // get IOException when flushing the buffered data. We should catch and log the exception
        // to ensure the revertPartialWritesAndClose() function doesn't throw an exception.
        case e: IOException =>
          logError(log"Exception occurred while manually close the output stream to file "
            + log"${MDC(PATH, file)}, ${MDC(ERROR, e.getMessage)}")
      }
    }
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  // checksum related
  private var checksumEnabled = false
  private var checksumOutputStream: MutableCheckedOutputStream = _
  private var checksum: Checksum = _

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|-----|
   *           ^          ^     ^
   *           |          |    channel.position()
   *           |        reportedPosition
   *         committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
   */
  private var committedPosition = file.length()
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   * And we reset it after every commitAndGet called.
   */
  private var numRecordsWritten = 0

  /**
   * Keep track the number of written records committed.
   */
  private var numRecordsCommitted = 0L

  /**
   * Set the checksum that the checksumOutputStream should use
   */
  def setChecksum(checksum: Checksum): Unit = {
    if (checksumOutputStream == null) {
      this.checksumEnabled = true
      this.checksum = checksum
    } else {
      checksumOutputStream.setChecksum(checksum)
    }
  }

  private def initialize(): Unit = {
    fos = new FileOutputStream(file, true)
    channel = fos.getChannel()
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    if (checksumEnabled) {
      assert(this.checksum != null, "Checksum is not set")
      checksumOutputStream = new MutableCheckedOutputStream(ts)
      checksumOutputStream.setChecksum(checksum)
    }
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(if (checksumEnabled) checksumOutputStream else ts, bufferSize)
        with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw SparkException.internalError(
        "Writer already closed. Cannot be reopened.", category = "STORAGE")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        channel = null
        mcs = null
        bs = null
        fos = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      val pos = channel.position()
      val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsCommitted += numRecordsWritten
      numRecordsWritten = 0
      fileSegment
    } else {
      new FileSegment(file, committedPosition, 0)
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      var truncateStream: FileOutputStream = null
      try {
        truncateStream = new FileOutputStream(file, true)
        truncateStream.getChannel.truncate(committedPosition)
      } catch {
        // ClosedByInterruptException is an excepted exception when kill task,
        // don't log the exception stack trace to avoid confusing users.
        // See: SPARK-28340
        case ce: ClosedByInterruptException =>
          logError(log"Exception occurred while reverting partial writes to file "
            + log"${MDC(PATH, file)}, ${MDC(ERROR, ce.getMessage)}")
        case e: Exception =>
          logError(
            log"Uncaught exception while reverting partial writes to file ${MDC(PATH, file)}", e)
      } finally {
        if (truncateStream != null) {
          truncateStream.close()
          truncateStream = null
        }
      }
    }
    file
  }

  /**
   * Reverts write metrics and delete the file held by current `DiskBlockObjectWriter`.
   * Callers should invoke this function when there are runtime exceptions in file
   * writing process and the file is no longer needed.
   */
  def closeAndDelete(): Unit = {
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition)
        writeMetrics.decRecordsWritten(numRecordsCommitted + numRecordsWritten)
        closeResources()
      }
    } {
      if (!Files.deleteIfExists(file.toPath)) {
        logWarning(s"Error deleting $file")
      }
    }
  }

  /**
   * Writes a key-value pair.
   */
  override def write(key: Any, value: Any): Unit = {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw SparkCoreErrors.unsupportedOperationError()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten(): Unit = {
    val pos = channel.position()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  override def flush(): Unit = {
    objOut.flush()
    bs.flush()
  }
}
