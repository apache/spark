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

package org.apache.spark.shuffle

import java.io.{Closeable, IOException, OutputStream}
import java.util.zip.Checksum

import org.apache.spark.io.MutableCheckedOutputStream
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.storage.{BlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.PairsWriter

/**
 * A key-value writer inspired by {@link DiskBlockObjectWriter} that pushes the bytes to an
 * arbitrary partition writer instead of writing to local disk through the block manager.
 */
private[spark] class ShufflePartitionPairsWriter(
    partitionWriter: ShufflePartitionWriter,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    blockId: BlockId,
    writeMetrics: ShuffleWriteMetricsReporter,
    checksum: Checksum)
  extends PairsWriter with Closeable {

  private var isClosed = false
  private var partitionStream: OutputStream = _
  private var timeTrackingStream: OutputStream = _
  private var wrappedStream: OutputStream = _
  private var objOut: SerializationStream = _
  private var numRecordsWritten = 0
  private var curNumBytesWritten = 0L
  // this would be only initialized when checksum != null,
  // which indicates shuffle checksum is enabled.
  private var checksumOutputStream: MutableCheckedOutputStream = _

  override def write(key: Any, value: Any): Unit = {
    if (isClosed) {
      throw new IOException("Partition pairs writer is already closed.")
    }
    if (objOut == null) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  private def open(): Unit = {
    try {
      partitionStream = partitionWriter.openStream
      timeTrackingStream = new TimeTrackingOutputStream(writeMetrics, partitionStream)
      if (checksum != null) {
        checksumOutputStream = new MutableCheckedOutputStream(timeTrackingStream)
        checksumOutputStream.setChecksum(checksum)
      }
      wrappedStream = serializerManager.wrapStream(blockId,
        if (checksumOutputStream != null) checksumOutputStream else timeTrackingStream)
      objOut = serializerInstance.serializeStream(wrappedStream)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          close()
        }
        throw e
    }
  }

  override def close(): Unit = {
    if (!isClosed) {
      Utils.tryWithSafeFinally {
        Utils.tryWithSafeFinally {
          objOut = closeIfNonNull(objOut)
          // Setting these to null will prevent the underlying streams from being closed twice
          // just in case any stream's close() implementation is not idempotent.
          wrappedStream = null
          timeTrackingStream = null
          partitionStream = null
        } {
          // Normally closing objOut would close the inner streams as well, but just in case there
          // was an error in initialization etc. we make sure we clean the other streams up too.
          Utils.tryWithSafeFinally {
            wrappedStream = closeIfNonNull(wrappedStream)
            // Same as above - if wrappedStream closes then assume it closes underlying
            // partitionStream and don't close again in the finally
            timeTrackingStream = null
            partitionStream = null
          } {
            Utils.tryWithSafeFinally {
              timeTrackingStream = closeIfNonNull(timeTrackingStream)
              partitionStream = null
            } {
              partitionStream = closeIfNonNull(partitionStream)
            }
          }
        }
        updateBytesWritten()
      } {
        isClosed = true
      }
    }
  }

  private def closeIfNonNull[T <: Closeable](closeable: T): T = {
    if (closeable != null) {
      closeable.close()
    }
    null.asInstanceOf[T]
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  private def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  private def updateBytesWritten(): Unit = {
    val numBytesWritten = partitionWriter.getNumBytesWritten
    val bytesWrittenDiff = numBytesWritten - curNumBytesWritten
    writeMetrics.incBytesWritten(bytesWrittenDiff)
    curNumBytesWritten = numBytesWritten
  }
}
