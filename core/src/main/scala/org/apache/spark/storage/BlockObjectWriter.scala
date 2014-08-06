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

import java.io.{BufferedOutputStream, FileOutputStream, File, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.executor.ShuffleWriteMetrics

/**
 * An interface for writing JVM objects to some underlying storage. This interface allows
 * appending data to an existing block, and can guarantee atomicity in the case of faults
 * as it allows the caller to revert partial writes.
 *
 * This interface does not support concurrent writes.
 */
private[spark] abstract class BlockObjectWriter(val blockId: BlockId) {

  def open(): BlockObjectWriter

  def close()

  def isOpen: Boolean

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  def commitAndClose(): Unit

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   */
  def revertPartialWritesAndClose()

  /**
   * Writes an object.
   */
  def write(value: Any)

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   */
  def fileSegment(): FileSegment
}

/**
 * BlockObjectWriter which writes directly to a file on disk. Appends to the given file.
 * The given write metrics will be updated incrementally, but will not necessarily be current until
 * commitAndClose is called.
 */
private[spark] class DiskBlockObjectWriter(
    blockId: BlockId,
    file: File,
    serializer: Serializer,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    writeMetrics: ShuffleWriteMetrics)
  extends BlockObjectWriter(blockId)
  with Logging
{
  /** Intercepts write calls and tracks total time spent writing. Not thread safe. */
  private class TimeTrackingOutputStream(out: OutputStream) extends OutputStream {
    def write(i: Int): Unit = callWithTiming(out.write(i))
    override def write(b: Array[Byte]) = callWithTiming(out.write(b))
    override def write(b: Array[Byte], off: Int, len: Int) = callWithTiming(out.write(b, off, len))
    override def close() = out.close()
    override def flush() = out.flush()
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private val initialPosition = file.length()
  private var finalPosition: Long = -1
  private var initialized = false

  /** Calling channel.position() to update the write metrics can be a little bit expensive, so we
    * only call it every N writes */
  private var writesSinceMetricsUpdate = 0
  private var lastPosition = initialPosition

  override def open(): BlockObjectWriter = {
    fos = new FileOutputStream(file, true)
    ts = new TimeTrackingOutputStream(fos)
    channel = fos.getChannel()
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializer.newInstance().serializeStream(bs)
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        objOut.flush()
        def sync = fos.getFD.sync()
        callWithTiming(sync)
      }
      objOut.close()

      channel = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
    }
  }

  override def isOpen: Boolean = objOut != null

  override def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      updateBytesWritten()
      close()
    }
    finalPosition = file.length()
  }

  // Discard current writes. We do this by flushing the outstanding writes and then
  // truncating the file to its initial position.
  override def revertPartialWritesAndClose() {
    try {
      writeMetrics.shuffleBytesWritten -= (lastPosition - initialPosition)

      if (initialized) {
        objOut.flush()
        bs.flush()
        close()
      }

      val truncateStream = new FileOutputStream(file, true)
      try {
        truncateStream.getChannel.truncate(initialPosition)
      } finally {
        truncateStream.close()
      }
    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes to file " + file, e)
    }
  }

  override def write(value: Any) {
    if (!initialized) {
      open()
    }

    objOut.writeObject(value)

    if (writesSinceMetricsUpdate == 32) {
      writesSinceMetricsUpdate = 0
      updateBytesWritten()
    } else {
      writesSinceMetricsUpdate += 1
    }
  }

  override def fileSegment(): FileSegment = {
    new FileSegment(file, initialPosition, finalPosition - initialPosition)
  }

  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.shuffleBytesWritten += (pos - lastPosition)
    lastPosition = pos
  }

  private def callWithTiming(f: => Unit) = {
    val start = System.nanoTime()
    f
    writeMetrics.shuffleWriteTime += (System.nanoTime() - start)
  }

  // For testing
  private[spark] def flush() {
    objOut.flush()
    bs.flush()
  }
}
