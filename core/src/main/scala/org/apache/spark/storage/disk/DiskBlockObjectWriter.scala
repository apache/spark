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

package org.apache.spark.storage.disk

import java.io.{File, OutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class DiskBlockObjectWriter(
    diskBlockWriter: DiskBlockWriter,
    serializerInstance: SerializerInstance,
    val blockId: BlockId = null)
  extends Logging {

  private var bs: OutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var hasBeenClosed = false
  private var commitAndCloseHasBeenCalled = false

  def file: File = diskBlockWriter.file

  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    bs = diskBlockWriter.open()
    objOut = serializerInstance.serializeStream(bs)
    initialized = true
    this
  }

  def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        if (diskBlockWriter.syncWrites) {
          objOut.flush()
        }
      } {
        objOut.close()
      }
      bs = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  def commitAndClose(): Unit = {
    if (initialized) {
      objOut.flush()
    }
    diskBlockWriter.commitAndClose()
    commitAndCloseHasBeenCalled = true
  }


  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    if (initialized) {
      objOut.flush()
    }
    diskBlockWriter.revertPartialWritesAndClose()
  }

  /**
   * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    diskBlockWriter.recordWritten()
  }

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   */
  def fileSegment(): FileSegment = diskBlockWriter.fileSegment()

  // For testing
  private[spark] def flush() {
    objOut.flush()
    bs.flush()
  }
}
