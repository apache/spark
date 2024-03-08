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
package org.apache.spark.internal.io.cloud

import java.nio.file.FileAlreadyExistsException
import java.util.EnumSet

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.AbstractFileContextBasedCheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream

class AbortableStreamBasedCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends AbstractFileContextBasedCheckpointFileManager(path, hadoopConf) with Logging {

  if (!fc.hasPathCapability(path, CommonPathCapabilities.ABORTABLE_STREAM)) {
    throw new UnsupportedFileSystemException("AbortableStreamBasedCheckpointFileManager requires" +
      s" an fs (path: $path) with abortable stream support")
  }

  logInfo(s"Writing atomically to $path based on abortable stream")

  class AbortableStreamBasedFSDataOutputStream(
      fsDataOutputStream: FSDataOutputStream,
      fc: FileContext,
      path: Path,
      overwriteIfPossible: Boolean) extends CancellableFSDataOutputStream(fsDataOutputStream) {

    @volatile private var terminated = false

    override def cancel(): Unit = synchronized {
      if (terminated) return
      try {
        fsDataOutputStream.abort()
        fsDataOutputStream.close()
      } catch {
          case NonFatal(e) =>
            logWarning(s"Error cancelling write to $path (stream: $fsDataOutputStream)", e)
      } finally {
        terminated = true
      }
    }

    override def close(): Unit = synchronized {
      if (terminated) return
      try {
        if (!overwriteIfPossible && fc.util().exists(path)) {
          fsDataOutputStream.abort()
          throw new FileAlreadyExistsException(
            s"Failed to close atomic stream $path (stream: " +
            s"$fsDataOutputStream) as destination already exists")
        }
        fsDataOutputStream.close()
      } catch {
          case NonFatal(e) =>
            logWarning(s"Error closing $path (stream: $fsDataOutputStream)", e)
      } finally {
        terminated = true
      }
    }

    override def toString(): String = {
      fsDataOutputStream.toString
    }
  }

  override def createAtomic(
      path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    import CreateFlag._
    val createFlag = if (overwriteIfPossible) {
      EnumSet.of(CREATE, OVERWRITE)
    } else {
      EnumSet.of(CREATE)
    }
    new AbortableStreamBasedFSDataOutputStream(
      fc.create(path, createFlag), fc, path, overwriteIfPossible)
  }
}
