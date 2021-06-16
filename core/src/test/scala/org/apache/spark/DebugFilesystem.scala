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

package org.apache.spark

import java.io.{FileDescriptor, InputStream}
import java.lang
import java.nio.ByteBuffer

import scala.collection.mutable

import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging

object DebugFilesystem extends Logging {
  // Stores the set of active streams and their creation sites.
  private val openStreams = mutable.Map.empty[FSDataInputStream, Throwable]

  def addOpenStream(stream: FSDataInputStream): Unit = openStreams.synchronized {
    openStreams.put(stream, new Throwable())
  }

  def clearOpenStreams(): Unit = openStreams.synchronized {
    openStreams.clear()
  }

  def removeOpenStream(stream: FSDataInputStream): Unit = openStreams.synchronized {
    openStreams.remove(stream)
  }

  def assertNoOpenStreams(): Unit = openStreams.synchronized {
    val numOpen = openStreams.values.size
    if (numOpen > 0) {
      for (exc <- openStreams.values) {
        logWarning("Leaked filesystem connection created at:")
        exc.printStackTrace()
      }
      throw new IllegalStateException(s"There are $numOpen possibly leaked file streams.",
        openStreams.values.head)
    }
  }
}

/**
 * DebugFilesystem wraps
 *     1) file open calls to track all open connections. This can be used in tests to check that
 *        connections are not leaked;
 *     2) rename calls to return false when destination's parent path does not exist. When
 *        destination parent does not exist, LocalFileSystem uses FileUtil#copy to copy the
 *        file and returns true if succeed, while many other hadoop file systems (e.g. HDFS, S3A)
 *        return false without renaming any file. This helps to test that Spark can work with the
 *        latter file systems.
 */
// TODO(ekl) we should consider always interposing this to expose num open conns as a metric
class DebugFilesystem extends LocalFileSystem {
  import DebugFilesystem._

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val wrapped: FSDataInputStream = super.open(f, bufferSize)
    addOpenStream(wrapped)
    new FSDataInputStream(wrapped.getWrappedStream) {
      override def setDropBehind(dropBehind: lang.Boolean): Unit = wrapped.setDropBehind(dropBehind)

      override def getWrappedStream: InputStream = wrapped.getWrappedStream

      override def getFileDescriptor: FileDescriptor = wrapped.getFileDescriptor

      override def getPos: Long = wrapped.getPos

      override def seekToNewSource(targetPos: Long): Boolean = wrapped.seekToNewSource(targetPos)

      override def seek(desired: Long): Unit = wrapped.seek(desired)

      override def setReadahead(readahead: lang.Long): Unit = wrapped.setReadahead(readahead)

      override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int =
        wrapped.read(position, buffer, offset, length)

      override def read(buf: ByteBuffer): Int = wrapped.read(buf)

      override def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit =
        wrapped.readFully(position, buffer, offset, length)

      override def readFully(position: Long, buffer: Array[Byte]): Unit =
        wrapped.readFully(position, buffer)

      override def available(): Int = wrapped.available()

      override def mark(readlimit: Int): Unit = wrapped.mark(readlimit)

      override def skip(n: Long): Long = wrapped.skip(n)

      override def markSupported(): Boolean = wrapped.markSupported()

      override def close(): Unit = {
        try {
          wrapped.close()
        } finally {
          removeOpenStream(wrapped)
        }
      }

      override def read(): Int = wrapped.read()

      override def reset(): Unit = wrapped.reset()

      override def toString: String = wrapped.toString

      override def equals(obj: scala.Any): Boolean = wrapped.equals(obj)

      override def hashCode(): Int = wrapped.hashCode()
    }
  }

  override def rename(src: Path, dst: Path): Boolean = {
    exists(dst.getParent) && super.rename(src, dst)
  }
}
