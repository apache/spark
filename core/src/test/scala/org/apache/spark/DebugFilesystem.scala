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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.hadoop.fs._

object DebugFilesystem {
  // Stores the set of active streams and their creation sites.
  val openStreams = new ConcurrentHashMap[FSDataInputStream, Throwable]()
}

/**
 * DebugFilesystem wraps file open calls to track all open connections. This can be used in tests
 * to check that connections are not leaked.
 */
class DebugFilesystem extends LocalFileSystem {
  import DebugFilesystem._

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val wrapped: FSDataInputStream = super.open(f, bufferSize)
    openStreams.put(wrapped, new Throwable())

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
        wrapped.close()
        openStreams.remove(wrapped)
      }

      override def read(): Int = wrapped.read()

      override def reset(): Unit = wrapped.reset()

      override def toString: String = wrapped.toString

      override def equals(obj: scala.Any): Boolean = wrapped.equals(obj)

      override def hashCode(): Int = wrapped.hashCode()
    }
  }
}
