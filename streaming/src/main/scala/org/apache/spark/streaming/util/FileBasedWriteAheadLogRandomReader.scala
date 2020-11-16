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

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

/**
 * A random access reader for reading write ahead log files written using
 * [[org.apache.spark.streaming.util.FileBasedWriteAheadLogWriter]]. Given the file segment info,
 * this reads the record (ByteBuffer) from the log file.
 */
private[streaming] class FileBasedWriteAheadLogRandomReader(path: String, conf: Configuration)
  extends Closeable {

  private val instream = HdfsUtils.getInputStream(path, conf)
  private var closed = (instream == null) // the file may be deleted as we're opening the stream

  def read(segment: FileBasedWriteAheadLogSegment): ByteBuffer = synchronized {
    assertOpen()
    instream.seek(segment.offset)
    val nextLength = instream.readInt()
    HdfsUtils.checkState(nextLength == segment.length,
      s"Expected message length to be ${segment.length}, but was $nextLength")
    val buffer = new Array[Byte](nextLength)
    instream.readFully(buffer)
    ByteBuffer.wrap(buffer)
  }

  override def close(): Unit = synchronized {
    closed = true
    instream.close()
  }

  private def assertOpen(): Unit = {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Reader to read from the file.")
  }
}
