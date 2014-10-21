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
package org.apache.spark.streaming.storage

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

private[streaming] class WriteAheadLogRandomReader(path: String, conf: Configuration)
  extends Closeable {

  private val instream = HdfsUtils.getInputStream(path, conf)
  private var closed = false

  def read(segment: FileSegment): ByteBuffer = synchronized {
    assertOpen()
    instream.seek(segment.offset)
    val nextLength = instream.readInt()
    HdfsUtils.checkState(nextLength == segment.length,
      "Expected message length to be " + segment.length + ", " + "but was " + nextLength)
    val buffer = new Array[Byte](nextLength)
    instream.readFully(buffer)
    ByteBuffer.wrap(buffer)
  }

  override def close(): Unit = synchronized {
    closed = true
    instream.close()
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Reader to read from the file.")
  }
}

