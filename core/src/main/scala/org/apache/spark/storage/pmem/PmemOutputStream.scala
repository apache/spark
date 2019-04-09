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

package org.apache.spark.storage.pmem

import java.io.OutputStream

import org.apache.spark.internal.Logging
import org.apache.spark.storage.pmem.PmemBuffer

class PmemOutputStream(
  persistentMemoryWriter: PersistentMemoryHandler,
  numPartitions: Int,
  blockId: String,
  numMaps: Int
  ) extends OutputStream with Logging {
  val buf = new PmemBuffer()
  var set_clean = true
  var is_closed = false
  logDebug(blockId)

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    buf.put(bytes, off, len)
  }

  override def write(byte: Int): Unit = {
    var bytes: Array[Byte] = Array(byte.toByte)
    buf.put(bytes, 0, 1)
  }

  override def flush(): Unit = {
    persistentMemoryWriter.setPartition(numPartitions, blockId, buf, set_clean, numMaps)
    if (set_clean == true) {
      set_clean = false
    }
  }

  def size(): Int = {
    buf.size()
  }

  def reset(): Unit = {
    buf.clean()
  }

  override def close(): Unit = {
    if (!is_closed) {
      buf.close()
      is_closed = true
    }
  }
}
