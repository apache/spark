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

import java.io.InputStream

import scala.util.control.Breaks._

import org.apache.spark.internal.Logging
import org.apache.spark.storage.pmem.PmemBuffer

class PmemInputStream(
  persistentMemoryHandler: PersistentMemoryHandler,
  blockId: String
  ) extends InputStream with Logging {
  val buf = new PmemBuffer()
  var index: Int = 0
  var remaining: Int = 0
  var available_bytes: Int = persistentMemoryHandler.getPartitionSize(blockId).toInt
  val blockInfo: Array[(Long, Int)] = persistentMemoryHandler.getPartitionBlockInfo(blockId)

  def loadNextStream(): Int = {
    if (index >= blockInfo.length) {
      return 0
    }
    val data_length = blockInfo(index)._2
    val data_addr = blockInfo(index)._1

    buf.load(data_addr, data_length)

    index += 1
    remaining += data_length
    data_length
  }

  override def read(): Int = {
    if (remaining == 0) {
      if (loadNextStream() == 0) {
        return -1
      }
    }
    remaining -= 1
    available_bytes -= 1
    buf.get()
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    load(len)
    if (remaining == 0) {
      return -1
    }

    val real_len = Math.min(len, remaining)
    buf.get(bytes, off, real_len)
    remaining -= real_len
    available_bytes -= real_len
    real_len
  }

  def load(len: Int): Unit = {
    breakable { while ((remaining > 0 && remaining < len) || remaining == 0) {
      if (loadNextStream() == 0) {
        break
      }
    } }
  }

  def getByteBufferDirectAddr(): Long = {
    buf.getDirectAddr()
  }

  override def available(): Int = {
    available_bytes
  }

  override def close(): Unit = {
    buf.close()
  }

  def deleteBlock(): Unit = {
    // FIXME: DELETE PMEM PARTITON HERE
    persistentMemoryHandler.deletePartition(blockId)
  }
}
