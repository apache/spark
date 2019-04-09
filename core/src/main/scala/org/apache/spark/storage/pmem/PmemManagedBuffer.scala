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

import io.netty.buffer.{ByteBuf, Unpooled}
import java.io.{InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.pmem.{PersistentMemoryPool, PmemInputStream}

class PmemManagedBuffer(pmHandler: PersistentMemoryHandler, blockId: String)
  extends ManagedBuffer with Logging {
  var inputStream: InputStream = _
  var total_size: Long = -1
  var byteBuffer: ByteBuffer = _
  private val refCount = new AtomicInteger(1)

  override def size(): Long = {
    if (total_size == -1) {
      total_size = pmHandler.getPartitionSize(blockId)
    }
    total_size
  }

  override def nioByteBuffer(): ByteBuffer = {
    // TODO: This function should be Deprecated by spark in near future.
    byteBuffer
  }

  override def createInputStream(): InputStream = {
    if (inputStream == null) {
      inputStream = new PmemInputStream(pmHandler, blockId)
    }
    inputStream
  }

  override def retain(): ManagedBuffer = {
    refCount.incrementAndGet()
    this
  }

  override def release(): ManagedBuffer = {
    if (refCount.decrementAndGet() == 0) {
      if (inputStream != null) {
        inputStream.close()
      }
    }
    this
  }

  override def convertToNetty(): Object = {
    val data_length = size().toInt
    val in = createInputStream()
    in.asInstanceOf[PmemInputStream].load(data_length)
    Unpooled.wrappedBuffer(in.asInstanceOf[PmemInputStream].getByteBufferDirectAddr(),
                           data_length, false)
  }
}
