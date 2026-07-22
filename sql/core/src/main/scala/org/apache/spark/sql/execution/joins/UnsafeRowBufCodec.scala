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

package org.apache.spark.sql.execution.joins

import io.netty.buffer.ByteBuf

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform

object UnsafeRowBufCodec {

  def makeAdvanceRead(ur: UnsafeRow, buf: ByteBuf): Int => UnsafeRow =
    if (buf.hasArray) { (len: Int) =>
      val idx = buf.readerIndex()
      ur.pointTo(buf.array(), Platform.BYTE_ARRAY_OFFSET + buf.arrayOffset() + idx, len)
      buf.readerIndex(idx + len)
      ur
    } else if (buf.hasMemoryAddress) { (len: Int) =>
      val idx = buf.readerIndex()
      ur.pointTo(null, buf.memoryAddress() + idx, len)
      buf.readerIndex(idx + len)
      ur
    } else { (len: Int) =>
      // fallback to bad perf case
      val arr = new Array[Byte](len)
      buf.readBytes(arr)
      ur.pointTo(arr, Platform.BYTE_ARRAY_OFFSET, len)
      ur
    }

  def makeAdvanceWrite(buf: ByteBuf): UnsafeRow => Unit =
    if (buf.hasArray) { (ur: UnsafeRow) =>
      buf.ensureWritable(ur.getSizeInBytes)
      val idx = buf.writerIndex()
      val dstOffset = Platform.BYTE_ARRAY_OFFSET + buf.arrayOffset() + idx
      Platform.copyMemory(
        ur.getBaseObject,
        ur.getBaseOffset,
        buf.array(),
        dstOffset,
        ur.getSizeInBytes)
      buf.writerIndex(idx + ur.getSizeInBytes)
    } else if (buf.hasMemoryAddress) { (ur: UnsafeRow) =>
      buf.ensureWritable(ur.getSizeInBytes)
      val idx = buf.writerIndex()
      Platform.copyMemory(
        ur.getBaseObject,
        ur.getBaseOffset,
        null,
        buf.memoryAddress() + idx,
        ur.getSizeInBytes)
      buf.writerIndex(idx + ur.getSizeInBytes)
    } else { (ur: UnsafeRow) =>
      // fallback to bad perf case
      buf.writeBytes(ur.getBytes)
    }
}
