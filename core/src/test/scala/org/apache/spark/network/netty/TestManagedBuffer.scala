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

package org.apache.spark.network.netty

import java.io.InputStream
import java.nio.ByteBuffer

import io.netty.buffer.Unpooled

import org.apache.spark.network.{NettyManagedBuffer, ManagedBuffer}


/**
 * A ManagedBuffer implementation that contains 0, 1, 2, 3, ..., (len-1).
 *
 * Used for testing.
 */
class TestManagedBuffer(len: Int) extends ManagedBuffer {

  require(len <= Byte.MaxValue)

  private val byteArray: Array[Byte] = Array.tabulate[Byte](len)(_.toByte)

  private val underlying = new NettyManagedBuffer(Unpooled.wrappedBuffer(byteArray))

  override def size: Long = underlying.size

  override private[network] def convertToNetty(): AnyRef = underlying.convertToNetty()

  override def nioByteBuffer(): ByteBuffer = underlying.nioByteBuffer()

  override def inputStream(): InputStream = underlying.inputStream()

  override def toString: String = s"${getClass.getName}($len)"

  override def equals(other: Any): Boolean = other match {
    case otherBuf: ManagedBuffer =>
      val nioBuf = otherBuf.nioByteBuffer()
      if (nioBuf.remaining() != len) {
        return false
      } else {
        var i = 0
        while (i < len) {
          if (nioBuf.get() != i) {
            return false
          }
          i += 1
        }
        return true
      }
    case _ => false
  }

  override def retain(): this.type = this

  override def release(): this.type = this
}
