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

package org.apache.spark.network.netty.client

import java.io.InputStream
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, ByteBufInputStream}


/**
 * A buffer abstraction based on Netty's ByteBuf so we don't expose Netty.
 * This is a Scala value class.
 *
 * The buffer's life cycle is NOT managed by the JVM, and thus requiring explicit declaration of
 * reference by the retain method and release method.
 */
private[spark]
class ReferenceCountedBuffer(val underlying: ByteBuf) extends AnyVal {

  /** Return the nio ByteBuffer view of the underlying buffer. */
  def byteBuffer(): ByteBuffer = underlying.nioBuffer

  /** Creates a new input stream that starts from the current position of the buffer. */
  def inputStream(): InputStream = new ByteBufInputStream(underlying)

  /** Increment the reference counter by one. */
  def retain(): Unit = underlying.retain()

  /** Decrement the reference counter by one and release the buffer if the ref count is 0. */
  def release(): Unit = underlying.release()
}
