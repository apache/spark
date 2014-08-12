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

import io.netty.buffer.{ByteBufInputStream, ByteBuf}


/**
 * A buffer abstraction based on Netty's ByteBuf.
 *
 * The buffer's life cycle is NOT managed by the JVM, and thus requiring explicit declaration of
 * reference by the retain method and release method.
 */
class ReferenceCountedBuffer(underlying: ByteBuf) {

  def byteBuffer(): ByteBuffer = underlying.nioBuffer

  def inputStream(): InputStream = new ByteBufInputStream(underlying)

  def retain(): Unit = underlying.retain()

  def release(): Unit = underlying.release()
}
