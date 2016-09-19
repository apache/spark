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

package org.apache.spark.storage

import java.io.InputStream

import com.google.common.base.Objects

import org.apache.spark.network.buffer.{ChunkedByteBuffer, ManagedBuffer, NioManagedBuffer}

private[storage] class ReleasableManagedBuffer(
  var managedBuffer: ManagedBuffer, val onDeallocate: () => Unit) extends ManagedBuffer {
  def this(chunkedBuffer: ChunkedByteBuffer, onDeallocate: () => Unit) {
    this(new NioManagedBuffer(chunkedBuffer), onDeallocate)
  }

  def size: Long = {
    managedBuffer.size()
  }

  def nioByteBuffer: ChunkedByteBuffer = {
    managedBuffer.nioByteBuffer()
  }

  def createInputStream: InputStream = {
    managedBuffer.createInputStream()
  }

  def convertToNetty: AnyRef = {
    managedBuffer.convertToNetty()
  }

  override def retain: ManagedBuffer = {
    super.retain
    managedBuffer.retain()
    this
  }

  override def release: Boolean = {
    super.release()
    managedBuffer.release()
  }

  override def deallocate(): Unit = {
    super.deallocate()
    onDeallocate()
  }

  override def toString: String = {
    Objects.toStringHelper(this).add("managedBuffer", managedBuffer).toString
  }
}
