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

/**
 * A simple iterator that lazily initializes the underlying iterator.
 *
 * The use case is that sometimes we might have many iterators open at the same time, and each of
 * the iterator might initialize its own buffer (e.g. decompression buffer, deserialization buffer).
 * This could lead to too many buffers open. If this iterator is used, we lazily initialize those
 * buffers.
 */
private[spark]
class LazyInitIterator(createIterator: => Iterator[Any]) extends Iterator[Any] {

  lazy val proxy = createIterator

  override def hasNext: Boolean = {
    val gotNext = proxy.hasNext
    if (!gotNext) {
      close()
    }
    gotNext
  }

  override def next(): Any = proxy.next()

  def close(): Unit = Unit
}
