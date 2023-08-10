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

package org.apache.spark.sql.connect.client

private[sql] trait CloseableIterator[E] extends Iterator[E] with AutoCloseable { self =>
  def asJava: java.util.Iterator[E] = new java.util.Iterator[E] with AutoCloseable {
    override def next() = self.next()

    override def hasNext() = self.hasNext

    override def close() = self.close()
  }
}

private[sql] object CloseableIterator {

  /**
   * Wrap iterator to get CloseeableIterator, if it wasn't closeable already.
   */
  def apply[T](iterator: Iterator[T]): CloseableIterator[T] = iterator match {
    case closeable: CloseableIterator[T] => closeable
    case _ =>
      new CloseableIterator[T] {
        override def next(): T = iterator.next()

        override def hasNext(): Boolean = iterator.hasNext

        override def close() = { /* empty */ }
      }
  }
}
