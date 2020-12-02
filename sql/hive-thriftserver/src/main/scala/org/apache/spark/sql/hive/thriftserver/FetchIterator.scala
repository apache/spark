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

package org.apache.spark.sql.hive.thriftserver

private[hive] sealed trait FetchIterator[A] extends Iterator[A] {
  def fetchNext(): Unit

  def setRelativePosition(diff: Long): Unit

  def setAbsolutePosition(pos: Long): Unit

  def getPosition: Long
}

private[hive] class ArrayFetchIterator[A](src: Array[A]) extends FetchIterator[A] {
  private var fetchStart: Long = 0

  private var position: Long = 0

  override def fetchNext(): Unit = fetchStart = position

  override def setRelativePosition(diff: Long): Unit = {
    setAbsolutePosition(fetchStart + diff)
  }

  override def setAbsolutePosition(pos: Long): Unit = {
    position = (pos max 0) min src.length
    fetchStart = position
  }

  override def getPosition: Long = position

  override def hasNext: Boolean = position < src.length

  override def next(): A = {
    position += 1
    src(position.toInt - 1)
  }
}

private[hive] class IterableFetchIterator[A](iterable: Iterable[A]) extends FetchIterator[A] {
  private var iter: Iterator[A] = iterable.iterator

  private var fetchStart: Long = 0

  private var position: Long = 0

  override def fetchNext(): Unit = fetchStart = position

  override def setRelativePosition(diff: Long): Unit = {
    setAbsolutePosition(fetchStart + diff)
  }

  override def setAbsolutePosition(pos: Long): Unit = {
    val newPos = pos max 0
    if (newPos < position) resetPosition()
    while (position < newPos && hasNext) next()
    fetchStart = position
  }

  override def getPosition: Long = position

  override def hasNext: Boolean = iter.hasNext

  override def next(): A = {
    position += 1
    iter.next()
  }

  private def resetPosition(): Unit = {
    if (position != 0) {
      iter = iterable.iterator
      position = 0
      fetchStart = 0
    }
  }
}
