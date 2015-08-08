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

import java.util.NoSuchElementException

import org.apache.spark.sql.catalyst.InternalRow

private[sql] abstract class RowIterator {
  def advanceNext(): Boolean
  def getRow: InternalRow
  def toScala: Iterator[InternalRow] = new RowIteratorToScala(this)
}

object RowIterator {
  def fromScala(scalaIter: Iterator[InternalRow]): RowIterator = {
    scalaIter match {
      case wrappedRowIter: RowIteratorToScala if !wrappedRowIter._wasUsed => wrappedRowIter.rowIter
      case _ => new RowIteratorFromScala(scalaIter)
    }
  }
}

private final class RowIteratorToScala(val rowIter: RowIterator) extends Iterator[InternalRow] {
  var _wasUsed: Boolean = false
  private [this] var _hasNext: Boolean = false
  override def hasNext: Boolean = {
    if (!_wasUsed) {
      _hasNext = rowIter.advanceNext()
      _wasUsed = true
    }
    _hasNext
  }
  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    val row: InternalRow = rowIter.getRow.copy()
    _hasNext = rowIter.advanceNext()
    row
  }
}

private final class RowIteratorFromScala(scalaIter: Iterator[InternalRow]) extends RowIterator {
  private[this] var _next: InternalRow = null
  override def advanceNext(): Boolean = {
    if (scalaIter.hasNext) {
      _next = scalaIter.next()
      true
    } else {
      _next = null
      false
    }
  }
  override def getRow: InternalRow = _next
  override def toScala: Iterator[InternalRow] = scalaIter
}
