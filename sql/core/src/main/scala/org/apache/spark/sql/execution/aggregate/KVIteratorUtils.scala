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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.KVIterator

object KVIteratorUtils {
  def fromIterator(
      inputIter: Iterator[InternalRow],
      keyProjection: InternalRow => InternalRow,
      valueProjection: InternalRow => InternalRow): KVIterator[InternalRow, InternalRow] = {
    new KVIterator[InternalRow, InternalRow] {
      private[this] var key: InternalRow = _
      private[this] var value: InternalRow = _
      override def getKey: InternalRow = key
      override def getValue: InternalRow = value
      override def close(): Unit = { /* Do nothing */ }
      override def next(): Boolean = {
        if (inputIter.hasNext) {
          val inputRow = inputIter.next()
          key = keyProjection(inputRow)
          value = valueProjection(inputRow)
          true
        } else {
          false
        }
      }
    }
  }
}
