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

package org.apache.spark.sql.columnar

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.expressions.MutableRow

private[sql] trait NullableColumnAccessor extends ColumnAccessor {
  private var nullsBuffer: ByteBuffer = _
  private var nullCount: Int = _
  private var seenNulls: Int = 0

  private var nextNullIndex: Int = _
  private var pos: Int = 0

  abstract override protected def initialize() {
    nullsBuffer = underlyingBuffer.duplicate().order(ByteOrder.nativeOrder())
    nullCount = nullsBuffer.getInt()
    nextNullIndex = if (nullCount > 0) nullsBuffer.getInt() else -1
    pos = 0

    underlyingBuffer.position(underlyingBuffer.position + 4 + nullCount * 4)
    super.initialize()
  }

  abstract override def extractTo(row: MutableRow, ordinal: Int) {
    if (pos == nextNullIndex) {
      seenNulls += 1

      if (seenNulls < nullCount) {
        nextNullIndex = nullsBuffer.getInt()
      }

      row.setNullAt(ordinal)
    } else {
      super.extractTo(row, ordinal)
    }

    pos += 1
  }

  abstract override def hasNext = seenNulls < nullCount || super.hasNext
}
