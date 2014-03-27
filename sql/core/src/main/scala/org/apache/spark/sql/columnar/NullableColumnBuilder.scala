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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.Row

/**
 * Builds a nullable column. The byte buffer of a nullable column contains:
 * - 4 bytes for the null count (number of nulls)
 * - positions for each null, in ascending order
 * - the non-null data (column data type, compression type, data...)
 */
private[sql] trait NullableColumnBuilder extends ColumnBuilder {
  private var nulls: ByteBuffer = _
  private var pos: Int = _
  private var nullCount: Int = _

  abstract override def initialize(initialSize: Int, columnName: String) {
    nulls = ByteBuffer.allocate(1024)
    nulls.order(ByteOrder.nativeOrder())
    pos = 0
    nullCount = 0
    super.initialize(initialSize, columnName)
  }

  abstract override def appendFrom(row: Row, ordinal: Int) {
    if (row.isNullAt(ordinal)) {
      nulls = ColumnBuilder.ensureFreeSpace(nulls, 4)
      nulls.putInt(pos)
      nullCount += 1
    } else {
      super.appendFrom(row, ordinal)
    }
    pos += 1
  }

  abstract override def build(): ByteBuffer = {
    val nonNulls = super.build()
    val typeId = nonNulls.getInt()
    val nullDataLen = nulls.position()

    nulls.limit(nullDataLen)
    nulls.rewind()

    // Column type ID is moved to the front, follows the null count, then non-null data
    //
    //      +---------+
    //      | 4 bytes | Column type ID
    //      +---------+
    //      | 4 bytes | Null count
    //      +---------+
    //      |   ...   | Null positions (if null count is not zero)
    //      +---------+
    //      |   ...   | Non-null part (without column type ID)
    //      +---------+
    val buffer = ByteBuffer
      .allocate(4 + nullDataLen + nonNulls.limit)
      .order(ByteOrder.nativeOrder())
      .putInt(typeId)
      .putInt(nullCount)
      .put(nulls)
      .put(nonNulls)

    buffer.rewind()
    buffer
  }
}
