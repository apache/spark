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

package org.apache.spark.sql.execution.columnar.compression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.{ColumnAccessor, NativeColumnAccessor}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.AtomicType

private[columnar] trait CompressibleColumnAccessor[T <: AtomicType] extends ColumnAccessor {
  this: NativeColumnAccessor[T] =>

  private var decoder: Decoder[T] = _

  abstract override protected def initialize(): Unit = {
    super.initialize()
    decoder = CompressionScheme(underlyingBuffer.getInt()).decoder(buffer, columnType)
  }

  abstract override def hasNext: Boolean = super.hasNext || decoder.hasNext

  override def extractSingle(row: InternalRow, ordinal: Int): Unit = {
    decoder.next(row, ordinal)
  }

  def decompress(columnVector: WritableColumnVector, capacity: Int): Unit =
    decoder.decompress(columnVector, capacity)
}
