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

package org.apache.spark.sql.columnar.compression

import org.apache.spark.sql.columnar._
import org.apache.spark.sql.types.NativeType

class TestCompressibleColumnBuilder[T <: NativeType](
    override val columnStats: ColumnStats,
    override val columnType: NativeColumnType[T],
    override val schemes: Seq[CompressionScheme])
  extends NativeColumnBuilder(columnStats, columnType)
  with NullableColumnBuilder
  with CompressibleColumnBuilder[T] {

  override protected def isWorthCompressing(encoder: Encoder[T]) = true
}

object TestCompressibleColumnBuilder {
  def apply[T <: NativeType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T],
      scheme: CompressionScheme) = {

    val builder = new TestCompressibleColumnBuilder(columnStats, columnType, Seq(scheme))
    builder.initialize(0, "", useCompression = true)
    builder
  }
}
