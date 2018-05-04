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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.data.{ArrayData, InternalRow, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * This package defines the internal data representation used by Spark SQL.
 *
 * These class can be used to provide values directly to Spark that will not be translated. For
 * example, when accessing a timestamp from [[InternalRow]], Spark retrieves an unboxed long value
 * using `getLong`. The value encodes the timestamp as microseconds from the Unix epoch in UTC, and
 * Spark carries out computations using that microsecond value.
 *
 * Spark uses the following in-memory representations for its data types:
 *
 * Container types:
 * - [[StructType]] - [[InternalRow]] contains a tuple of values with different types
 * - [[ArrayType]] - [[ArrayData]] contains a sequence of values with the same type
 * - [[MapType]] - [[MapData]] wraps an [[ArrayData]] of map keys and an [[ArrayData]] of values
 *
 * Scalar types:
 * - [[BooleanType]] - unboxed boolean
 * - [[ByteType]] - unboxed byte
 * - [[ShortType]] - unboxed short
 * - [[IntegerType]] - unboxed int
 * - [[LongType]] - unboxed long
 * - [[FloatType]] - unboxed float
 * - [[DoubleType]] - unboxed double
 * - [[DecimalType]] - [[Decimal]] that contains the value's scale, precision, and unscaled value.
 *   Unscaled value is stored as an unboxed long if precision is less than 19 or with a
 *   [[BigDecimal]] when precision 19 or greater
 * - [[DateType]] - unboxed int that encodes the number of days from the Unix epoch, 1970-01-01
 * - [[TimestampType]] - unboxed long that encodes microseconds from the Unix epoch,
 *   1970-01-01T00:00:00.000000
 * - [[CalendarIntervalType]] - [[CalendarInterval]]
 * - [[StringType]] - [[UTF8String]] backed by a byte array storing UTF-8 encoded text
 * - [[BinaryType]] - a byte array (Java `byte[]`)
 * - [[NullType]] - always null; accessed using `isNullAt` and `setNullAt`.
 */
package object data {
}
