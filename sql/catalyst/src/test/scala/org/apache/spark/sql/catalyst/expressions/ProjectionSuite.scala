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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class ProjectionSuite extends SparkFunSuite {

  test("unsafe projections") {
    val row = InternalRow(1, 2L, 3.1, null, UTF8String.fromString("hello"), Array[Byte](12, 12),
      Decimal("10.01"))
    val fields = Seq(IntegerType, LongType, DoubleType, FloatType, StringType, BinaryType,
      DecimalType(10, 2))

    val toUnsafe = new ToUnsafeProjection(fields)
    val unsafeRow = toUnsafe(row).asInstanceOf[UnsafeRow]
    assert(unsafeRow.getInt(0) === 1)
    assert(unsafeRow.getLong(1) === 2L)
    assert(unsafeRow.getDouble(2) === 3.1)
    assert(unsafeRow.isNullAt(3) === true)
    assert(unsafeRow.getAs[UTF8String](4) === UTF8String.fromString("hello"))
    assert(java.util.Arrays.equals(unsafeRow.getAs[Array[Byte]](5), Array[Byte](12, 12)))
    assert(unsafeRow.getAs[Decimal](6) === Decimal("10.01"))

    val fromUnsafe = new FromUnsafeProjection(fields)
    val genericRow = fromUnsafe(unsafeRow)
    assert(genericRow === row)
  }
}
