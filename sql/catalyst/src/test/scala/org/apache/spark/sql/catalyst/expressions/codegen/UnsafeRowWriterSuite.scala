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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.Decimal

class UnsafeRowWriterSuite extends SparkFunSuite {

  def checkDecimalSizeInBytes(decimal: Decimal, numBytes: Int): Unit = {
    assert(decimal.toJavaBigDecimal.unscaledValue().toByteArray.length == numBytes)
  }

  test("SPARK-25538: zero-out all bits for decimals") {
    val decimal1 = Decimal(0.431)
    decimal1.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal1, 8)

    val decimal2 = Decimal(123456789.1232456789)
    decimal2.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal2, 11)
    // On an UnsafeRowWriter we write decimal2 first and then decimal1
    val unsafeRowWriter1 = new UnsafeRowWriter(1)
    unsafeRowWriter1.resetRowWriter()
    unsafeRowWriter1.write(0, decimal2, decimal2.precision, decimal2.scale)
    unsafeRowWriter1.reset()
    unsafeRowWriter1.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res1 = unsafeRowWriter1.getRow
    // On a second UnsafeRowWriter we write directly decimal1
    val unsafeRowWriter2 = new UnsafeRowWriter(1)
    unsafeRowWriter2.resetRowWriter()
    unsafeRowWriter2.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res2 = unsafeRowWriter2.getRow
    // The two rows should be the equal
    assert(res1 == res2)
  }
}
