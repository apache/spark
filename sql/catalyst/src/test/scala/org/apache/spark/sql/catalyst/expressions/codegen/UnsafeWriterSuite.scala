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

class UnsafeWriterSuite extends SparkFunSuite {

  test("SPARK-25538: zero-out all bits for decimals") {
    // This decimal holds 8 bytes
    val decimal1 = Decimal(0.431)
    decimal1.changePrecision(38, 18)
    // This decimal holds 11 bytes
    val decimal2 = Decimal(123456789.1232456789)
    decimal2.changePrecision(38, 18)
    val unsafeRowWriter = new UnsafeRowWriter(1)
    unsafeRowWriter.resetRowWriter()
    unsafeRowWriter.write(0, decimal2, decimal2.precision, decimal2.scale)
    unsafeRowWriter.reset()
    unsafeRowWriter.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res = unsafeRowWriter.getRow
    assert(res.getDecimal(0, decimal1.precision, decimal1.scale) == decimal1)
    // Check that the bytes which are not used by decimal1 (but are allocated) are zero-ed out
    assert(res.getBytes()(25) == 0x00)
  }

}
