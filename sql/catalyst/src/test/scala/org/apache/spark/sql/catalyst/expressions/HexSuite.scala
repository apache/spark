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

class HexSuite extends SparkFunSuite {
  test("SPARK-48596: hex long values") {
    assert(Hex.hex(0).toString === "0")
    assert(Hex.hex(1).toString === "1")
    assert(Hex.hex(15).toString === "F")
    assert(Hex.hex(16).toString === "10")
    assert(Hex.hex(255).toString === "FF")
    assert(Hex.hex(256).toString === "100")
    assert(Hex.hex(4095).toString === "FFF")
    assert(Hex.hex(4096).toString === "1000")
    assert(Hex.hex(65535).toString === "FFFF")
    assert(Hex.hex(65536).toString === "10000")
    assert(Hex.hex(1048575).toString === "FFFFF")
    assert(Hex.hex(1048576).toString === "100000")
    assert(Hex.hex(-1).toString === "FFFFFFFFFFFFFFFF")
    assert(Hex.hex(Long.MinValue).toString === "8000000000000000")
    assert(Hex.hex(Long.MaxValue).toString === "7FFFFFFFFFFFFFFF")
  }
}
