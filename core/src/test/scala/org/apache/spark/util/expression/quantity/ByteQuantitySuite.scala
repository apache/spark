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
package org.apache.spark.util.expression.quantity

import org.scalatest.FunSuite
class ByteQuantitySuite extends FunSuite {

  test("kb") {
    assert( ByteQuantity(1, "kb").toBytes == 1000)
  }

  test("mb") {
    assert( ByteQuantity(1, "mb").toBytes == 1000L * 1000)
  }

  test("gb") {
    assert( ByteQuantity(1, "gb").toBytes == 1000L * 1000 * 1000)
  }

  test("tb") {
    assert( ByteQuantity(1, "tb").toBytes == 1000L * 1000 * 1000 * 1000)
  }

  test("pb") {
    assert( ByteQuantity(1, "pb").toBytes == 1000L * 1000 * 1000 * 1000 * 1000)
  }

  test("eb") {
    assert( ByteQuantity(1, "eb").toBytes == 1000L * 1000 * 1000 * 1000 * 1000 * 1000)
  }

  test("eb to kb") {
    assert( ByteQuantity(1, "eb").toKB == 1000L * 1000 * 1000 * 1000 * 1000)
  }

  test("eb to mb") {
    assert( ByteQuantity(1, "eb").toMB == 1000L * 1000 * 1000 * 1000)
  }

  test("eb to gb") {
    assert( ByteQuantity(1, "eb").toGB == 1000L * 1000 * 1000)
  }

  test("eb to tb") {
    assert( ByteQuantity(1, "eb").toTB == 1000L * 1000)
  }

  test("eb to pb") {
    assert( ByteQuantity(1, "eb").toPB == 1000L)
  }

  test("eb to eb") {
    assert( ByteQuantity(1, "eb").toEB == 1)
  }

  test("K") {
    assert( ByteQuantity(1, "k").toBytes == 1024)
  }

  test("M") {
    assert( ByteQuantity(1, "m").toBytes == 1024L * 1024)
  }

  test("G") {
    assert( ByteQuantity(1, "g").toBytes == 1024L * 1024 * 1024)
  }

  test("T") {
    assert( ByteQuantity(1, "t").toBytes == 1024L * 1024 * 1024 * 1024)
  }

  test("kib") {
    assert( ByteQuantity(1, "kib").toBytes == 1024)
  }

  test("mib") {
    assert( ByteQuantity(1, "mib").toBytes == 1024L * 1024)
  }

  test("gib") {
    assert( ByteQuantity(1, "gib").toBytes == 1024L * 1024 * 1024)
  }

  test("tib") {
    assert( ByteQuantity(1, "tib").toBytes == 1024L * 1024 * 1024 * 1024)
  }

  test("pib") {
    assert( ByteQuantity(1, "pib").toBytes == 1024L * 1024 * 1024 * 1024 * 1024)
  }

  test("eib") {
    assert( ByteQuantity(1, "eib").toBytes == 1024L * 1024 * 1024 * 1024 * 1024 * 1024)
  }

  test("eib to kib") {
    assert( ByteQuantity(1, "eib").toKiB == 1024L * 1024 * 1024 * 1024 * 1024 )
  }

  test("eib to mib") {
    assert( ByteQuantity(1, "eib").toMiB == 1024L * 1024 * 1024 * 1024 )
  }

  test("eib to gib") {
    assert( ByteQuantity(1, "eib").toGiB == 1024L * 1024 * 1024)
  }

  test("eib to Tib") {
    assert( ByteQuantity(1, "eib").toTiB == 1024L * 1024)
  }

  test("eib to PiB") {
    assert( ByteQuantity(1, "eib").toPiB == 1024L)
  }

  test("eib to EiB") {
    assert( ByteQuantity(1, "eib").toEiB == 1)
  }
}
