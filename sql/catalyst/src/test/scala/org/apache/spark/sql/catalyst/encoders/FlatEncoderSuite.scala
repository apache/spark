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

package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.SparkFunSuite

class FlatEncoderSuite extends SparkFunSuite {
  test("int") {
    val enc = FlatEncoder[Int]
    val row = enc.toRow(3)
    val back = enc.fromRow(row)
    assert(3 === back)
  }

  test("string") {
    val enc = FlatEncoder[String]
    val row = enc.toRow("abc")
    val back = enc.fromRow(row)
    assert("abc" === back)
  }

  test("seq") {
    val enc = FlatEncoder[Seq[String]]
    val row = enc.toRow(Seq("abc", "xzy"))
    val back = enc.fromRow(row)
    assert(back.length == 2)
    Seq("abc", "xzy").zip(back).foreach {
      case (a, b) => assert(a == b)
    }
    // not sure why `===` doesn't work...
    //assert(Seq("abc", "xyz") === back)
  }
}
