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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite

// scalastyle:off
class UTF8StringSuite extends SparkFunSuite {
  test("basic") {
    def check(str: String, len: Int) {

      assert(UTF8String(str).length == len)
      assert(UTF8String(str.getBytes("utf8")).length() == len)

      assert(UTF8String(str) == str)
      assert(UTF8String(str.getBytes("utf8")) == str)
      assert(UTF8String(str).toString == str)
      assert(UTF8String(str.getBytes("utf8")).toString == str)
      assert(UTF8String(str.getBytes("utf8")) == UTF8String(str))

      assert(UTF8String(str).hashCode() == UTF8String(str.getBytes("utf8")).hashCode())
    }

    check("hello", 5)
    check("世 界", 3)
  }

  test("contains") {
    assert(UTF8String("hello").contains(UTF8String("ello")))
    assert(!UTF8String("hello").contains(UTF8String("vello")))
    assert(UTF8String("大千世界").contains(UTF8String("千世")))
    assert(!UTF8String("大千世界").contains(UTF8String("世千")))
  }

  test("prefix") {
    assert(UTF8String("hello").startsWith(UTF8String("hell")))
    assert(!UTF8String("hello").startsWith(UTF8String("ell")))
    assert(UTF8String("大千世界").startsWith(UTF8String("大千")))
    assert(!UTF8String("大千世界").startsWith(UTF8String("千")))
  }

  test("suffix") {
    assert(UTF8String("hello").endsWith(UTF8String("ello")))
    assert(!UTF8String("hello").endsWith(UTF8String("ellov")))
    assert(UTF8String("大千世界").endsWith(UTF8String("世界")))
    assert(!UTF8String("大千世界").endsWith(UTF8String("世")))
  }

  test("slice") {
    assert(UTF8String("hello").slice(1, 3) == UTF8String("el"))
    assert(UTF8String("大千世界").slice(0, 1) == UTF8String("大"))
    assert(UTF8String("大千世界").slice(1, 3) == UTF8String("千世"))
    assert(UTF8String("大千世界").slice(3, 5) == UTF8String("界"))
  }
}
