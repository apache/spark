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

package org.apache.spark.sql.catalyst.expressions.xml

import java.io.IOException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.xml.UDFXPathUtil.ReusableStringReader

/**
 * Unit tests for [[UDFXPathUtil.ReusableStringReader]].
 *
 * Loosely based on Hive's TestReusableStringReader.java.
 */
class ReusableStringReaderSuite extends SparkFunSuite {

  private val fox = "Quick brown fox jumps over the lazy dog."

  test("empty reader") {
    val reader = new ReusableStringReader

    intercept[IOException] {
      reader.read()
    }

    intercept[IOException] {
      reader.ready()
    }

    reader.close()
  }

  test("mark reset") {
    val reader = new ReusableStringReader

    if (reader.markSupported()) {
      reader.set(fox)
      assert(reader.ready())

      val cc = new Array[Char](6)
      var read = reader.read(cc)
      assert(read == 6)
      assert("Quick " == new String(cc))

      reader.mark(100)

      read = reader.read(cc)
      assert(read == 6)
      assert("brown " == new String(cc))

      reader.reset()
      read = reader.read(cc)
      assert(read == 6)
      assert("brown " == new String(cc))
    }
    reader.close()
  }

  test("skip") {
    val reader = new ReusableStringReader
    reader.set(fox)

    // skip entire the data:
    var skipped = reader.skip(fox.length() + 1)
    assert(fox.length() == skipped)
    assert(-1 == reader.read())

    reader.set(fox) // reset the data
    val cc = new Array[Char](6)
    var read = reader.read(cc)
    assert(read == 6)
    assert("Quick " == new String(cc))

    // skip some piece of data:
    skipped = reader.skip(30)
    assert(skipped == 30)
    read = reader.read(cc)
    assert(read == 4)
    assert("dog." == new String(cc, 0, read))

    // skip when already at EOF:
    skipped = reader.skip(300)
    assert(skipped == 0, skipped)
    assert(reader.read() == -1)

    reader.close()
  }
}
