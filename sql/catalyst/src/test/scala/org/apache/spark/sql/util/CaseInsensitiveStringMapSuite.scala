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

package org.apache.spark.sql.util

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class CaseInsensitiveStringMapSuite extends SparkFunSuite {

  test("put and get") {
    val options = CaseInsensitiveStringMap.empty()
    intercept[UnsupportedOperationException] {
      options.put("kEy", "valUE")
    }
  }

  test("clear") {
    val options = new CaseInsensitiveStringMap(Map("kEy" -> "valUE").asJava)
    intercept[UnsupportedOperationException] {
      options.clear()
    }
  }

  test("key and value set") {
    val options = new CaseInsensitiveStringMap(Map("kEy" -> "valUE").asJava)
    assert(options.keySet().asScala == Set("key"))
    assert(options.values().asScala.toSeq == Seq("valUE"))
  }

  test("getInt") {
    val options = new CaseInsensitiveStringMap(Map("numFOo" -> "1", "foo" -> "bar").asJava)
    assert(options.getInt("numFOO", 10) == 1)
    assert(options.getInt("numFOO2", 10) == 10)

    intercept[NumberFormatException]{
      options.getInt("foo", 1)
    }
  }

  test("getBoolean") {
    val options = new CaseInsensitiveStringMap(
      Map("isFoo" -> "true", "isFOO2" -> "false", "foo" -> "bar").asJava)
    assert(options.getBoolean("isFoo", false))
    assert(!options.getBoolean("isFoo2", true))
    assert(options.getBoolean("isBar", true))
    assert(!options.getBoolean("isBar", false))

    intercept[IllegalArgumentException] {
      options.getBoolean("FOO", true)
    }
  }

  test("getLong") {
    val options = new CaseInsensitiveStringMap(Map("numFoo" -> "9223372036854775807",
      "foo" -> "bar").asJava)
    assert(options.getLong("numFOO", 0L) == 9223372036854775807L)
    assert(options.getLong("numFoo2", -1L) == -1L)

    intercept[NumberFormatException]{
      options.getLong("foo", 0L)
    }
  }

  test("getDouble") {
    val options = new CaseInsensitiveStringMap(Map("numFoo" -> "922337.1",
      "foo" -> "bar").asJava)
    assert(options.getDouble("numFOO", 0d) == 922337.1d)
    assert(options.getDouble("numFoo2", -1.02d) == -1.02d)

    intercept[NumberFormatException]{
      options.getDouble("foo", 0.1d)
    }
  }

  test("asCaseSensitiveMap") {
    val originalMap = new util.HashMap[String, String] {
      put("Foo", "Bar")
      put("OFO", "ABR")
      put("OoF", "bar")
    }

    val options = new CaseInsensitiveStringMap(originalMap)
    val caseSensitiveMap = options.asCaseSensitiveMap
    assert(caseSensitiveMap.equals(originalMap))
    // The result of `asCaseSensitiveMap` is read-only.
    intercept[UnsupportedOperationException] {
      caseSensitiveMap.put("kEy", "valUE")
    }
  }
}
