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

package org.apache.spark.sql.sources.v2

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

/**
 * A simple test suite to verify `DataSourceV2Options`.
 */
class DataSourceV2OptionsSuite extends SparkFunSuite {

  test("key is case-insensitive") {
    val options = new DataSourceV2Options(Map("foo" -> "bar").asJava)
    assert(options.get("foo").get() == "bar")
    assert(options.get("FoO").get() == "bar")
    assert(!options.get("abc").isPresent)
  }

  test("value is case-sensitive") {
    val options = new DataSourceV2Options(Map("foo" -> "bAr").asJava)
    assert(options.get("foo").get == "bAr")
  }

  test("getInt") {
    val options = new DataSourceV2Options(Map("numFoo" -> "1", "foo" -> "bar").asJava)
    assert(options.getInt("numFoo").get == 1)

    intercept[NumberFormatException]{
      options.getInt("foo").isPresent
    }
  }

  test("getBoolean") {
    val options = new DataSourceV2Options(
      Map("isFoo" -> "true", "isFoo2" -> "false", "foo" -> "bar").asJava)
    assert(options.getBoolean("isFoo").get == true)
    assert(options.getBoolean("isFoo2").get == false)
    assert(options.getBoolean("isFoo2").isPresent)
    assert(options.getBoolean("foo").get == false)
    assert(!options.getBoolean("isBar").isPresent)
  }

  test("getLong") {
    val options = new DataSourceV2Options(Map("numFoo" -> "9223372036854775807",
      "foo" -> "bar").asJava)
    assert(options.getLong("numFoo").get == 9223372036854775807L)

    intercept[NumberFormatException]{
      options.getLong("foo").isPresent
    }
  }
}
