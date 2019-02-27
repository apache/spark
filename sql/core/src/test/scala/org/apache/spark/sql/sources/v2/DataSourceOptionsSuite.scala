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
 * A simple test suite to verify `DataSourceOptions`.
 */
class DataSourceOptionsSuite extends SparkFunSuite {

  test("key is case-insensitive") {
    val options = new DataSourceOptions(Map("foo" -> "bar").asJava)
    assert(options.get("foo").get() == "bar")
    assert(options.get("FoO").get() == "bar")
    assert(!options.get("abc").isPresent)
  }

  test("value is case-sensitive") {
    val options = new DataSourceOptions(Map("foo" -> "bAr").asJava)
    assert(options.get("foo").get == "bAr")
  }

  test("getInt") {
    val options = new DataSourceOptions(Map("numFOo" -> "1", "foo" -> "bar").asJava)
    assert(options.getInt("numFOO", 10) == 1)
    assert(options.getInt("numFOO2", 10) == 10)

    intercept[NumberFormatException]{
      options.getInt("foo", 1)
    }
  }

  test("getBoolean") {
    val options = new DataSourceOptions(
      Map("isFoo" -> "true", "isFOO2" -> "false", "foo" -> "bar").asJava)
    assert(options.getBoolean("isFoo", false))
    assert(!options.getBoolean("isFoo2", true))
    assert(options.getBoolean("isBar", true))
    assert(!options.getBoolean("isBar", false))
    assert(!options.getBoolean("FOO", true))
  }

  test("getLong") {
    val options = new DataSourceOptions(Map("numFoo" -> "9223372036854775807",
      "foo" -> "bar").asJava)
    assert(options.getLong("numFOO", 0L) == 9223372036854775807L)
    assert(options.getLong("numFoo2", -1L) == -1L)

    intercept[NumberFormatException]{
      options.getLong("foo", 0L)
    }
  }

  test("getDouble") {
    val options = new DataSourceOptions(Map("numFoo" -> "922337.1",
      "foo" -> "bar").asJava)
    assert(options.getDouble("numFOO", 0d) == 922337.1d)
    assert(options.getDouble("numFoo2", -1.02d) == -1.02d)

    intercept[NumberFormatException]{
      options.getDouble("foo", 0.1d)
    }
  }

  test("standard options") {
    val options = new DataSourceOptions(Map(
      DataSourceOptions.PATH_KEY -> "abc",
      DataSourceOptions.TABLE_KEY -> "tbl").asJava)

    assert(options.paths().toSeq == Seq("abc"))
    assert(options.tableName().get() == "tbl")
    assert(!options.databaseName().isPresent)
  }

  test("standard options with both singular path and multi-paths") {
    val options = new DataSourceOptions(Map(
      DataSourceOptions.PATH_KEY -> "abc",
      DataSourceOptions.PATHS_KEY -> """["c", "d"]""").asJava)

    assert(options.paths().toSeq == Seq("abc", "c", "d"))
  }

  test("standard options with only multi-paths") {
    val options = new DataSourceOptions(Map(
      DataSourceOptions.PATHS_KEY -> """["c", "d\"e"]""").asJava)

    assert(options.paths().toSeq == Seq("c", "d\"e"))
  }
}
