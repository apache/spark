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
package org.apache.spark.util

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class SparkSchemaUtilsSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("escapeMetaCharacters escapes each supported control character") {
    assert(SparkSchemaUtils.escapeMetaCharacters("\n") === "\\n")
    assert(SparkSchemaUtils.escapeMetaCharacters("\r") === "\\r")
    assert(SparkSchemaUtils.escapeMetaCharacters("\t") === "\\t")
    assert(SparkSchemaUtils.escapeMetaCharacters("\f") === "\\f")
    assert(SparkSchemaUtils.escapeMetaCharacters("\b") === "\\b")
    assert(SparkSchemaUtils.escapeMetaCharacters("\u000B") === "\\v")
    assert(SparkSchemaUtils.escapeMetaCharacters("\u0007") === "\\a")
  }

  test("escapeMetaCharacters leaves ordinary characters unchanged") {
    assert(SparkSchemaUtils.escapeMetaCharacters("") === "")
    assert(SparkSchemaUtils.escapeMetaCharacters("abc 123 XYZ") === "abc 123 XYZ")
    // A regular space is not a meta character and must be preserved.
    assert(SparkSchemaUtils.escapeMetaCharacters(" ") === " ")
  }

  test("escapeMetaCharacters escapes every occurrence, including mixed input") {
    assert(SparkSchemaUtils.escapeMetaCharacters("a\nb\tc") === "a\\nb\\tc")
    assert(SparkSchemaUtils.escapeMetaCharacters("\n\n") === "\\n\\n")
    assert(SparkSchemaUtils.escapeMetaCharacters("x\ry\fz") === "x\\ry\\fz")
  }
}
