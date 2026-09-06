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

class SparkCollectionUtilsSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("toMapWithIndex maps each key to its position, matching zipWithIndex.toMap") {
    assert(SparkCollectionUtils.toMapWithIndex(Seq("a", "b", "c")) ===
      Map("a" -> 0, "b" -> 1, "c" -> 2))
    assert(SparkCollectionUtils.toMapWithIndex(Seq.empty[String]) === Map.empty[String, Int])
    // Duplicate keys keep the last index, matching zipWithIndex.toMap.
    assert(SparkCollectionUtils.toMapWithIndex(Seq("a", "a")) === Map("a" -> 1))
    val keys = Seq(10, 20, 30, 40)
    assert(SparkCollectionUtils.toMapWithIndex(keys) === keys.zipWithIndex.toMap)
  }

  test("isEmpty and isNotEmpty handle null, empty and non-empty maps") {
    val nullMap: java.util.Map[String, Int] = null
    assert(SparkCollectionUtils.isEmpty(nullMap))
    assert(!SparkCollectionUtils.isNotEmpty(nullMap))

    val empty = new java.util.HashMap[String, Int]()
    assert(SparkCollectionUtils.isEmpty(empty))
    assert(!SparkCollectionUtils.isNotEmpty(empty))

    val nonEmpty = new java.util.HashMap[String, Int]()
    nonEmpty.put("a", 1)
    assert(!SparkCollectionUtils.isEmpty(nonEmpty))
    assert(SparkCollectionUtils.isNotEmpty(nonEmpty))
  }

  test("createArray fills primitive-typed arrays with the default value") {
    assert(SparkCollectionUtils.createArray(3, 7) === Array(7, 7, 7))
    assert(SparkCollectionUtils.createArray(2, true) === Array(true, true))
    assert(SparkCollectionUtils.createArray(2, 1L) === Array(1L, 1L))
    assert(SparkCollectionUtils.createArray(2, 2.5d) === Array(2.5d, 2.5d))
    assert(SparkCollectionUtils.createArray(2, 1.5f) === Array(1.5f, 1.5f))
    assert(SparkCollectionUtils.createArray(2, 3.toByte) === Array(3.toByte, 3.toByte))
    assert(SparkCollectionUtils.createArray(2, 4.toShort) === Array(4.toShort, 4.toShort))
    assert(SparkCollectionUtils.createArray(2, 'a') === Array('a', 'a'))
    assert(SparkCollectionUtils.createArray(0, 7).isEmpty)
  }

  test("createArray fills reference-typed arrays and returns empty for size 0") {
    assert(SparkCollectionUtils.createArray(2, "x") === Array("x", "x"))
    assert(SparkCollectionUtils.createArray(0, "x").isEmpty)
  }
}
