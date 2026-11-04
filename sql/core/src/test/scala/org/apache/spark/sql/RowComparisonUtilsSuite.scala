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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite

class RowComparisonUtilsSuite extends SparkFunSuite {
  import RowComparisonUtils.compare

  test("sets are compared regardless of iteration order") {
    // Scala keeps the insertion order of immutable sets with up to four elements.
    assert(compare(Set(1, 2, 3), Set(3, 1, 2)))
    assert(compare(Set(1, 2, 3, 4), Set(4, 3, 2, 1)))
    assert(compare(Set(1, 2, 3, 4, 5), Set(5, 4, 3, 2, 1)))
    assert(compare(Seq(Set(1, 2, 3)), Seq(Set(3, 1, 2))))
    assert(compare(Row(Set("a", "b")), Row(Set("b", "a"))))
    assert(compare(scala.collection.mutable.LinkedHashSet(1, 2), Set(2, 1)))
    assert(compare(Set[Any](1, null), Set[Any](null, 1)))
  }

  test("sets with different elements are not equal") {
    assert(!compare(Set(1, 2, 3), Set(1, 2)))
    assert(!compare(Set(1, 2), Set(1, 2, 3)))
    assert(!compare(Set(1, 2, 3), Set(1, 2, 4)))
    assert(!compare(Set(1), Set.empty[Int]))
    assert(compare(Set.empty[Int], Set.empty[Int]))
  }

  test("set elements are compared with compare") {
    // Arrays are compared by content, even though Scala sets hold them by reference.
    assert(compare(Set(Array(1, 2), Array(3)), Set(Array(3), Array(1, 2))))
    assert(!compare(Set(Array(1, 2), Array(3)), Set(Array(3), Array(1, 3))))
    // NaN equals NaN, and 0.0 differs from -0.0.
    assert(compare(Set(Double.NaN, 1.0), Set(1.0, Double.NaN)))
    assert(!compare(Set(0.0, 1.0), Set(1.0, -0.0)))
    assert(compare(Set(Set(1, 2), Set(3)), Set(Set(3), Set(2, 1))))
  }

  test("sequences are still compared in order") {
    assert(compare(Seq(1, 2, 3), Seq(1, 2, 3)))
    assert(!compare(Seq(1, 2, 3), Seq(3, 1, 2)))
  }
}
