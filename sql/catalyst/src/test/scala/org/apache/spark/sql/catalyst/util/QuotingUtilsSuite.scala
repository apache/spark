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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite

class QuotingUtilsSuite extends SparkFunSuite {

  test("do not quote for legal identifier strings") {
    assert(quoteIfNeeded("a") == "a")
    assert(quoteIfNeeded("a0") == "a0")
    assert(quoteIfNeeded("a0a") == "a0a")
    assert(quoteIfNeeded("_") == "_")
    assert(quoteIfNeeded("_a") == "_a")
    assert(quoteIfNeeded("_0") == "_0")
    assert(quoteIfNeeded("_ab_") === "_ab_")
  }

  test("quote for illegal identifier strings") {
    assert(quoteIfNeeded("&") == "`&`")
    assert(quoteIfNeeded("a b") === "`a b`")
    assert(quoteIfNeeded("a*b") === "`a*b`")
    assert(quoteIfNeeded("0") == "`0`")
    assert(quoteIfNeeded("01") == "`01`")
    assert(quoteIfNeeded("0d") == "`0d`")
    assert(quoteIfNeeded("") === "``")
  }
}
