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

class SparkStringUtilsSuite
  extends AnyFunSuite { // scalastyle:ignore funsuite

  test("leftPad with spaces") {
    assert(SparkStringUtils.leftPad(null, 3) == null)
    assert(SparkStringUtils.leftPad("", 3) == "   ")
    assert(SparkStringUtils.leftPad("bat", 3) == "bat")
    assert(SparkStringUtils.leftPad("bat", 5) == "  bat")
    assert(SparkStringUtils.leftPad("bat", 1) == "bat")
    assert(SparkStringUtils.leftPad("bat", -1) == "bat")
  }

  test("leftPad with specified character") {
    assert(SparkStringUtils.leftPad(null, 3, 'z') == null)
    assert(SparkStringUtils.leftPad("", 3, 'z') == "zzz")
    assert(SparkStringUtils.leftPad("bat", 3, 'z') == "bat")
    assert(SparkStringUtils.leftPad("bat", 5, 'z') == "zzbat")
    assert(SparkStringUtils.leftPad("bat", 1, 'z') == "bat")
    assert(SparkStringUtils.leftPad("bat", -1, 'z') == "bat")
  }

  test("rightPad with spaces") {
    assert(SparkStringUtils.rightPad(null, 3) == null)
    assert(SparkStringUtils.rightPad("", 3) == "   ")
    assert(SparkStringUtils.rightPad("bat", 3) == "bat")
    assert(SparkStringUtils.rightPad("bat", 5) == "bat  ")
    assert(SparkStringUtils.rightPad("bat", 1) == "bat")
    assert(SparkStringUtils.rightPad("bat", -1) == "bat")
  }

  test("rightPad with specified character") {
    assert(SparkStringUtils.rightPad(null, 3, 'z') == null)
    assert(SparkStringUtils.rightPad("", 3, 'z') == "zzz")
    assert(SparkStringUtils.rightPad("bat", 3, 'z') == "bat")
    assert(SparkStringUtils.rightPad("bat", 5, 'z') == "batzz")
    assert(SparkStringUtils.rightPad("bat", 1, 'z') == "bat")
    assert(SparkStringUtils.rightPad("bat", -1, 'z') == "bat")
  }
}
