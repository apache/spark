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
package org.apache.spark

import org.json4s._
import org.json4s.jackson.JsonMethods

trait JsonTestUtils {
  def assertValidDataInJson(validateJson: JValue, expectedJson: JValue): Unit = {
    val Diff(c, a, d) = validateJson.diff(expectedJson)
    val validatePretty = JsonMethods.pretty(validateJson)
    val expectedPretty = JsonMethods.pretty(expectedJson)
    val errorMessage = s"Expected:\n$expectedPretty\nFound:\n$validatePretty"
    import org.scalactic.TripleEquals._
    assert(c === JNothing, s"$errorMessage\nChanged:\n${JsonMethods.pretty(c)}")
    assert(a === JNothing, s"$errorMessage\nAdded:\n${JsonMethods.pretty(a)}")
    assert(d === JNothing, s"$errorMessage\nDeleted:\n${JsonMethods.pretty(d)}")
  }

}
