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
import org.apache.spark.sql.catalyst.util.StringUtils._

class StringUtilsSuite extends SparkFunSuite {
  test("escapeLikeRegex") {
    assert(escapeLikeRegex("abdef".getBytes()) === "abdef".getBytes())
    assert(escapeLikeRegex("a\\__b".getBytes()) === "a_.b".getBytes())
    assert(escapeLikeRegex("a_%b".getBytes()) === "a..*b".getBytes())
    assert(escapeLikeRegex("a%\\%b".getBytes()) === "a.*%b".getBytes())
    assert(escapeLikeRegex("a%".getBytes()) === "a.*".getBytes())
    assert(escapeLikeRegex("**".getBytes()) === "**".getBytes())
    assert(escapeLikeRegex("a_b".getBytes()) === "a.b".getBytes())
  }

  test("escapeLikeRegexJavaFallback") {
    assert(escapeLikeRegexJavaFallback("abdef") === "(?s)\\Qa\\E\\Qb\\E\\Qd\\E\\Qe\\E\\Qf\\E")
    assert(escapeLikeRegexJavaFallback("a\\__b") === "(?s)\\Qa\\E_.\\Qb\\E")
    assert(escapeLikeRegexJavaFallback("a_%b") === "(?s)\\Qa\\E..*\\Qb\\E")
    assert(escapeLikeRegexJavaFallback("a%\\%b") === "(?s)\\Qa\\E.*%\\Qb\\E")
    assert(escapeLikeRegexJavaFallback("a%") === "(?s)\\Qa\\E.*")
    assert(escapeLikeRegexJavaFallback("**") === "(?s)\\Q*\\E\\Q*\\E")
    assert(escapeLikeRegexJavaFallback("a_b") === "(?s)\\Qa\\E.\\Qb\\E")
  }
}
