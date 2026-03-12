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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.{escapePathName, unescapePathName}

class ExternalCatalogUtilsSuite extends SparkFunSuite {

  test("SPARK-48551: escapePathName") {
    ExternalCatalogUtils.charToEscape.stream().toArray.map(_.asInstanceOf[Char]).foreach { c =>
      // Check parity with old conversion technique:
      assert(escapePathName(c.toString) === "%" + f"$c%02X",
        s"wrong escaping for $c")
    }
    assert(escapePathName("") === "")
    assert(escapePathName(" ") === " ")
    assert(escapePathName("\n") === "%0A")
    assert(escapePathName("a b") === "a b")
    assert(escapePathName("a:b") === "a%3Ab")
    assert(escapePathName(":ab") === "%3Aab")
    assert(escapePathName("ab:") === "ab%3A")
    assert(escapePathName("a%b") === "a%25b")
    assert(escapePathName("a,b") === "a,b")
    assert(escapePathName("a/b") === "a%2Fb")
  }

  test("SPARK-48551: unescapePathName") {
    ExternalCatalogUtils.charToEscape.stream().toArray.map(_.asInstanceOf[Char]).foreach { c =>
      // Check parity with old conversion technique:
      assert(unescapePathName("%" + f"$c%02X") === c.toString,
        s"wrong unescaping for $c")
    }
    assert(unescapePathName(null) === null)
    assert(unescapePathName("") === "")
    assert(unescapePathName(" ") === " ")
    assert(unescapePathName("%0A") === "\n")
    assert(unescapePathName("a b") === "a b")
    assert(unescapePathName("a%3Ab") === "a:b")
    assert(unescapePathName("%3Aab") === ":ab")
    assert(unescapePathName("ab%3A") === "ab:")
    assert(unescapePathName("a%25b") === "a%b")
    assert(unescapePathName("a,b") === "a,b")
    assert(unescapePathName("a%2Fb") === "a/b")
    assert(unescapePathName("a%2") === "a%2")
    assert(unescapePathName("a%F ") === "a%F ")
    assert(unescapePathName("%0") === "%0")
    assert(unescapePathName("0%") === "0%")
    // scalastyle:off nonascii
    assert(unescapePathName("a\u00FF") === "a\u00FF")
    // scalastyle:on nonascii
  }
}
