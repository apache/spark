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

package org.apache.spark.unsafe.types

import scala.collection.parallel.immutable.ParSeq
import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.SparkException
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.catalyst.util.CollationFactory._
import org.apache.spark.unsafe.types.UTF8String.{fromString => toUTF8}

class CollationFactorySuite extends AnyFunSuite with Matchers { // scalastyle:ignore funsuite
  test("collationId stability") {
    val utf8Binary = fetchCollation(0)
    assert(utf8Binary.collationName == "UTF8_BINARY")
    assert(utf8Binary.supportsBinaryEquality)

    val utf8BinaryLcase = fetchCollation(1)
    assert(utf8BinaryLcase.collationName == "UTF8_BINARY_LCASE")
    assert(!utf8BinaryLcase.supportsBinaryEquality)

    val unicode = fetchCollation(2)
    assert(unicode.collationName == "UNICODE")
    assert(unicode.supportsBinaryEquality);

    val unicodeCi = fetchCollation(3)
    assert(unicodeCi.collationName == "UNICODE_CI")
    assert(!unicodeCi.supportsBinaryEquality)
  }

  test("fetch invalid collation name") {
    val error = intercept[SparkException] {
      fetchCollation("UTF8_BS")
    }

    assert(error.getErrorClass === "COLLATION_INVALID_NAME")
    assert(error.getMessageParameters.asScala ===
      Map("proposal" -> "UTF8_BINARY", "collationName" -> "UTF8_BS"))
  }

  case class CollationTestCase[R](collationName: String, s1: String, s2: String, expectedResult: R)

  test("collation aware equality and hash") {
    val checks = Seq(
      CollationTestCase("UTF8_BINARY", "aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY", "aaa", "AAA", false),
      CollationTestCase("UTF8_BINARY", "aaa", "bbb", false),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "AAA", true),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "AaA", true),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "AaA", true),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "aa", false),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "bbb", false),
      CollationTestCase("UNICODE", "aaa", "aaa", true),
      CollationTestCase("UNICODE", "aaa", "AAA", false),
      CollationTestCase("UNICODE", "aaa", "bbb", false),
      CollationTestCase("UNICODE_CI", "aaa", "aaa", true),
      CollationTestCase("UNICODE_CI", "aaa", "AAA", true),
      CollationTestCase("UNICODE_CI", "aaa", "bbb", false))

    checks.foreach(testCase => {
      val collation = fetchCollation(testCase.collationName)
      assert(collation.equalsFunction(toUTF8(testCase.s1), toUTF8(testCase.s2)) ==
        testCase.expectedResult)

      val hash1 = collation.hashFunction.applyAsLong(toUTF8(testCase.s1))
      val hash2 = collation.hashFunction.applyAsLong(toUTF8(testCase.s1))
      assert(hash1 == hash2)
    })
  }

  test("collation aware compare") {
    val checks = Seq(
      CollationTestCase("UTF8_BINARY", "aaa", "aaa", 0),
      CollationTestCase("UTF8_BINARY", "aaa", "AAA", 1),
      CollationTestCase("UTF8_BINARY", "aaa", "bbb", -1),
      CollationTestCase("UTF8_BINARY", "aaa", "BBB", 1),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "aaa", 0),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "AAA", 0),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "aa", 1),
      CollationTestCase("UTF8_BINARY_LCASE", "aaa", "bbb", -1),
      CollationTestCase("UNICODE", "aaa", "aaa", 0),
      CollationTestCase("UNICODE", "aaa", "AAA", -1),
      CollationTestCase("UNICODE", "aaa", "bbb", -1),
      CollationTestCase("UNICODE", "aaa", "BBB", -1),
      CollationTestCase("UNICODE_CI", "aaa", "aaa", 0),
      CollationTestCase("UNICODE_CI", "aaa", "AAA", 0),
      CollationTestCase("UNICODE_CI", "aaa", "bbb", -1))

    checks.foreach(testCase => {
      val collation = fetchCollation(testCase.collationName)
      val result = collation.comparator.compare(toUTF8(testCase.s1), toUTF8(testCase.s2))
      assert(Integer.signum(result) == testCase.expectedResult)
    })
  }

  test("collation aware string search") {
    val checks = Seq(
      CollationTestCase("UNICODE_CI", "abcde", "", 0),
      CollationTestCase("UNICODE_CI", "abcde", "abc", 3),
      CollationTestCase("UNICODE_CI", "abcde", "C", 1),
      CollationTestCase("UNICODE_CI", "abcde", "dE", 2),
      CollationTestCase("UNICODE_CI", "abcde", "abcde", 5),
      CollationTestCase("UNICODE_CI", "abcde", "ABCDE", 5),
      CollationTestCase("UNICODE_CI", "abcde", "fgh", 0),
      CollationTestCase("UNICODE_CI", "abcde", "FGH", 0)
    )

    checks.foreach(testCase => {
      val collationId = collationNameToId(testCase.collationName)
      val stringSearch = getStringSearch(toUTF8(testCase.s1), toUTF8(testCase.s2), collationId)
      var result = 0
      while (stringSearch.next() != -1 && result == 0) {
        if (stringSearch.getMatchLength == stringSearch.getPattern.length()) {
          result = stringSearch.getMatchLength
        }
      }
      assert(result == testCase.expectedResult)
    })
  }

  test("test concurrently generating collation keys") {
    // generating ICU sort keys is not thread-safe by default so this should fail
    // if we don't handle the concurrency properly on Collator level

    (0 to 10).foreach(_ => {
      val collator = fetchCollation("UNICODE").collator

      ParSeq(0 to 100).foreach { _ =>
        collator.getCollationKey("aaa")
      }
    })
  }
}
