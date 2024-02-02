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

import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.SparkException
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.catalyst.util.CollationFactory._
import org.apache.spark.unsafe.types.UTF8String.{fromString => toUTF8}

class CollationFactorySuite extends AnyFunSuite with Matchers { // scalastyle:ignore funsuite
  test("collationId stability") {
    val ucsBasic = fetchCollation(0)
    assert(ucsBasic.collationName == "UCS_BASIC")
    assert(ucsBasic.isBinaryCollation)

    val ucsBasicLcase = fetchCollation(1)
    assert(ucsBasicLcase.collationName == "UCS_BASIC_LCASE")
    assert(!ucsBasicLcase.isBinaryCollation)

    val unicode = fetchCollation(2)
    assert(unicode.collationName == "UNICODE")
    assert(unicode.isBinaryCollation);

    val unicodeCi = fetchCollation(3)
    assert(unicodeCi.collationName == "UNICODE_CI")
    assert(!unicodeCi.isBinaryCollation)
  }

  test("fetch invalid collation name") {
    val error = intercept[SparkException] {
      fetchCollation("UCS_BASIS")
    }

    assert(error.getErrorClass === "COLLATION_INVALID_NAME")
    assert(error.getMessageParameters.asScala ===
      Map("proposal" -> "UCS_BASIC", "collationName" -> "UCS_BASIS"))
  }

  case class CollationTestCase[R](collationName: String, s1: String, s2: String, expectedResult: R)

  test("collation aware equality and hash") {
    val checks = Seq(
      CollationTestCase("UCS_BASIC", "aaa", "aaa", true),
      CollationTestCase("UCS_BASIC", "aaa", "AAA", false),
      CollationTestCase("UCS_BASIC", "aaa", "bbb", false),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "aaa", true),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "AAA", true),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "AaA", true),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "AaA", true),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "aa", false),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "bbb", false),
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
      CollationTestCase("UCS_BASIC", "aaa", "aaa", 0),
      CollationTestCase("UCS_BASIC", "aaa", "AAA", 1),
      CollationTestCase("UCS_BASIC", "aaa", "bbb", -1),
      CollationTestCase("UCS_BASIC", "aaa", "BBB", 1),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "aaa", 0),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "AAA", 0),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "aa", 1),
      CollationTestCase("UCS_BASIC_LCASE", "aaa", "bbb", -1),
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
}
