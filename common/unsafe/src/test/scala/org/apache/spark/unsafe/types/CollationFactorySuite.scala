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

import com.ibm.icu.util.ULocale

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.util.CollationFactory.fetchCollation
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.catalyst.util.CollationFactory._
import org.apache.spark.unsafe.types.UTF8String.{fromString => toUTF8}

class CollationFactorySuite extends AnyFunSuite with Matchers { // scalastyle:ignore funsuite
  test("collationId stability") {
    assert(INDETERMINATE_COLLATION_ID == -1)

    assert(UTF8_BINARY_COLLATION_ID == 0)
    val utf8Binary = fetchCollation(UTF8_BINARY_COLLATION_ID)
    assert(utf8Binary.collationName == "UTF8_BINARY")
    assert(utf8Binary.supportsBinaryEquality)
    assert(utf8Binary.supportsBinaryOrdering)
    assert(!utf8Binary.supportsLowercaseEquality)
    assert(!utf8Binary.supportsTrimming)

    assert(UTF8_BINARY_TRIM_COLLATION_ID == (0 | (1 << 18)))
    val utf8BinaryTrim = fetchCollation(UTF8_BINARY_TRIM_COLLATION_ID)
    assert(utf8BinaryTrim.collationName == "UTF8_BINARY_TRIM")
    assert(!utf8BinaryTrim.supportsBinaryEquality)
    assert(!utf8BinaryTrim.supportsBinaryOrdering)
    assert(!utf8BinaryTrim.supportsLowercaseEquality)
    assert(utf8BinaryTrim.supportsTrimming)

    assert(UTF8_BINARY_LTRIM_COLLATION_ID == (0 | (2 << 18)))
    val utf8BinaryLTrim = fetchCollation(UTF8_BINARY_LTRIM_COLLATION_ID)
    assert(utf8BinaryLTrim.collationName == "UTF8_BINARY_LTRIM")
    assert(!utf8BinaryLTrim.supportsBinaryEquality)
    assert(!utf8BinaryLTrim.supportsBinaryOrdering)
    assert(!utf8BinaryLTrim.supportsLowercaseEquality)
    assert(utf8BinaryLTrim.supportsTrimming)

    assert(UTF8_BINARY_RTRIM_COLLATION_ID == (0 | (3 << 18)))
    val utf8BinaryRTrim = fetchCollation(UTF8_BINARY_RTRIM_COLLATION_ID)
    assert(utf8BinaryRTrim.collationName == "UTF8_BINARY_RTRIM")
    assert(!utf8BinaryRTrim.supportsBinaryEquality)
    assert(!utf8BinaryRTrim.supportsBinaryOrdering)
    assert(!utf8BinaryRTrim.supportsLowercaseEquality)
    assert(utf8BinaryRTrim.supportsTrimming)

    assert(UTF8_LCASE_COLLATION_ID == 1)
    val utf8Lcase = fetchCollation(UTF8_LCASE_COLLATION_ID)
    assert(utf8Lcase.collationName == "UTF8_LCASE")
    assert(!utf8Lcase.supportsBinaryEquality)
    assert(!utf8Lcase.supportsBinaryOrdering)
    assert(utf8Lcase.supportsLowercaseEquality)
    assert(!utf8Lcase.supportsTrimming)

    assert(UTF8_LCASE_TRIM_COLLATION_ID == (1 | (1 << 18)))
    val utf8LcaseTrim = fetchCollation(UTF8_LCASE_TRIM_COLLATION_ID)
    assert(utf8LcaseTrim.collationName == "UTF8_LCASE_TRIM")
    assert(!utf8LcaseTrim.supportsBinaryEquality)
    assert(!utf8LcaseTrim.supportsBinaryOrdering)
    assert(!utf8LcaseTrim.supportsLowercaseEquality)
    assert(utf8LcaseTrim.supportsTrimming)

    assert(UTF8_LCASE_LTRIM_COLLATION_ID == (1 | (2 << 18)))
    val utf8LcaseLTrim = fetchCollation(UTF8_LCASE_LTRIM_COLLATION_ID)
    assert(utf8LcaseLTrim.collationName == "UTF8_LCASE_LTRIM")
    assert(!utf8LcaseLTrim.supportsBinaryEquality)
    assert(!utf8LcaseLTrim.supportsBinaryOrdering)
    assert(!utf8LcaseLTrim.supportsLowercaseEquality)
    assert(utf8LcaseLTrim.supportsTrimming)

    assert(UTF8_LCASE_RTRIM_COLLATION_ID == (1 | (3 << 18)))
    val utf8LcaseRTrim = fetchCollation(UTF8_LCASE_RTRIM_COLLATION_ID)
    assert(utf8LcaseRTrim.collationName == "UTF8_LCASE_RTRIM")
    assert(!utf8LcaseRTrim.supportsBinaryEquality)
    assert(!utf8LcaseRTrim.supportsBinaryOrdering)
    assert(!utf8LcaseRTrim.supportsLowercaseEquality)
    assert(utf8LcaseRTrim.supportsTrimming)

    assert(UNICODE_COLLATION_ID == (1 << 29))
    val unicode = fetchCollation(UNICODE_COLLATION_ID)
    assert(unicode.collationName == "UNICODE")
    assert(!unicode.supportsBinaryEquality)
    assert(!unicode.supportsBinaryOrdering)
    assert(!unicode.supportsLowercaseEquality)
    assert(!unicode.supportsTrimming)

    assert(UNICODE_TRIM_COLLATION_ID == ((1 << 29) | (1 << 18)))
    val unicodeTrim = fetchCollation(UNICODE_TRIM_COLLATION_ID)
    assert(unicodeTrim.collationName == "UNICODE_TRIM")
    assert(!unicodeTrim.supportsBinaryEquality)
    assert(!unicodeTrim.supportsBinaryOrdering)
    assert(!unicodeTrim.supportsLowercaseEquality)
    assert(unicodeTrim.supportsTrimming)

    assert(UNICODE_LTRIM_COLLATION_ID == ((1 << 29) | (2 << 18)))
    val unicodeLTrim = fetchCollation(UNICODE_LTRIM_COLLATION_ID)
    assert(unicodeLTrim.collationName == "UNICODE_LTRIM")
    assert(!unicodeLTrim.supportsBinaryEquality)
    assert(!unicodeLTrim.supportsBinaryOrdering)
    assert(!unicodeLTrim.supportsLowercaseEquality)
    assert(unicodeLTrim.supportsTrimming)

    assert(UNICODE_RTRIM_COLLATION_ID == ((1 << 29) | (3 << 18)))
    val unicodeRTrim = fetchCollation(UNICODE_RTRIM_COLLATION_ID)
    assert(unicodeRTrim.collationName == "UNICODE_RTRIM")
    assert(!unicodeRTrim.supportsBinaryEquality)
    assert(!unicodeRTrim.supportsBinaryOrdering)
    assert(!unicodeRTrim.supportsLowercaseEquality)
    assert(unicodeRTrim.supportsTrimming)

    assert(UNICODE_CI_COLLATION_ID == ((1 << 29) | (1 << 17)))
    val unicodeCi = fetchCollation(UNICODE_CI_COLLATION_ID)
    assert(unicodeCi.collationName == "UNICODE_CI")
    assert(!unicodeCi.supportsBinaryEquality)
    assert(!unicodeCi.supportsBinaryOrdering)
    assert(!unicodeCi.supportsLowercaseEquality)
    assert(!unicodeCi.supportsTrimming)

    assert(UNICODE_CI_TRIM_COLLATION_ID == ((1 << 29) | (1 << 17) | (1 << 18)))
    val unicodeCiTrim = fetchCollation(UNICODE_CI_TRIM_COLLATION_ID)
    assert(unicodeCiTrim.collationName == "UNICODE_CI_TRIM")
    assert(!unicodeCiTrim.supportsBinaryEquality)
    assert(!unicodeCiTrim.supportsBinaryOrdering)
    assert(!unicodeCiTrim.supportsLowercaseEquality)
    assert(unicodeCiTrim.supportsTrimming)

    assert(UNICODE_CI_LTRIM_COLLATION_ID == ((1 << 29) | (1 << 17) | (2 << 18)))
    val unicodeCiLTrim = fetchCollation(UNICODE_CI_LTRIM_COLLATION_ID)
    assert(unicodeCiLTrim.collationName == "UNICODE_CI_LTRIM")
    assert(!unicodeCiLTrim.supportsBinaryEquality)
    assert(!unicodeCiLTrim.supportsBinaryOrdering)
    assert(!unicodeCiLTrim.supportsLowercaseEquality)
    assert(unicodeCiLTrim.supportsTrimming)

    assert(UNICODE_CI_RTRIM_COLLATION_ID == ((1 << 29) | (1 << 17) | (3 << 18)))
    val unicodeCiRTrim = fetchCollation(UNICODE_CI_RTRIM_COLLATION_ID)
    assert(unicodeCiRTrim.collationName == "UNICODE_CI_RTRIM")
    assert(!unicodeCiRTrim.supportsBinaryEquality)
    assert(!unicodeCiRTrim.supportsBinaryOrdering)
    assert(!unicodeCiRTrim.supportsLowercaseEquality)
    assert(unicodeCiRTrim.supportsTrimming)
  }

  test("UTF8_BINARY and ICU root locale collation names") {
    // Collation name already normalized.
    Seq(
      "UTF8_BINARY",
      "UTF8_BINARY_TRIM",
      "UTF8_BINARY_LTRIM",
      "UTF8_BINARY_RTRIM",
      "UTF8_LCASE",
      "UTF8_LCASE_TRIM",
      "UTF8_LCASE_LTRIM",
      "UTF8_LCASE_RTRIM",
      "UNICODE",
      "UNICODE_TRIM",
      "UNICODE_LTRIM",
      "UNICODE_RTRIM",
      "UNICODE_CI",
      "UNICODE_CI_TRIM",
      "UNICODE_CI_LTRIM",
      "UNICODE_CI_RTRIM",
      "UNICODE_AI",
      "UNICODE_AI_TRIM",
      "UNICODE_AI_LTRIM",
      "UNICODE_AI_RTRIM",
      "UNICODE_CI_AI",
      "UNICODE_CI_AI_TRIM",
      "UNICODE_CI_AI_LTRIM",
      "UNICODE_CI_AI_RTRIM"
    ).foreach(collationName => {
      val col = fetchCollation(collationName)
      assert(col.collationName == collationName)
    })
    // Collation name normalization.
    Seq(
      // ICU root locale.
      ("UNICODE_CS", "UNICODE"),
      ("UNICODE_CS_AS", "UNICODE"),
      ("UNICODE_CI_AS", "UNICODE_CI"),
      ("UNICODE_AI_CS", "UNICODE_AI"),
      ("UNICODE_AI_CI", "UNICODE_CI_AI"),
      // Randomized case collation names.
      ("utf8_binary", "UTF8_BINARY"),
      ("utf8_binary_trim", "UTF8_BINARY_TRIM"),
      ("UtF8_LcasE", "UTF8_LCASE"),
      ("UtF8_LcasE_TrIm", "UTF8_LCASE_TRIM"),
      ("unicode", "UNICODE"),
      ("UnICoDe_cs_aI", "UNICODE_AI")
    ).foreach{
      case (name, normalized) =>
        val col = fetchCollation(name)
        assert(col.collationName == normalized)
    }
  }

  test("fetch invalid UTF8_BINARY and ICU root locale collation names") {
    Seq(
      ("UTF8_BINARY_CS", "UTF8_BINARY"),
      ("UTF8_BINARY_AS", "UTF8_BINARY"), // this should be UNICODE_AS
      ("UTF8_BINARY_CS_AS","UTF8_BINARY"), // this should be UNICODE_CS_AS
      ("UTF8_BINARY_AS_CS","UTF8_BINARY"),
      ("UTF8_BINARY_CI","UTF8_BINARY"),
      ("UTF8_BINARY_AI","UTF8_BINARY"),
      ("UTF8_BINARY_CI_AI","UTF8_BINARY"),
      ("UTF8_BINARY_AI_CI","UTF8_BINARY"),
      ("UTF8_TRIM_BINARY", "UTF8_BINARY"),
      ("UTF8_LCASE_TRIMBOTH", "UTF8_LCASE"),
      ("UTF8_BS","UTF8_LCASE"),
      ("BINARY_UTF8","ar_SAU"),
      ("UTF8_BINARY_A","UTF8_BINARY"),
      ("UNICODE_X","UNICODE"),
      ("UNICODE_CI_X","UNICODE"),
      ("UNICODE_TRIM_CI","UNICODE_CI"),
      ("UNICODE_TRIM_AI_CI","UNICODE_AI_CI"),
      ("UNICODE_TRIM_CI_AI", "UNICODE_CI_AI"),
      ("UNICODE_LCASE_X","UNICODE"),
      ("UTF8_UNICODE","UTF8_LCASE"),
      ("UTF8_BINARY_UNICODE","UTF8_BINARY"),
      ("CI_UNICODE", "UNICODE"),
      ("LCASE_UNICODE", "UNICODE"),
      ("UNICODE_UNSPECIFIED", "UNICODE"),
      ("UNICODE_CI_UNSPECIFIED", "UNICODE"),
      ("UNICODE_UNSPECIFIED_CI_UNSPECIFIED", "UNICODE"),
      ("UNICODE_INDETERMINATE", "UNICODE"),
      ("UNICODE_CI_INDETERMINATE", "UNICODE")
    ).foreach{case (collationName, proposals) =>
      checkCollationNameError(collationName, proposals)
    }
  }

  case class CollationTestCase[R](collationName: String, s1: String, s2: String, expectedResult: R)

  test("collation aware equality and hash") {
    val checks = Seq(
      CollationTestCase("UTF8_BINARY", "aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY", "aaa", "AAA", false),
      CollationTestCase("UTF8_BINARY", "aaa", "bbb", false),
      CollationTestCase("UTF8_BINARY", "å", "a\u030A", false),
      CollationTestCase("UTF8_BINARY_TRIM", "aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY_TRIM", " aaa ", "aaa", true),
      CollationTestCase("UTF8_BINARY_TRIM", " aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY_TRIM", "aaa ", "aaa", true),
      CollationTestCase("UTF8_BINARY_LTRIM", "aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY_LTRIM", " aaa ", "aaa", false),
      CollationTestCase("UTF8_BINARY_LTRIM", " aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY_LTRIM", "aaa ", "aaa", false),
      CollationTestCase("UTF8_BINARY_RTRIM", "aaa", "aaa", true),
      CollationTestCase("UTF8_BINARY_RTRIM", " aaa ", "aaa", false),
      CollationTestCase("UTF8_BINARY_RTRIM", " aaa", "aaa", false),
      CollationTestCase("UTF8_BINARY_RTRIM", "aaa ", "aaa", true),
      CollationTestCase("UTF8_LCASE", "aaa", "aaa", true),
      CollationTestCase("UTF8_LCASE", "aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", true),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", true),
      CollationTestCase("UTF8_LCASE", "aaa", "aa", false),
      CollationTestCase("UTF8_LCASE", "aaa", "bbb", false),
      CollationTestCase("UTF8_LCASE", "å", "a\u030A", false),
      CollationTestCase("UTF8_LCASE_TRIM", "aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE_TRIM", " aaa ", "AAA", true),
      CollationTestCase("UTF8_LCASE_TRIM", " aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE_TRIM", "aaa ", "AAA", true),
      CollationTestCase("UTF8_LCASE_LTRIM", "aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE_LTRIM", " aaa ", "AAA", false),
      CollationTestCase("UTF8_LCASE_LTRIM", " aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE_LTRIM", "aaa ", "AAA", false),
      CollationTestCase("UTF8_LCASE_RTRIM", "aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE_RTRIM", " aaa ", "AAA", false),
      CollationTestCase("UTF8_LCASE_RTRIM", " aaa", "AAA", false),
      CollationTestCase("UTF8_LCASE_RTRIM", "aaa ", "AAA", true),
      CollationTestCase("UNICODE", "aaa", "aaa", true),
      CollationTestCase("UNICODE", "aaa", "AAA", false),
      CollationTestCase("UNICODE", "aaa", "bbb", false),
      CollationTestCase("UNICODE", "å", "a\u030A", true),
      CollationTestCase("UNICODE_TRIM", "å", "a\u030A", true),
      CollationTestCase("UNICODE_TRIM", " å ", "a\u030A", true),
      CollationTestCase("UNICODE_TRIM", " å", "a\u030A", true),
      CollationTestCase("UNICODE_TRIM", "å ", "a\u030A", true),
      CollationTestCase("UNICODE_LTRIM", "å", "a\u030A", true),
      CollationTestCase("UNICODE_LTRIM", " å ", "a\u030A", false),
      CollationTestCase("UNICODE_LTRIM", " å", "a\u030A", true),
      CollationTestCase("UNICODE_LTRIM", "å ", "a\u030A", false),
      CollationTestCase("UNICODE_RTRIM", "å", "a\u030A", true),
      CollationTestCase("UNICODE_RTRIM", " å ", "a\u030A", false),
      CollationTestCase("UNICODE_RTRIM", " å", "a\u030A", false),
      CollationTestCase("UNICODE_RTRIM", "å ", "a\u030A", true),
      CollationTestCase("UNICODE_CI", "aaa", "aaa", true),
      CollationTestCase("UNICODE_CI", "aaa", "AAA", true),
      CollationTestCase("UNICODE_CI", "aaa", "bbb", false),
      CollationTestCase("UNICODE_CI", "å", "a\u030A", true),
      CollationTestCase("UNICODE_CI", "Å", "a\u030A", true),
      CollationTestCase("UNICODE_CI_TRIM", "Å", "a\u030A", true),
      CollationTestCase("UNICODE_CI_TRIM", " Å ", "a\u030A", true),
      CollationTestCase("UNICODE_CI_TRIM", " Å", "a\u030A", true),
      CollationTestCase("UNICODE_CI_TRIM", "Å ", "a\u030A", true),
      CollationTestCase("UNICODE_CI_LTRIM", "Å", "a\u030A", true),
      CollationTestCase("UNICODE_CI_LTRIM", " Å ", "a\u030A", false),
      CollationTestCase("UNICODE_CI_LTRIM", " Å", "a\u030A", true),
      CollationTestCase("UNICODE_CI_LTRIM", "Å ", "a\u030A", false),
      CollationTestCase("UNICODE_CI_RTRIM", "Å", "a\u030A", true),
      CollationTestCase("UNICODE_CI_RTRIM", " Å ", "a\u030A", false),
      CollationTestCase("UNICODE_CI_RTRIM", " Å", "a\u030A", false),
      CollationTestCase("UNICODE_CI_RTRIM", "Å ", "a\u030A", true)
    )

    checks.foreach(testCase => {
      val collation = fetchCollation(testCase.collationName)
      assert(collation.equalsFunction(toUTF8(testCase.s1), toUTF8(testCase.s2)) ==
        testCase.expectedResult)

      val hash1 = collation.hashFunction.applyAsLong(toUTF8(testCase.s1))
      val hash2 = collation.hashFunction.applyAsLong(toUTF8(testCase.s2))
      assert((hash1 == hash2) == testCase.expectedResult)
    })
  }

  test("collation aware compare") {
    val checks = Seq(
      CollationTestCase("UTF8_BINARY", "aaa", "aaa", 0),
      CollationTestCase("UTF8_BINARY", "aaa", "AAA", 1),
      CollationTestCase("UTF8_BINARY", "aaa", "bbb", -1),
      CollationTestCase("UTF8_BINARY", "aaa", "BBB", 1),
      CollationTestCase("UTF8_BINARY_TRIM", " aaa ", "aaa", 0),
      CollationTestCase("UTF8_BINARY_TRIM", " aaa ", "AAA", 1),
      CollationTestCase("UTF8_BINARY_TRIM", " aaa ", "bbb", -1),
      CollationTestCase("UTF8_BINARY_TRIM", " aaa ", "BBB", 1),
      CollationTestCase("UTF8_BINARY_LTRIM", " aaa ", "aaa", 1),
      CollationTestCase("UTF8_BINARY_LTRIM", " aaa", "aaa", 0),
      CollationTestCase("UTF8_BINARY_LTRIM", " aaa", "AAA", 1),
      CollationTestCase("UTF8_BINARY_LTRIM", " aaa", "bbb", -1),
      CollationTestCase("UTF8_BINARY_RTRIM", " aaa ", "aaa", -1),
      CollationTestCase("UTF8_BINARY_RTRIM", "aaa ", "aaa", 0),
      CollationTestCase("UTF8_BINARY_RTRIM", "aaa ", "AAA", 1),
      CollationTestCase("UTF8_BINARY_RTRIM", "aaa ", "bbb", -1),
      CollationTestCase("UTF8_LCASE", "aaa", "aaa", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "AAA", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "aa", 1),
      CollationTestCase("UTF8_LCASE", "aaa", "bbb", -1),
      CollationTestCase("UTF8_LCASE_TRIM", " aaa ", "aaa", 0),
      CollationTestCase("UTF8_LCASE_TRIM", " aaa ", "AAA", 0),
      CollationTestCase("UTF8_LCASE_TRIM", " aaa ", "AA", 1),
      CollationTestCase("UTF8_LCASE_TRIM", " aaa ", "BBB", -1),
      CollationTestCase("UTF8_LCASE_LTRIM", " aaa ", "AAA", 1),
      CollationTestCase("UTF8_LCASE_LTRIM", " aaa", "AAA", 0),
      CollationTestCase("UTF8_LCASE_LTRIM", " aaa", "AA", 1),
      CollationTestCase("UTF8_LCASE_LTRIM", " aaa", "BBB", -1),
      CollationTestCase("UTF8_LCASE_RTRIM", " aaa ", "AAA", -1),
      CollationTestCase("UTF8_LCASE_RTRIM", "aaa ", "AAA", 0),
      CollationTestCase("UTF8_LCASE_RTRIM", "aaa ", "AA", 1),
      CollationTestCase("UTF8_LCASE_RTRIM", "aaa ", "BBB", -1),
      CollationTestCase("UNICODE", "aaa", "aaa", 0),
      CollationTestCase("UNICODE", "aaa", "AAA", -1),
      CollationTestCase("UNICODE", "aaa", "bbb", -1),
      CollationTestCase("UNICODE", "aaa", "BBB", -1),
      CollationTestCase("UNICODE_TRIM", " aaa ", "aaa", 0),
      CollationTestCase("UNICODE_TRIM", " aaa ", "AAA", -1),
      CollationTestCase("UNICODE_TRIM", " aaa ", "bbb", -1),
      CollationTestCase("UNICODE_TRIM", " aaa ", "BBB", -1),
      CollationTestCase("UNICODE_LTRIM", " aaa ", "aaa", 1),
      CollationTestCase("UNICODE_LTRIM", " aaa", "aaa", 0),
      CollationTestCase("UNICODE_LTRIM", " aaa", "AAA", -1),
      CollationTestCase("UNICODE_LTRIM", " aaa", "bbb", -1),
      CollationTestCase("UNICODE_RTRIM", " aaa ", "aaa", -1),
      CollationTestCase("UNICODE_RTRIM", "aaa ", "aaa", 0),
      CollationTestCase("UNICODE_RTRIM", "aaa ", "AAA", -1),
      CollationTestCase("UNICODE_RTRIM", "aaa ", "bbb", -1),
      CollationTestCase("UNICODE_CI", "aaa", "aaa", 0),
      CollationTestCase("UNICODE_CI", "aaa", "AAA", 0),
      CollationTestCase("UNICODE_CI", "aaa", "bbb", -1),
      CollationTestCase("UNICODE_CI", "aaa", "BBB", -1),
      CollationTestCase("UNICODE_CI_TRIM", " aaa ", "aaa", 0),
      CollationTestCase("UNICODE_CI_TRIM", " aaa ", "AAA", 0),
      CollationTestCase("UNICODE_CI_TRIM", " aaa ", "AA", 1),
      CollationTestCase("UNICODE_CI_TRIM", " aaa ", "BBB", -1),
      CollationTestCase("UNICODE_CI_LTRIM", " aaa ", "AAA", 1),
      CollationTestCase("UNICODE_CI_LTRIM", " aaa", "AAA", 0),
      CollationTestCase("UNICODE_CI_LTRIM", " aaa", "AA", 1),
      CollationTestCase("UNICODE_CI_LTRIM", " aaa", "BBB", -1),
      CollationTestCase("UNICODE_CI_RTRIM", " aaa ", "AAA", -1),
      CollationTestCase("UNICODE_CI_RTRIM", "aaa ", "AAA", 0),
      CollationTestCase("UNICODE_CI_RTRIM", "aaa ", "AA", 1),
      CollationTestCase("UNICODE_CI_RTRIM", "aaa ", "BBB", -1),
    )

    checks.foreach(testCase => {
      val collation = fetchCollation(testCase.collationName)
      val result = collation.comparator.compare(toUTF8(testCase.s1), toUTF8(testCase.s2))
      assert(Integer.signum(result) == testCase.expectedResult)
    })
  }

  test("collation aware string search") {
    val checks = Seq(
      CollationTestCase("UNICODE_CI", "abcde", "", (0, 0)),
      CollationTestCase("UNICODE_CI", "abcde", "abc", (0, 3)),
      CollationTestCase("UNICODE_CI", "abcde", "C", (2, 1)),
      CollationTestCase("UNICODE_CI", "abcde", "dE", (3, 2)),
      CollationTestCase("UNICODE_CI", "abcde", "abcde", (0, 5)),
      CollationTestCase("UNICODE_CI", "abcde", "ABCDE", (0, 5)),
      CollationTestCase("UNICODE_CI", "abcde", "fgh", (0, 0)),
      CollationTestCase("UNICODE_CI", "abcde", "FGH", (0, 0))
    )

    checks.foreach(testCase => {
      val collationId = collationNameToId(testCase.collationName)
      val stringSearch = getStringSearch(toUTF8(testCase.s1), toUTF8(testCase.s2), collationId)
      val (expectedIndex, expectedLength) = testCase.expectedResult
      var (index, length) = (0, 0)
      while (index != -1 && length == 0) {
        if (stringSearch.getMatchLength == stringSearch.getPattern.length()) {
          length = stringSearch.getMatchLength
          assert((index, length) == (expectedIndex, expectedLength))
        }
        index = stringSearch.next()
      }
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

  test("test collation caching") {
    Seq(
      "UTF8_BINARY",
      "UTF8_BINARY_TRIM",
      "UTF8_BINARY_LTRIM",
      "UTF8_BINARY_RTRIM",
      "UTF8_LCASE",
      "UTF8_LCASE_TRIM",
      "UTF8_LCASE_LTRIM",
      "UTF8_LCASE_RTRIM",
      "UNICODE",
      "UNICODE_TRIM",
      "UNICODE_LTRIM",
      "UNICODE_RTRIM",
      "UNICODE_CI",
      "UNICODE_CI_TRIM",
      "UNICODE_CI_LTRIM",
      "UNICODE_CI_RTRIM",
      "UNICODE_AI",
      "UNICODE_AI_TRIM",
      "UNICODE_AI_LTRIM",
      "UNICODE_AI_RTRIM",
      "UNICODE_CI_AI",
      "UNICODE_CI_AI_TRIM",
      "UNICODE_CI_AI_LTRIM",
      "UNICODE_CI_AI_RTRIM",
      "UNICODE_AI_CI",
      "UNICODE_AI_CI_TRIM",
      "UNICODE_AI_CI_LTRIM",
      "UNICODE_AI_CI_RTRIM"
    ).foreach(collationId => {
      val col1 = fetchCollation(collationId)
      val col2 = fetchCollation(collationId)
      assert(col1 eq col2) // Check for reference equality.
    })
  }

  test("collations with ICU non-root localization") {
    Seq(
      // Language only.
      "en",
      "en_CS",
      "en_CI",
      "en_AS",
      "en_AI",
      // Language + 3-letter country code.
      "en_USA",
      "en_USA_CS",
      "en_USA_CI",
      "en_USA_AS",
      "en_USA_AI",
      // Language + script code.
      "sr_Cyrl",
      "sr_Cyrl_CS",
      "sr_Cyrl_CI",
      "sr_Cyrl_AS",
      "sr_Cyrl_AI",
      // Language + script code + 3-letter country code.
      "sr_Cyrl_SRB",
      "sr_Cyrl_SRB_CS",
      "sr_Cyrl_SRB_CI",
      "sr_Cyrl_SRB_AS",
      "sr_Cyrl_SRB_AI",
      // Language + script code + 3-letter country code + trim sensitivity.
      "sr_Cyrl_SRB_TRIM",
      "sr_Cyrl_SRB_CS_TRIM",
      "sr_Cyrl_SRB_CI_TRIM",
      "sr_Cyrl_SRB_AS_TRIM",
      "sr_Cyrl_SRB_AI_TRIM",
      "sr_Cyrl_SRB_CS_LTRIM",
      "sr_Cyrl_SRB_CI_LTRIM",
      "sr_Cyrl_SRB_AS_LTRIM",
      "sr_Cyrl_SRB_AI_LTRIM",
      "sr_Cyrl_SRB_CS_RTRIM",
      "sr_Cyrl_SRB_CI_RTRIM",
      "sr_Cyrl_SRB_AS_RTRIM",
      "sr_Cyrl_SRB_AI_RTRIM"
    ).foreach(collationICU => {
      val col = fetchCollation(collationICU)
      assert(col.collator.getLocale(ULocale.VALID_LOCALE) != ULocale.ROOT)
      assert(col.supportsTrimming == collationICU.contains("TRIM"))
    })
  }

  test("invalid names of collations with ICU non-root localization") {
    Seq(
      ("en_US", "en_USA"), // Must use 3-letter country code
      ("eN_US", "en_USA"), // verify that proper casing is captured in error.
      ("enn", "en, nn, bn"),
      ("en_AAA", "en_USA"),
      ("en_Something", "UNICODE"),
      ("en_Something_USA", "en_USA"),
      ("en_LCASE", "en_USA"),
      ("en_UCASE", "en_USA"),
      ("en_CI_LCASE", "UNICODE"),
      ("en_CI_UCASE", "en_USA"),
      ("en_CI_UNSPECIFIED", "en_USA"),
      ("en_USA_UNSPECIFIED", "en_USA"),
      ("en_USA_UNSPECIFIED_CI", "en_USA_CI"),
      ("en_INDETERMINATE", "en_USA"),
      ("en_USA_INDETERMINATE", "en_USA"),
      ("en_Latn_USA", "en_USA"),
      ("en_Cyrl_USA", "en_USA"),
      ("en_USA_AAA", "en_USA"),
      ("sr_Cyrl_SRB_AAA", "sr_Cyrl_SRB"),
      // Invalid ordering of language, script and country code.
      ("USA_en", "en"),
      ("sr_SRB_Cyrl", "sr_Cyrl"),
      ("SRB_sr", "ar_SAU"),
      ("SRB_sr_Cyrl", "bs_Cyrl"),
      ("SRB_Cyrl_sr", "sr_Cyrl_SRB"),
      ("Cyrl_sr", "sr_Cyrl_SRB"),
      ("Cyrl_sr_SRB", "sr_Cyrl_SRB"),
      ("Cyrl_SRB_sr", "sr_Cyrl_SRB"),
      // Invalid ordering of trim sensitivity with respect to language, script and country code.
      ("sr_Cyrl_TRIM_SRB", "sr_Cyrl_SRB"),
      ("sr_Cyrl_SRB_TRIM_CS", "sr_Cyrl_SRB_CS"),
      ("sr_Cyrl_TRIM_SRB_CI", "sr_Cyrl_SRB_CI"),
      ("sr_TRIM_Cyrl_SRB_AS", "sr_Cyrl_SRB_AS"),
      ("TRIM_sr_Cyrl_SRB_AI", "sr_Cyrl_SRB_AI"),
      // Collation specifiers in the middle of locale.
      ("CI_en", "ceb"),
      ("USA_CI_en", "UNICODE"),
      ("en_CI_USA", "en_USA"),
      ("CI_sr_Cyrl_SRB", "sr_Cyrl_SRB"),
      ("sr_CI_Cyrl_SRB", "sr_Cyrl_SRB"),
      ("sr_Cyrl_CI_SRB", "sr_Cyrl_SRB"),
      ("CI_Cyrl_sr", "sr_Cyrl_SRB"),
      ("Cyrl_CI_sr", "he_ISR"),
      ("Cyrl_CI_sr_SRB", "sr_Cyrl_SRB"),
      ("Cyrl_sr_CI_SRB", "sr_Cyrl_SRB"),
      // no locale specified
      ("_CI_AI", "af_CI_AI, am_CI_AI, ar_CI_AI"),
      ("", "af, am, ar")
    ).foreach { case (collationName, proposals) =>
      checkCollationNameError(collationName, proposals)
    }
  }

  test("collations name normalization for ICU non-root localization") {
    Seq(
      ("en_USA", "en_USA"),
      ("en_CS", "en"),
      ("en_AS", "en"),
      ("en_CS_AS", "en"),
      ("en_AS_CS", "en"),
      ("en_CI", "en_CI"),
      ("en_AI", "en_AI"),
      ("en_AI_CI", "en_CI_AI"),
      ("en_CI_AI", "en_CI_AI"),
      ("en_CS_AI", "en_AI"),
      ("en_AI_CS", "en_AI"),
      ("en_CI_AS", "en_CI"),
      ("en_AS_CI", "en_CI"),
      ("en_USA_AI_CI", "en_USA_CI_AI"),
      // Randomized case.
      ("EN_USA", "en_USA"),
      ("SR_CYRL", "sr_Cyrl"),
      ("sr_cyrl_srb", "sr_Cyrl_SRB"),
      ("sR_cYRl_sRb", "sr_Cyrl_SRB")
    ).foreach {
      case (name, normalized) =>
        val col = fetchCollation(name)
        assert(col.collationName == normalized)
    }
  }

  test("invalid collationId") {
    val badCollationIds = Seq(
      INDETERMINATE_COLLATION_ID, // Indeterminate collation.
      1 << 30, // User-defined collation range.
      (1 << 30) | 1, // User-defined collation range.
      (1 << 30) | (1 << 29), // User-defined collation range.
      1 << 1, // UTF8_BINARY mandatory zero bit 1 breach.
      1 << 2, // UTF8_BINARY mandatory zero bit 2 breach.
      1 << 3, // UTF8_BINARY mandatory zero bit 3 breach.
      1 << 4, // UTF8_BINARY mandatory zero bit 4 breach.
      1 << 5, // UTF8_BINARY mandatory zero bit 5 breach.
      1 << 6, // UTF8_BINARY mandatory zero bit 6 breach.
      1 << 7, // UTF8_BINARY mandatory zero bit 7 breach.
      1 << 8, // UTF8_BINARY mandatory zero bit 8 breach.
      1 << 9, // UTF8_BINARY mandatory zero bit 9 breach.
      1 << 10, // UTF8_BINARY mandatory zero bit 10 breach.
      1 << 11, // UTF8_BINARY mandatory zero bit 11 breach.
      1 << 12, // UTF8_BINARY mandatory zero bit 12 breach.
      1 << 13, // UTF8_BINARY mandatory zero bit 13 breach.
      1 << 14, // UTF8_BINARY mandatory zero bit 14 breach.
      1 << 15, // UTF8_BINARY mandatory zero bit 15 breach.
      1 << 16, // UTF8_BINARY mandatory zero bit 16 breach.
      1 << 17, // UTF8_BINARY mandatory zero bit 17 breach.
      1 << 23, // UTF8_BINARY mandatory zero bit 23 breach.
      1 << 24, // UTF8_BINARY mandatory zero bit 24 breach.
      1 << 25, // UTF8_BINARY mandatory zero bit 25 breach.
      1 << 26, // UTF8_BINARY mandatory zero bit 26 breach.
      1 << 27, // UTF8_BINARY mandatory zero bit 27 breach.
      1 << 28, // UTF8_BINARY mandatory zero bit 28 breach.
      (1 << 29) | (1 << 12), // ICU mandatory zero bit 12 breach.
      (1 << 29) | (1 << 13), // ICU mandatory zero bit 13 breach.
      (1 << 29) | (1 << 14), // ICU mandatory zero bit 14 breach.
      (1 << 29) | (1 << 15), // ICU mandatory zero bit 15 breach.
      (1 << 29) | (1 << 22), // ICU mandatory zero bit 22 breach.
      (1 << 29) | (1 << 23), // ICU mandatory zero bit 23 breach.
      (1 << 29) | (1 << 24), // ICU mandatory zero bit 24 breach.
      (1 << 29) | (1 << 25), // ICU mandatory zero bit 25 breach.
      (1 << 29) | (1 << 26), // ICU mandatory zero bit 26 breach.
      (1 << 29) | (1 << 27), // ICU mandatory zero bit 27 breach.
      (1 << 29) | (1 << 28), // ICU mandatory zero bit 28 breach.
      (1 << 29) | 0xFFFF // ICU with invalid locale id.
    )
    badCollationIds.foreach(collationId => {
      // Assumptions about collation id will break and assert statement will fail.
      intercept[AssertionError](fetchCollation(collationId))
    })
  }

  test("repeated and/or incompatible and/or misplaced specifiers in collation name") {
    Seq(
      ("UTF8_LCASE_LCASE", "UTF8_LCASE"),
      ("UTF8_TRIM_TRIM", "UTF8_LCASE_TRIM"),
      ("UNICODE_CS_CS", "UNICODE_CS"),
      ("UNICODE_CI_CI", "UNICODE_CI"),
      ("UNICODE_CI_CS", "UNICODE_CS"),
      ("UNICODE_CS_CI", "UNICODE_CS"),
      ("UNICODE_AS_AS", "UNICODE_AS"),
      ("UNICODE_AI_AI", "UNICODE_AI"),
      ("UNICODE_AS_AI", "UNICODE_AS"),
      ("UNICODE_AI_AS", "UNICODE_AS"),
//      ("UNICODE_TRIM_AS", "UNICODE_AS"),
//      ("TRIM_UNICODE_AS", "UNICODE_AS"),
      ("UNICODE_AS_CS_AI", "UNICODE_AS_CS"),
      ("UNICODE_CS_AI_CI", "UNICODE_CS_AI"),
      ("UNICODE_CS_AS_CI_AI", "UNICODE_CS_AS"),
      ("UNICODE__CS__AS", "UNICODE_AS"),
      ("UNICODE-CS-AS", "UNICODE"),
      ("UNICODECSAS", "UNICODE"),
//      ("UNICODETRIM", "UNICODE"),
//      ("_CS_AS_UNICODE", "UNICODE"),
//      ("_UNICODE_TRIM", "UNICODE")
    ).foreach { case (collationName, proposals) =>
      checkCollationNameError(collationName, proposals)
    }
  }

  test("basic ICU collator checks") {
    Seq(
      CollationTestCase("UNICODE_CI", "a", "A", true),
      CollationTestCase("UNICODE_CI", "a", "å", false),
      CollationTestCase("UNICODE_CI", "a", "Å", false),
      CollationTestCase("UNICODE_CI_TRIM", " a ", "A", true),
      CollationTestCase("UNICODE_AI", "a", "A", false),
      CollationTestCase("UNICODE_AI", "a", "å", true),
      CollationTestCase("UNICODE_AI", "a", "Å", false),
      CollationTestCase("UNICODE_AI_LTRIM", " a", "å", true),
      CollationTestCase("UNICODE_CI_AI", "a", "A", true),
      CollationTestCase("UNICODE_CI_AI", "a", "å", true),
      CollationTestCase("UNICODE_CI_AI", "a", "Å", true),
      CollationTestCase("UNICODE_CI_AI_RTRIM", "a ", "Å", true)
    ).foreach(testCase => {
      val collation = fetchCollation(testCase.collationName)
      assert(collation.equalsFunction(toUTF8(testCase.s1), toUTF8(testCase.s2)) ==
        testCase.expectedResult)
    })
    Seq(
      CollationTestCase("en", "a", "A", -1),
      CollationTestCase("en_CI", "a", "A", 0),
      CollationTestCase("en_AI", "a", "å", 0),
      CollationTestCase("sv", "Kypper", "Köpfe", -1),
      CollationTestCase("de", "Kypper", "Köpfe", 1)
    ).foreach(testCase => {
      val collation = fetchCollation(testCase.collationName)
      val result = collation.comparator.compare(toUTF8(testCase.s1), toUTF8(testCase.s2))
      assert(Integer.signum(result) == testCase.expectedResult)
    })
  }

  private def checkCollationNameError(collationName: String, proposals: String): Unit = {
    val e = intercept[SparkException] {
      fetchCollation(collationName)
    }
    assert(e.getErrorClass === "COLLATION_INVALID_NAME")
    assert(e.getMessageParameters.asScala === Map(
      "collationName" -> collationName, "proposals" -> proposals))
  }
}
