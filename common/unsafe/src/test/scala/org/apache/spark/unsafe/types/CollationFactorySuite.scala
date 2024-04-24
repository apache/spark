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
    assert(UTF8_BINARY_COLLATION_ID == 0)

    val utf8Binary = fetchCollation(UTF8_BINARY_COLLATION_ID)
    assert(utf8Binary.collationName == "UTF8_BINARY")
    assert(utf8Binary.supportsBinaryEquality)

    val utf8BinaryLcase = fetchCollation(UTF8_BINARY_LCASE_COLLATION_ID)
    assert(utf8BinaryLcase.collationName == "UTF8_BINARY_LCASE")
    assert(!utf8BinaryLcase.supportsBinaryEquality)

    val unicode = fetchCollation(UNICODE_COLLATION_ID)
    assert(unicode.collationName == "UNICODE")
    assert(unicode.supportsBinaryEquality)

    val unicodeCi = fetchCollation(UNICODE_CI_COLLATION_ID)
    assert(unicodeCi.collationName == "UNICODE_CI")
    assert(!unicodeCi.supportsBinaryEquality)
  }

  test("UTF8_BINARY and ICU root locale collation names") {
    // collation name already normalized
    Seq(
      "UTF8_BINARY",
      "UTF8_BINARY_LCASE",
      "UTF8_BINARY_UCASE",
      "UNICODE",
      "UNICODE_CI",
      "UNICODE_AI",
      "UNICODE_CI_AI",
      "UNICODE_LCASE",
      "UNICODE_UCASE"
    ).foreach(collationName => {
      val col = fetchCollation(collationName)
      assert(col.collationName == collationName)
    })
    // collation name normalization
    Seq(
      // UTF8_BINARY
      ("UTF8_BINARY_CS", "UTF8_BINARY"),
      ("UTF8_BINARY_CI", "UTF8_BINARY"),
      ("UTF8_BINARY_AS", "UTF8_BINARY"),
      ("UTF8_BINARY_AI", "UTF8_BINARY"),
      ("UTF8_BINARY_CS_AS", "UTF8_BINARY"),
      ("UTF8_BINARY_CI_AI", "UTF8_BINARY"),
      ("UTF8_BINARY_LCASE_CS", "UTF8_BINARY_LCASE"),
      ("UTF8_BINARY_CS_LCASE", "UTF8_BINARY_LCASE"),
      // ICU root locale
      ("UNICODE_CS", "UNICODE"),
      ("UNICODE_CS_AS", "UNICODE"),
      ("UNICODE_CI_AS", "UNICODE_CI"),
      ("UNICODE_AI_CS", "UNICODE_AI"),
      ("UNICODE_AI_CI", "UNICODE_CI_AI"),
      ("UNICODE_LCASE_CS", "UNICODE_LCASE"),
      ("UNICODE_LCASE_AS", "UNICODE_LCASE"),
      ("UNICODE_LCASE_CI", "UNICODE_CI_LCASE"),
      ("UNICODE_UCASE_CI", "UNICODE_CI_UCASE"),
      ("UNICODE_AI_UCASE_CI", "UNICODE_CI_AI_UCASE"),
      // randomized case collation names
      ("utf8_binary", "UTF8_BINARY"),
      ("UtF8_binARy_LcasE", "UTF8_BINARY_LCASE"),
      ("unicode", "UNICODE"),
      ("UnICoDe_lcase_cs_aI", "UNICODE_AI_LCASE")
    ).foreach{
      case (name, normalized) =>
        val col = fetchCollation(name)
        assert(col.collationName == normalized)
    }
  }

  test("fetch invalid UTF8_BINARY and ICU root locale collation names") {
    Seq(
      "UTF8_BS",
      "BINARY_UTF8",
      "UTF8_BINARY_A",
      "UNICODE_X",
      "UNICODE_CI_X",
      "UNICODE_LCASE_X",
      "UTF8_UNICODE",
      "UTF8_BINARY_UNICODE",
      "CI_UNICODE",
      "LCASE_UNICODE"
    ).foreach(collationName => {
      val error = intercept[SparkException] {
        fetchCollation(collationName)
      }

      assert(error.getErrorClass === "COLLATION_INVALID_NAME")
      assert(error.getMessageParameters.asScala === Map("collationName" -> collationName))
    })
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

  test("test collation caching") {
    Seq(
      "UTF8_BINARY",
      "UTF8_BINARY_LCASE",
      "UTF8_BINARY_UCASE",
      "UNICODE",
      "UNICODE_LCASE",
      "UNICODE_UCASE",
      "UNICODE_CI",
      "UNICODE_AI_CI",
      "UNICODE_AI_CI_LCASE",
      "UNICODE_AI_CI_UCASE"
    ).foreach(collationId => {
      val col1 = fetchCollation(collationId)
      val col2 = fetchCollation(collationId)
      assert(col1 eq col2) // reference equality
    })
  }

  test("collations with ICU non-root localization") {
    Seq(
      // language only
      "en",
      "en_CS",
      "en_CI",
      "en_AS",
      "en_AI",
      "en_LCASE",
      "en_UCASE",
      // language + 3-letter country code
      "en_USA",
      "en_USA_CS",
      "en_USA_CI",
      "en_USA_AS",
      "en_USA_AI",
      "en_USA_LCASE",
      "en_USA_UCASE",
      // language + script code
      "sr_Cyrl",
      "sr_Cyrl_CS",
      "sr_Cyrl_CI",
      "sr_Cyrl_AS",
      "sr_Cyrl_AI",
      "sr_Cyrl_LCASE",
      "sr_Cyrl_UCASE",
      // language + script code + 3-letter country code
      "sr_Cyrl_SRB",
      "sr_Cyrl_SRB_CS",
      "sr_Cyrl_SRB_CI",
      "sr_Cyrl_SRB_AS",
      "sr_Cyrl_SRB_AI",
      "sr_Cyrl_SRB_LCASE",
      "sr_Cyrl_SRB_UCASE"
    ).foreach(collationICU => {
      val col = fetchCollation(collationICU)
      assert(col.collator.getLocale(ULocale.VALID_LOCALE) != ULocale.ROOT)
    })
  }

  test("invalid names of collations with ICU non-root localization") {
    Seq(
      "en_US", // must use 3-letter country code
      "enn",
      "en_AAA",
      "en_Something",
      "en_Something_USA",
      "en_Latn_USA", // use en_USA instead
      "en_Cyrl_USA",
      "en_USA_AAA",
      "sr_Cyrl_SRB_AAA"
    ).foreach(collationName => {
      val error = intercept[SparkException] {
        fetchCollation(collationName)
      }

      assert(error.getErrorClass === "COLLATION_INVALID_NAME")
      assert(error.getMessageParameters.asScala === Map("collationName" -> collationName))
    })
  }

  test("collations name normalization for ICU non-root localization") {
    Seq(
      ("en_USA", "en_USA"),
      ("en_CS", "en"),
      ("en_AS", "en"),
      ("en_CS_AS", "en"),
      ("en_AI_CI", "en_CI_AI"),
      ("en_USA_AI_CI", "en_USA_CI_AI"),
      // randomized case
      ("EN_USA", "en_USA"),
      ("eN_usA_ci_uCASe_aI", "en_USA_CI_AI_UCASE"),
      ("SR_CYRL", "sr_Cyrl"),
      ("sr_cyrl_srb", "sr_Cyrl_SRB"),
      ("sR_cYRl_sRb", "sr_Cyrl_SRB")
    ).foreach {
      case (name, normalized) =>
        val col = fetchCollation(name)
        assert(col.collationName == normalized)
    }
  }
}
