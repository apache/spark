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

    assert(UTF8_LCASE_COLLATION_ID == 1)
    val utf8Lcase = fetchCollation(UTF8_LCASE_COLLATION_ID)
    assert(utf8Lcase.collationName == "UTF8_LCASE")
    assert(!utf8Lcase.supportsBinaryEquality)

    assert(UNICODE_COLLATION_ID == (1 << 29))
    val unicode = fetchCollation(UNICODE_COLLATION_ID)
    assert(unicode.collationName == "UNICODE")
    assert(!unicode.supportsBinaryEquality)

    assert(UNICODE_CI_COLLATION_ID == ((1 << 29) | (1 << 17)))
    val unicodeCi = fetchCollation(UNICODE_CI_COLLATION_ID)
    assert(unicodeCi.collationName == "UNICODE_CI")
    assert(!unicodeCi.supportsBinaryEquality)
  }

  test("UTF8_BINARY and ICU root locale collation names") {
    // Collation name already normalized.
    Seq(
      "UTF8_BINARY",
      "UTF8_LCASE",
      "UNICODE",
      "UNICODE_CI",
      "UNICODE_AI",
      "UNICODE_CI_AI"
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
      ("UtF8_LcasE", "UTF8_LCASE"),
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
      ("UTF8_BS","UTF8_LCASE"),
      ("BINARY_UTF8","ar_SAU"),
      ("UTF8_BINARY_A","UTF8_BINARY"),
      ("UNICODE_X","UNICODE"),
      ("UNICODE_CI_X","UNICODE"),
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
      CollationTestCase("UTF8_LCASE", "aaa", "aaa", true),
      CollationTestCase("UTF8_LCASE", "aaa", "AAA", true),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", true),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", true),
      CollationTestCase("UTF8_LCASE", "aaa", "aa", false),
      CollationTestCase("UTF8_LCASE", "aaa", "bbb", false),
      CollationTestCase("UTF8_LCASE", "å", "a\u030A", false),
      CollationTestCase("UNICODE", "aaa", "aaa", true),
      CollationTestCase("UNICODE", "aaa", "AAA", false),
      CollationTestCase("UNICODE", "aaa", "bbb", false),
      CollationTestCase("UNICODE", "å", "a\u030A", true),
      CollationTestCase("UNICODE_CI", "aaa", "aaa", true),
      CollationTestCase("UNICODE_CI", "aaa", "AAA", true),
      CollationTestCase("UNICODE_CI", "aaa", "bbb", false),
      CollationTestCase("UNICODE_CI", "å", "a\u030A", true),
      CollationTestCase("UNICODE_CI", "Å", "a\u030A", true)
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
      CollationTestCase("UTF8_LCASE", "aaa", "aaa", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "AAA", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "AaA", 0),
      CollationTestCase("UTF8_LCASE", "aaa", "aa", 1),
      CollationTestCase("UTF8_LCASE", "aaa", "bbb", -1),
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
      "UTF8_LCASE",
      "UNICODE",
      "UNICODE_CI",
      "UNICODE_AI",
      "UNICODE_CI_AI",
      "UNICODE_AI_CI"
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
      "sr_Cyrl_SRB_AI"
    ).foreach(collationICU => {
      val col = fetchCollation(collationICU)
      assert(col.collator.getLocale(ULocale.VALID_LOCALE) != ULocale.ROOT)
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
      1 << 19, // UTF8_BINARY mandatory zero bit 19 breach.
      1 << 20, // UTF8_BINARY mandatory zero bit 20 breach.
      1 << 21, // UTF8_BINARY mandatory zero bit 21 breach.
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
      (1 << 29) | (1 << 19), // ICU mandatory zero bit 19 breach.
      (1 << 29) | (1 << 20), // ICU mandatory zero bit 20 breach.
      (1 << 29) | (1 << 21), // ICU mandatory zero bit 21 breach.
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
      ("UNICODE_CS_CS", "UNICODE_CS"),
      ("UNICODE_CI_CI", "UNICODE_CI"),
      ("UNICODE_CI_CS", "UNICODE_CS"),
      ("UNICODE_CS_CI", "UNICODE_CS"),
      ("UNICODE_AS_AS", "UNICODE_AS"),
      ("UNICODE_AI_AI", "UNICODE_AI"),
      ("UNICODE_AS_AI", "UNICODE_AS"),
      ("UNICODE_AI_AS", "UNICODE_AS"),
      ("UNICODE_AS_CS_AI", "UNICODE_AS_CS"),
      ("UNICODE_CS_AI_CI", "UNICODE_CS_AI"),
      ("UNICODE_CS_AS_CI_AI", "UNICODE_CS_AS"),
      ("UNICODE__CS__AS", "UNICODE_AS"),
      ("UNICODE-CS-AS", "UNICODE"),
      ("UNICODECSAS", "UNICODE"),
      ("_CS_AS_UNICODE", "UNICODE")
    ).foreach { case (collationName, proposals) =>
      checkCollationNameError(collationName, proposals)
    }
  }

  test("basic ICU collator checks") {
    Seq(
      CollationTestCase("UNICODE_CI", "a", "A", true),
      CollationTestCase("UNICODE_CI", "a", "å", false),
      CollationTestCase("UNICODE_CI", "a", "Å", false),
      CollationTestCase("UNICODE_AI", "a", "A", false),
      CollationTestCase("UNICODE_AI", "a", "å", true),
      CollationTestCase("UNICODE_AI", "a", "Å", false),
      CollationTestCase("UNICODE_CI_AI", "a", "A", true),
      CollationTestCase("UNICODE_CI_AI", "a", "å", true),
      CollationTestCase("UNICODE_CI_AI", "a", "Å", true)
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
