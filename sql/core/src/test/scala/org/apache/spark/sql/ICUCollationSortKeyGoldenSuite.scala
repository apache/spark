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

package org.apache.spark.sql

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.unsafe.types.UTF8String

/**
 * Golden file of ICU collation behavior, to make an icu4j or CLDR upgrade visible in review.
 *
 * The golden has two sections:
 *  1. Sort-key bytes per (collation, input). These bytes feed `hash()`, `xxhash64()` and
 *     sketch aggregates on collated strings, so a change here changes user-visible values.
 *  2. Per collation, the cell inputs sorted by `comparator`, which is what `ORDER BY` uses.
 *     Section 1 can change without any change here (a weight shift); a change here is a
 *     reorder.
 *
 * To re-generate the golden file, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly *ICUCollationSortKeyGoldenSuite"
 * }}}
 */
// scalastyle:off nonascii
class ICUCollationSortKeyGoldenSuite extends SparkFunSuite {

  import ICUCollationSortKeyGoldenSuite._

  private val goldenFile: Path = getWorkspaceFilePath(
    "sql", "core", "src", "test", "resources",
    "collations", "ICU-collations-sort-keys.md")

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (regenerateGoldenFiles) {
      Files.writeString(goldenFile, goldenText, StandardCharsets.UTF_8)
    }
  }

  test("golden file matches the current ICU output") {
    val expected = Files.readAllLines(goldenFile, StandardCharsets.UTF_8).asScala.toSeq
    val actual = goldenText.split("\n", -1).toSeq.dropRight(1)
    val changed = expected.zipAll(actual, "<missing>", "<missing>").zipWithIndex.collect {
      case ((e, a), i) if e != a => s"line ${i + 1}:\n  golden: $e\n  actual: $a"
    }
    assert(changed.isEmpty,
      s"${changed.size} golden line(s) differ from the current output ($icuVersionLine). " +
        s"Review them, then $REGEN_HINT\n${changed.mkString("\n")}")
  }

  test("golden cells are unique") {
    val dups = cells.groupBy(identity).collect { case (c, s) if s.size > 1 => c }
    assert(dups.isEmpty, s"duplicate golden cells: $dups")
  }

  test("UNICODE orders basic Latin by letter first, then by case") {
    val sorted = Seq("b", "B", "a", "c", "A").sortWith(less("UNICODE"))
    assert(sorted == Seq("a", "A", "b", "B", "c"))
  }

  test("UNICODE_CI and en_USA_CI fold case; UNICODE and en_USA do not") {
    assert(compare("UNICODE_CI", "a", "A") == 0)
    assert(compare("en_USA_CI", "b", "B") == 0)
    assert(compare("UNICODE", "a", "A") != 0)
    assert(compare("en_USA", "a", "A") != 0)
  }

  test("UNICODE_AI and en_USA_AI fold accents; UNICODE does not") {
    assert(compare("UNICODE_AI", "e", "\u00e9") == 0)
    assert(compare("en_USA_AI", "e", "\u00e9") == 0)
    assert(compare("UNICODE", "e", "\u00e9") != 0)
  }

  test("UNICODE: NFC and NFD e-acute share a key; reordered combining marks do not") {
    // Observed ICU behavior with Spark's collation settings, recorded so that an
    // upgrade that changes it fails here.
    assert(hex("UNICODE", "\u00e9") == hex("UNICODE", "e\u0301"))
    assert(hex("UNICODE", "a\u0301\u0323") != hex("UNICODE", "a\u0323\u0301"))
  }

  test("UNICODE treats hyphen and space as non-ignorable") {
    // With the SHIFTED alternate setting these would match "ab" at all three levels.
    assert(hex("UNICODE", "a-b") != hex("UNICODE", "ab"))
    assert(hex("UNICODE", "a b") != hex("UNICODE", "ab"))
  }

  test("Turkish case folding pairs I with dotless i, and dotted capital I with i") {
    assert(compare("tr_CI", "I", "\u0131") == 0)
    assert(compare("tr_CI", "\u0130", "i") == 0)
    assert(compare("tr_CI", "I", "i") != 0)
    assert(compare("en_USA_CI", "I", "i") == 0)
  }

  test("UNICODE ignores control characters and variation selectors inside a string") {
    // Embedded between letters, so a truncation at NUL would change the key.
    Seq("a\u0000b", "a\u001fb", "a\ufe0eb", "a\ufe0fb").foreach { s =>
      assert(hex("UNICODE", s) == hex("UNICODE", "ab"), s"key of ${codePoints(s)}")
    }
    assert(hex("UNICODE", "a\u0000b") != hex("UNICODE", "a"))
  }

  test("UNICODE keeps supplementary, Hangul and U+FFFD code points") {
    // A code point dropped before reaching ICU would give the empty-string key.
    val empty = hex("UNICODE", "")
    Seq(new String(Character.toChars(0x1F600)), new String(Character.toChars(0x10000)),
        "한글", "\ufffd").foreach { s =>
      assert(hex("UNICODE", s) != empty, s"key of ${codePoints(s)}")
    }
  }
}

object ICUCollationSortKeyGoldenSuite {
  private val REGEN_HINT: String = "regenerate with SPARK_GENERATE_GOLDEN_FILES=1"

  private[sql] val GOLDEN_DISCLAIMER: String =
    "<!-- Review trigger for ICU upgrades. Section 1 bytes feed hash(), xxhash64() and " +
      "sketch aggregates on collated strings; section 2 is the ORDER BY order. A change " +
      "here changes user-visible values. Not a promise that they stay fixed. -->"

  /** A single golden cell: a collation name and an input string. */
  private[sql] case class GoldenCell(collation: String, input: String)

  /**
   * Cells covered by the golden. For the locale tailorings, inputs are exemplar characters
   * whose sort keys differ between ICU4J 78.3 and 79.1 (sr and sr_Latn are a baseline that
   * did not change). The zh set also has neighbours whose comparator order changes in 79.1.
   */
  private[sql] val cells: Seq[GoldenCell] = Seq(
    // Basic Latin letters and case.
    "UNICODE" -> "a", "UNICODE" -> "b", "UNICODE" -> "c",
    "UNICODE" -> "A", "UNICODE" -> "B", "UNICODE" -> "C",
    "UNICODE_CI" -> "a", "UNICODE_CI" -> "A", "UNICODE_CI" -> "b", "UNICODE_CI" -> "B",
    "en_USA" -> "a", "en_USA" -> "A", "en_USA" -> "b", "en_USA" -> "B",
    // Accents.
    "UNICODE" -> "e", "UNICODE" -> "\u00e9", "UNICODE" -> "e\u0301",
    "UNICODE_AI" -> "e", "UNICODE_AI" -> "\u00e9",
    "en_USA_AI" -> "e", "en_USA_AI" -> "\u00e9",
    // Combining-mark order.
    "UNICODE" -> "a\u0301\u0323", "UNICODE" -> "a\u0323\u0301",
    // Supplementary planes and Hangul.
    "UNICODE" -> new String(Character.toChars(0x1F600)),
    "UNICODE" -> new String(Character.toChars(0x10000)),
    "UNICODE" -> "한글",
    // Punctuation and space.
    "UNICODE" -> "ab", "UNICODE" -> "a-b", "UNICODE" -> "a b",
    // Turkish i/I.
    "en_USA" -> "i", "en_USA" -> "I", "en_USA" -> "\u0131", "en_USA" -> "\u0130",
    "tr" -> "i", "tr" -> "I", "tr" -> "\u0131", "tr" -> "\u0130",
    // Han.
    "UNICODE" -> "北京", "UNICODE" -> "中国",
    // Edge inputs; controls and variation selectors sit between letters.
    "UNICODE" -> "", "UNICODE" -> "\ufffd",
    "UNICODE" -> "a\u0000b", "UNICODE" -> "a\u001fb",
    "UNICODE" -> "a\ufe0eb", "UNICODE" -> "a\ufe0fb",
    // Locale tailorings.
    "sr" -> "\u0431", "sr" -> "\u0452",
    "sr_Latn" -> "\u010d", "sr_Latn" -> "\u0111",
    "zh" -> "\u5319", "zh" -> "\u5566", "zh" -> "\u54c7",
    "zh" -> "\u6301", "zh" -> "\u62c9", "zh" -> "\u8a93", "zh" -> "\u6536", "zh" -> "\u6316",
    "zh_Hant" -> "\u4e01",
    "ja" -> "\u3005", "ja" -> "\u30fc",
    "sv" -> "\u00e5", "sv" -> "\u00f6",
    "de" -> "\u00df",
    "hi" -> "\u0922",
    "ko" -> "\u90ce"
  ).map { case (c, i) => GoldenCell(c, i) }

  private def collation(name: String): CollationFactory.Collation =
    CollationFactory.fetchCollation(CollationFactory.collationNameToId(name))

  private[sql] def compare(name: String, a: String, b: String): Int =
    collation(name).comparator.compare(UTF8String.fromString(a), UTF8String.fromString(b))

  private[sql] def less(name: String)(a: String, b: String): Boolean = compare(name, a, b) < 0

  private[sql] def hex(name: String, input: String): String =
    collation(name).sortKeyFunction.apply(UTF8String.fromString(input))
      .map(b => f"${b & 0xff}%02x").mkString(" ")

  private[sql] def codePoints(s: String): String =
    if (s.isEmpty) "(empty)" else s.codePoints().toArray.map(cp => f"U+$cp%04X").mkString(" ")

  // Character types that do not show as a distinct glyph in a diff.
  private val invisibleTypes: Set[Int] = Set(
    Character.CONTROL, Character.FORMAT, Character.NON_SPACING_MARK, Character.ENCLOSING_MARK,
    Character.SPACE_SEPARATOR, Character.UNASSIGNED).map(_.toInt)

  /** Input as shown in the golden: printable text with `|` and `\` escaped, else code points. */
  private[sql] def display(s: String): String = {
    val visible = s.nonEmpty &&
      s.codePoints().toArray.forall(cp => !invisibleTypes.contains(Character.getType(cp)))
    if (visible) s.replace("\\", "\\\\").replace("|", "\\|") else s"[${codePoints(s)}]"
  }

  private def icuVersionLine: String = s"ICU ${collation("UNICODE").version}"

  /** Inputs of one collation sorted by `comparator`; equal neighbours joined with `=`. */
  private def orderLine(name: String, inputs: Seq[String]): String = {
    val sorted = inputs.sortWith(less(name))
    sorted.indices.map { i =>
      val sep =
        if (i == 0) "" else if (compare(name, sorted(i - 1), sorted(i)) == 0) " = " else " < "
      sep + display(sorted(i))
    }.mkString
  }

  /** The full golden file content. */
  private[sql] def goldenText: String = {
    val keyRows = cells.map { c =>
      s"| ${c.collation} | ${display(c.input)} | ${codePoints(c.input)} | " +
        s"${hex(c.collation, c.input)} |"
    }
    val orderRows = cells.map(_.collation).distinct.map { n =>
      s"| $n | ${orderLine(n, cells.filter(_.collation == n).map(_.input))} |"
    }
    (Seq(GOLDEN_DISCLAIMER, "", icuVersionLine, "",
      "## 1. Sort keys", "",
      "| collation | input | code points | sort key (hex) |", "|---|---|---|---|") ++ keyRows ++
      Seq("", "## 2. Comparator order (ORDER BY)", "",
        "| collation | order |", "|---|---|") ++ orderRows).mkString("", "\n", "\n")
  }
}
// scalastyle:on nonascii
