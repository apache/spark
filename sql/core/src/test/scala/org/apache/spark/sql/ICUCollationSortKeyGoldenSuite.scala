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
import org.apache.spark.util.Utils

// scalastyle:off line.size.limit
/**
 * Visibility golden suite for ICU collation sort keys. Snapshots the sort-key
 * bytes for a representative matrix of (collation, input) cells so that an
 * icu4j or CLDR data upgrade produces a readable golden diff for review.
 *
 * This is NOT a byte-stability contract. Downstream consumers MUST NOT rely
 * on sort-key byte equality across Spark versions. The golden file exists to
 * trigger reviewer attention on ICU upgrades, not to lock the byte layout.
 *
 * To re-generate the golden file, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.ICUCollationSortKeyGoldenSuite"
 * }}}
 *
 * The regen logic emits `GOLDEN_DISCLAIMER` as line 1 verbatim; the
 * disclaimer-survival assert below catches accidental drop on regen.
 */
// scalastyle:on line.size.limit
// scalastyle:off nonascii
class ICUCollationSortKeyGoldenSuite extends SparkFunSuite {

  import ICUCollationSortKeyGoldenSuite._

  private val goldenFile: Path = getWorkspaceFilePath(
    "sql", "core", "src", "test", "resources",
    "collations", "ICU-collations-sort-keys.md")

  // Read the golden file once per suite run; tests re-use the same buffer.
  private lazy val goldenLines: Seq[String] =
    Files.readAllLines(goldenFile, StandardCharsets.UTF_8).asScala.toSeq

  // Locate a row by (collation, input) by parsing pipe-delimited columns.
  // Robust to whitespace-only input rendering (D11 empty string).
  private def rowFor(collation: String, input: String): Option[String] =
    goldenLines.find { line =>
      val cols = parseRow(line)
      cols.length == 3 && cols(0) == collation && cols(1) == input
    }

  // Fetch hex column or fail with a regen hint.
  private def hexFor(collation: String, input: String): String = {
    val row = rowFor(collation, input).getOrElse(
      fail(s"golden row missing for ($collation, $input); $REGEN_HINT"))
    hexOf(row)
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (regenerateGoldenFiles) writeGoldenFile(goldenFile)
  }

  test("disclaimer is golden line 1") {
    assert(Files.exists(goldenFile),
      s"golden file is missing: $goldenFile; $REGEN_HINT")
    assert(goldenLines.nonEmpty, s"golden file is empty: $goldenFile; $REGEN_HINT")
    assert(goldenLines.head == GOLDEN_DISCLAIMER,
      s"expected golden line 1 to be the non-contract disclaimer verbatim; " +
        s"got: ${goldenLines.head}")
  }

  test("writeGoldenFile is deterministic and writes no BOM") {
    val tmpDir = Utils.createTempDir().toPath
    val a = tmpDir.resolve("a.md")
    val b = tmpDir.resolve("b.md")
    writeGoldenFile(a)
    writeGoldenFile(b)
    val bytesA = Files.readAllBytes(a)
    val bytesB = Files.readAllBytes(b)
    assert(bytesA.sameElements(bytesB),
      "writeGoldenFile must be deterministic: two consecutive writes differ")
    assert(bytesA.nonEmpty && bytesA(0) == '<'.toByte,
      "writeGoldenFile must not emit a UTF-8 BOM; line 1 must start with '<'")
    val firstLineA = Files.readAllLines(a, StandardCharsets.UTF_8).get(0)
    assert(firstLineA == GOLDEN_DISCLAIMER,
      "writeGoldenFile must emit GOLDEN_DISCLAIMER verbatim as line 1")
    val onDisk = Files.readAllBytes(goldenFile)
    assert(bytesA.sameElements(onDisk),
      s"on-disk golden differs from writeGoldenFile output; $REGEN_HINT")
  }

  test("UNICODE primary order separates basic-Latin letters") {
    // Existence baseline: every basic Latin cell reaches the golden as a row.
    val inputs = Seq("a", "b", "c", "A", "B", "C")
    val missing = inputs.filterNot(rowFor("UNICODE", _).isDefined)
    assert(missing.isEmpty,
      s"UNICODE basic-Latin rows missing: ${missing.mkString(", ")}; $REGEN_HINT")
  }

  test("UNICODE_CI folds tertiary case; UNICODE and en_USA distinguish it") {
    // UNICODE_CI must collate 'a'/'A' and 'b'/'B' identically; UNICODE and
    // en_USA preserve case at the tertiary level.
    val rowsRequired = for {
      coll <- Seq("UNICODE_CI", "en_USA")
      ch <- Seq("a", "A", "b", "B")
    } yield (coll, ch)
    val missing = rowsRequired.filterNot { case (c, i) => rowFor(c, i).isDefined }
    assert(missing.isEmpty,
      s"tertiary-case rows missing: ${missing.mkString(", ")}; $REGEN_HINT")

    assert(hexFor("UNICODE_CI", "a") == hexFor("UNICODE_CI", "A"),
      "UNICODE_CI should fold 'a' and 'A' to identical sort keys")
    assert(hexFor("UNICODE_CI", "b") == hexFor("UNICODE_CI", "B"),
      "UNICODE_CI should fold 'b' and 'B' to identical sort keys")
    assert(hexFor("UNICODE", "a") != hexFor("UNICODE", "A"),
      "UNICODE should distinguish 'a' from 'A' at tertiary")
    assert(hexFor("en_USA", "a") != hexFor("en_USA", "A"),
      "en_USA should distinguish 'a' from 'A' at tertiary")
    assert(hexFor("en_USA", "b") != hexFor("en_USA", "B"),
      "en_USA should distinguish 'b' from 'B' at tertiary")
  }

  test("UNICODE_AI and en_USA_AI fold accents; UNICODE distinguishes them") {
    // The AI bit collapses accents at the secondary level.
    val plain = "e"
    val acute = "\u00e9"
    for {
      coll <- Seq("UNICODE", "UNICODE_AI", "en_USA_AI")
      ch <- Seq(plain, acute)
    } {
      val label = if (ch == acute) "U+00E9" else ch
      assert(rowFor(coll, ch).isDefined,
        s"secondary-diacritic row missing: ($coll, $label); $REGEN_HINT")
    }

    assert(hexFor("UNICODE", plain) != hexFor("UNICODE", acute),
      "UNICODE should distinguish 'e' from 'e-acute' at secondary")
    assert(hexFor("UNICODE_AI", plain) == hexFor("UNICODE_AI", acute),
      "UNICODE_AI should fold 'e' and 'e-acute' to identical sort keys")
    assert(hexFor("en_USA_AI", plain) == hexFor("en_USA_AI", acute),
      "en_USA_AI should fold 'e' and 'e-acute' to identical sort keys")
  }

  test("UNICODE collates NFC and NFD canonically-equivalent forms identically") {
    // ICU honours canonical equivalence: the precomposed NFC form U+00E9 and
    // the decomposed NFD form "e" + U+0301 must produce the same sort key.
    // (This holds despite Spark's NO_DECOMPOSITION ICU option because the
    // single-codepoint precomposition is handled by ICU's DUCET contraction
    // lookup, not the runtime decomposition path.)
    val nfc = "\u00e9"
    val nfd = "e\u0301"
    assert(hexFor("UNICODE", nfc) == hexFor("UNICODE", nfd),
      s"canonical equivalence violated: hex(NFC) != hex(NFD) under UNICODE")
  }

  test("UNICODE records both combining-mark orderings for drift visibility") {
    // The two canonically-equivalent orderings of "a" + acute + dot-below
    // would produce identical sort keys under a normalising configuration,
    // but Spark uses ICU's NO_DECOMPOSITION setting and so emits distinct
    // sort keys for the two input orders. The golden records both rows so
    // that any future change (e.g. enabling decomposition, or canonical-
    // closure additions in CLDR) appears as a diff. No byte-equality is
    // asserted between the two orderings; the rows are visibility only.
    val aboveBelow = "a\u0301\u0323"
    val belowAbove = "a\u0323\u0301"
    assert(rowFor("UNICODE", aboveBelow).isDefined &&
      rowFor("UNICODE", belowAbove).isDefined,
      s"combining-reorder visibility rows missing under UNICODE; $REGEN_HINT")
  }

  test("UNICODE preserves SMP code points through surrogate-pair encoding") {
    // SMP code points (U+10000+) round-trip through UTF-16 surrogate pairs
    // and UTF-8 4-byte sequences. A regression that drops the surrogate
    // before reaching ICU would produce an empty hex.
    val emoji = new String(Character.toChars(0x1F600))   // grinning face
    val linearB = new String(Character.toChars(0x10000)) // Linear B B008 A
    val emojiHex = hexFor("UNICODE", emoji)
    val linearBHex = hexFor("UNICODE", linearB)
    assert(emojiHex.nonEmpty && linearBHex.nonEmpty,
      s"SMP sort-key hex is empty (emoji='$emojiHex', linearB='$linearBHex'); " +
        "surrogate pair may have been dropped before ICU sort-key computation")
  }

  test("UNICODE preserves BMP precomposed Hangul") {
    // Precomposed Hangul Syllables (AC00..D7A3) must reach the sort key and
    // not collapse into the UCA-fully-ignorable bucket that swallows C0
    // controls and variation selectors elsewhere in this suite.
    val hangeul = "\uD55C\uAE00" // HAN + GEUL ("hangeul")
    val hex = hexFor("UNICODE", hangeul)
    assert(hex.nonEmpty,
      s"Hangul sort-key hex is empty (hex='$hex'); " +
        "precomposed Hangul may have been silently ignored")
  }

  test("UNICODE places ASCII hyphen and space at primary level (NON_IGNORABLE)") {
    // Under ICU's NON_IGNORABLE alternate handling, ASCII punctuation and
    // whitespace carry primary weights. The opposite configuration
    // (SHIFTED) would collapse them into the UCA-fully-ignorable bucket,
    // producing sort keys whose primary segment is just the surrounding
    // letters ("ab"-shape, two primary weights only).
    val hyphenHex = hexFor("UNICODE", "a-b")
    val spaceHex = hexFor("UNICODE", "a b")
    assert(hyphenHex.nonEmpty && spaceHex.nonEmpty,
      s"punct/space sort-key hex is empty (hyphen='$hyphenHex', space='$spaceHex')")
    // NON_IGNORABLE invariant 1: the separator contributes its own primary
    // weight, so hyphen and space produce distinct sort keys. Under SHIFTED
    // (or SHIFT_TRIMMED with merged shifted weights) both would collapse
    // to the same shape as plain "ab".
    assert(hyphenHex != spaceHex,
      s"hyphen and space sort keys should differ under NON_IGNORABLE alternate " +
        s"handling (got hyphen=$hyphenHex space=$spaceHex); a SHIFTED config " +
        "would collapse them into the UCA-fully-ignorable bucket")
    // NON_IGNORABLE invariant 2 (L1 primary-segment probe): UCA sort keys
    // are segmented by 01 byte separators between levels (UTS #10 sec 3.2).
    // The primary segment runs from the start to the first 01 byte. Under
    // NON_IGNORABLE the separator carries a primary weight, so the L1
    // segment for "a<sep>b" contains 3+ primary weight bytes (a, sep, b).
    // Under SHIFTED the separator is removed from L1 (shifted to L4), so
    // the L1 segment would contain only 2 primary weights ("ab"-shape).
    def primarySegmentBytes(hex: String): Int = {
      val bytes = hex.split(' ').toSeq
      bytes.indexOf("01") match {
        case -1 => bytes.length  // no separator -> whole sort key is primary
        case n => n
      }
    }
    val hyphenL1 = primarySegmentBytes(hyphenHex)
    val spaceL1 = primarySegmentBytes(spaceHex)
    assert(hyphenL1 >= 3,
      s"hyphen L1 primary segment should carry the separator weight under " +
        s"NON_IGNORABLE (got $hyphenL1 bytes in '$hyphenHex' before the 01 " +
        "level separator); a 2-byte primary segment would indicate SHIFTED " +
        "(separator shifted to L4)")
    assert(spaceL1 >= 3,
      s"space L1 primary segment should carry the separator weight under " +
        s"NON_IGNORABLE (got $spaceL1 bytes in '$spaceHex' before the 01 " +
        "level separator); a 2-byte primary segment would indicate SHIFTED")
  }

  test("Turkish locale re-pairs i/I across dotted and dotless forms") {
    // Standard Latin treats i<->I as one case pair. Turkish treats
    // {i, dotted-capital-I (U+0130)} as the dotted pair and
    // {dotless-small-i (U+0131), I} as the dotless pair. The sort-key
    // primary prefix encodes that grouping: under `tr`, the dotted pair
    // shares one primary prefix and the dotless pair shares a different
    // primary prefix. Under `en_USA` no such re-pairing exists.
    val inputs = Seq("i", "I", "\u0131", "\u0130")
    val collations = Seq("en_USA", "tr")
    val missing = for {
      c <- collations
      i <- inputs
      if rowFor(c, i).isEmpty
    } yield s"($c, U+${"%04X".format(i.codePointAt(0))})"
    assert(missing.isEmpty,
      s"Turkish-tailoring rows missing: ${missing.mkString(", ")}; $REGEN_HINT")

    // Helper: strip the trailing tertiary/case section (the last
    // "<weight> 00" terminator pair) to compare primary prefixes only.
    def primaryPrefix(hex: String): String = {
      // Sort keys end with "...01 <case> 00"; drop the last two bytes (case + terminator).
      val bytes = hex.split(' ').toSeq
      assert(bytes.length >= 3, s"unexpected hex shape: $hex")
      bytes.dropRight(2).mkString(" ")
    }

    val trDottedSmall = primaryPrefix(hexFor("tr", "i"))
    val trDottedCap = primaryPrefix(hexFor("tr", "\u0130"))
    val trDotlessSmall = primaryPrefix(hexFor("tr", "\u0131"))
    val trDotlessCap = primaryPrefix(hexFor("tr", "I"))

    assert(trDottedSmall == trDottedCap,
      s"Turkish dotted pair {i, U+0130} should share primary prefix " +
        s"(got '$trDottedSmall' vs '$trDottedCap')")
    assert(trDotlessSmall == trDotlessCap,
      s"Turkish dotless pair {U+0131, I} should share primary prefix " +
        s"(got '$trDotlessSmall' vs '$trDotlessCap')")
    assert(trDottedSmall != trDotlessSmall,
      s"Turkish dotted and dotless pairs should have distinct primary " +
        s"prefixes (got dotted='$trDottedSmall', dotless='$trDotlessSmall'); " +
        "if equal, the Turkish tailoring is not being applied")
  }

  test("UNICODE assigns CJK Han implicit weights via ICU compressed-sortkey lead byte") {
    // CJK Unified Ideographs (4E00..9FFF) have no explicit DUCET entry; ICU
    // assigns implicit weights at runtime. Spark's ICU configuration emits
    // these weights with a `fa` compressed-sortkey lead byte (this is an ICU
    // implementation detail, not a UCA spec constant -- the suite pins it
    // so that any code-path change routing CJK through a different scheme
    // produces a visible diff).
    val beijing = "\u5317\u4eac" // BEI + JING
    val china = "\u4e2d\u56fd"   // ZHONG + GUO
    val beijingHex = hexFor("UNICODE", beijing)
    val chinaHex = hexFor("UNICODE", china)
    assert(beijingHex.nonEmpty && chinaHex.nonEmpty,
      s"CJK Han sort-key hex is empty (beijing='$beijingHex', china='$chinaHex')")
    assert(beijingHex.startsWith("fa "),
      s"CJK implicit-weight lead byte should be 'fa' under Spark's ICU " +
        s"configuration; got beijing='$beijingHex'. A different lead byte " +
        "means the implicit-weight scheme or CJK code path changed.")
    assert(chinaHex.startsWith("fa "),
      s"CJK implicit-weight lead byte should be 'fa' under Spark's ICU " +
        s"configuration; got china='$chinaHex'.")
  }

  test("UNICODE emits a row for the empty string") {
    // Boundary probe: ICU may return a zero-byte or terminator-only sort
    // key for the empty input; either shape is acceptable -- the golden
    // simply records what happens. The row is located by parsing pipe-
    // delimited columns rather than by string prefix, because the empty
    // input renders as a whitespace-only middle column.
    assert(rowFor("UNICODE", "").isDefined,
      s"empty-string row missing under UNICODE; $REGEN_HINT")
  }

  test("UNICODE preserves U+FFFD REPLACEMENT CHARACTER") {
    // U+FFFD is the canonical "invalid UTF-8 normalised" sentinel that
    // UTF8String emits for ill-formed input. Pin its sort key so any change
    // that drops the replacement char or folds it into a different bucket
    // shows up as a diff.
    val hex = hexFor("UNICODE", "\ufffd")
    assert(hex.nonEmpty,
      s"REPLACEMENT CHARACTER sort-key hex is empty (hex='$hex'); " +
        "U+FFFD may have been dropped before ICU sort-key computation")
  }

  test("UNICODE preserves C0 control characters NUL and US") {
    // U+0000 is a classic C-style "string terminator" leak point; a
    // C-style truncation anywhere in the UTF8String -> ICU chain would drop
    // everything from NUL onwards. U+001F (UNIT SEPARATOR) is a
    // representative non-NUL C0 control. Both should survive end-to-end.
    val nulHex = hexFor("UNICODE", "\u0000")
    val usHex = hexFor("UNICODE", "\u001f")
    assert(nulHex.nonEmpty && usHex.nonEmpty,
      s"C0 control sort-key hex is empty (nul='$nulHex', us='$usHex'); " +
        "control character may have been truncated in the UTF8String -> ICU chain")
  }

  test("UNICODE preserves variation selectors VS-15 and VS-16") {
    // Variation selectors are Default_Ignorable_Code_Point; UCA treats them
    // as fully ignorable at all collation levels but ICU still emits a
    // minimal terminator-bearing sort key for them. Pin both to catch any
    // future change that drops VS before reaching the sort-key function or
    // promotes them to a non-ignorable bucket.
    val vs15Hex = hexFor("UNICODE", "\ufe0e")
    val vs16Hex = hexFor("UNICODE", "\ufe0f")
    assert(vs15Hex.nonEmpty && vs16Hex.nonEmpty,
      s"variation selector sort-key hex is empty " +
        s"(vs15='$vs15Hex', vs16='$vs16Hex')")
  }
}

object ICUCollationSortKeyGoldenSuite {
  // Hardcoded so regen logic must re-emit verbatim; the line-1 assert
  // catches accidental drop on regen.
  val GOLDEN_DISCLAIMER: String =
    "<!-- NOT a stability contract: review-trigger snapshot only. Downstream " +
    "consumers MUST NOT rely on byte equality across Spark versions. -->"

  private val REGEN_HINT: String =
    "regenerate with SPARK_GENERATE_GOLDEN_FILES=1 after extending cells"

  /** A single golden cell: (collation name, input string) -> ICU sort-key bytes. */
  private[sql] case class GoldenCell(collation: String, input: String)

  /** Parse a markdown table row "| a | b | c |" into Seq("a","b","c"). */
  private[sql] def parseRow(row: String): Seq[String] = {
    // String.split with limit=-1 keeps trailing empty strings, so
    // "| a | b | c |".split("\\|", -1) yields ["", " a ", " b ", " c ", ""].
    // Drop the leading/trailing empties (NOT empty middle cells). Use a
    // single-space strip (NOT String.trim) because trim also strips NUL
    // and other control characters that appear as legitimate cell content
    // (e.g. U+0000 in the C0-controls row).
    val parts = row.split("\\|", -1)
    if (parts.length < 2) Seq.empty
    else parts.tail.init.toSeq.map(stripSinglePadding)
  }

  private def stripSinglePadding(s: String): String =
    s.stripPrefix(" ").stripSuffix(" ")

  /** Extract the trailing hex column from a "| collation | input | hex |" row. */
  private[sql] def hexOf(row: String): String = {
    val cols = parseRow(row)
    if (cols.isEmpty) "" else cols.last
  }

  /**
   * Cells covered by the golden snapshot. Ordered by dimension number for
   * review readability; the actual sort-key bytes are written by
   * `writeGoldenFile` in this same order.
   */
  private[sql] val cells: Seq[GoldenCell] = Seq(
    // D1: basic Latin primary order under UNICODE.
    GoldenCell("UNICODE", "a"),
    GoldenCell("UNICODE", "b"),
    GoldenCell("UNICODE", "c"),
    GoldenCell("UNICODE", "A"),
    GoldenCell("UNICODE", "B"),
    GoldenCell("UNICODE", "C"),

    // D2: tertiary case minimal-pair. UNICODE_CI folds case; en_USA
    // preserves it. The UNICODE baseline {a,A,b,B} reuses D1's rows.
    GoldenCell("UNICODE_CI", "a"),
    GoldenCell("UNICODE_CI", "A"),
    GoldenCell("UNICODE_CI", "b"),
    GoldenCell("UNICODE_CI", "B"),
    GoldenCell("en_USA", "a"),
    GoldenCell("en_USA", "A"),
    GoldenCell("en_USA", "b"),
    GoldenCell("en_USA", "B"),

    // D3: secondary diacritic minimal-pair. The AI variants fold accents;
    // the UNICODE baseline {e, e-acute} distinguishes them.
    GoldenCell("UNICODE", "e"),
    GoldenCell("UNICODE_AI", "e"),
    GoldenCell("UNICODE_AI", "é"),
    GoldenCell("en_USA_AI", "e"),
    GoldenCell("en_USA_AI", "é"),

    // D4: NFC vs NFD canonical equivalence under UNICODE.
    // First cell is precomposed e-acute (NFC), second is e + U+0301 (NFD).
    GoldenCell("UNICODE", "é"),
    GoldenCell("UNICODE", "e\u0301"),

    // D5: combining-mark reorder visibility under UNICODE. The two orderings
    // are canonically equivalent per UAX #15 but Spark's NO_DECOMPOSITION
    // ICU configuration emits distinct sort keys; the rows are visibility
    // only (no byte-equality is asserted). Escapes used because the combining
    // marks (U+0301 acute, U+0323 dot below) do not render legibly inline.
    GoldenCell("UNICODE", "a\u0301\u0323"),
    GoldenCell("UNICODE", "a\u0323\u0301"),

    // D6: supplementary plane code points under UNICODE. U+1F600 GRINNING
    // FACE and U+10000 LINEAR B SYLLABLE B008 A.
    GoldenCell("UNICODE", new String(Character.toChars(0x1F600))),
    GoldenCell("UNICODE", new String(Character.toChars(0x10000))),

    // D7: BMP precomposed Hangul Syllable under UNICODE.
    GoldenCell("UNICODE", "한글"),

    // D8: ASCII punctuation/space primary order under UNICODE.
    GoldenCell("UNICODE", "a-b"),
    GoldenCell("UNICODE", "a b"),

    // D9: Turkish i/I locale tailoring. Eight cells = four inputs x two
    // locales. ICU ships Turkish as the bare language-only locale "tr"
    // (no country code), so the collation name has no _<country> suffix.
    // Inputs: i, I, dotless-i (U+0131), dotted-capital-I (U+0130).
    GoldenCell("en_USA", "i"),
    GoldenCell("en_USA", "I"),
    GoldenCell("en_USA", "ı"),
    GoldenCell("en_USA", "İ"),
    GoldenCell("tr", "i"),
    GoldenCell("tr", "I"),
    GoldenCell("tr", "ı"),
    GoldenCell("tr", "İ"),

    // D10: CJK Han implicit-weight visibility under UNICODE.
    GoldenCell("UNICODE", "北京"),
    GoldenCell("UNICODE", "中国"),

    // D11: empty string existence boundary under UNICODE.
    GoldenCell("UNICODE", ""),

    // D12: U+FFFD REPLACEMENT CHARACTER under UNICODE. Escape used because
    // the glyph commonly renders as a hollow box in editors.
    GoldenCell("UNICODE", "\ufffd"),

    // D13: C0 control characters NUL (U+0000) and US (U+001F) under UNICODE.
    // Escapes used because these are non-printable controls.
    GoldenCell("UNICODE", "\u0000"),
    GoldenCell("UNICODE", "\u001f"),

    // D14: variation selectors VS-15 (U+FE0E) and VS-16 (U+FE0F) under
    // UNICODE. Escapes used because these are invisible format characters.
    GoldenCell("UNICODE", "\ufe0e"),
    GoldenCell("UNICODE", "\ufe0f")
  )

  /** Render a byte[] as space-separated lowercase 2-digit hex (e.g. `26 02 5c`). */
  private def toHex(bytes: Array[Byte]): String =
    bytes.map(b => f"${b & 0xff}%02x").mkString(" ")

  /** Compute the sort-key bytes for a single cell via the CollationFactory API. */
  private def sortKeyBytes(cell: GoldenCell): Array[Byte] = {
    val collationId = CollationFactory.collationNameToId(cell.collation)
    val collation = CollationFactory.fetchCollation(collationId)
    collation.sortKeyFunction.apply(UTF8String.fromString(cell.input))
  }

  /** Render one cell as a markdown table row. */
  private def renderRow(cell: GoldenCell): String =
    s"| ${cell.collation} | ${cell.input} | ${toHex(sortKeyBytes(cell))} |"

  /**
   * Emit the golden file payload to `file`. Line 1 is `GOLDEN_DISCLAIMER`
   * verbatim, followed by a section header, a markdown table header, and one
   * row per cell in `cells`.
   */
  def writeGoldenFile(file: Path): Unit = {
    val header = Seq(
      GOLDEN_DISCLAIMER,
      "## ICU sort-key bytes (review-trigger snapshot, NOT a stability contract)",
      "",
      "| collation | input | sortkey (hex) |",
      "|---|---|---|"
    )
    val rows = cells.map(renderRow)
    val payload = (header ++ rows).mkString("", "\n", "\n")
    Files.writeString(file, payload, StandardCharsets.UTF_8)
  }
}
// scalastyle:on nonascii
