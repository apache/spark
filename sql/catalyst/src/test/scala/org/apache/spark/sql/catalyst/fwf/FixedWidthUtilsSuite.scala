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

package org.apache.spark.sql.catalyst.fwf

import org.apache.spark.SparkFunSuite

class FixedWidthUtilsSuite extends SparkFunSuite {

  test("widthsToColspecs converts contiguous widths to half-open intervals") {
    assert(FixedWidthUtils.widthsToColspecs(Seq(5, 5, 10)) ===
      Seq((0, 5), (5, 10), (10, 20)))
    assert(FixedWidthUtils.widthsToColspecs(Seq.empty) === Seq.empty)
  }

  test("detectColspecs infers boundaries from whitespace-separated sample rows") {
    // Column 1 occupies [0, 2), column 2 [4, 6), column 3 [8, 11) across both rows; hand-verified
    // against the mask-based algorithm (occupied = union of non-blank positions per row).
    val rows = Seq("a   bb  ccc", "aa  b   cc ")
    val colspecs = FixedWidthUtils.detectColspecs(rows, comment = None, delimiter = None)
    assert(colspecs === Seq((0, 2), (4, 6), (8, 11)))

    assert(FixedWidthUtils.sliceLine(rows(0), colspecs, None).toSeq === Seq("a", "bb", "ccc"))
    assert(FixedWidthUtils.sliceLine(rows(1), colspecs, None).toSeq === Seq("aa", "b", "cc"))
  }

  test("detectColspecs strips comment text before building the occupancy mask") {
    // Without stripping the comment, "aa#bb" would mask all 5 positions as one field, [(0, 5)].
    // The comment character and everything after it must not contribute to any column boundary.
    val colspecs =
      FixedWidthUtils.detectColspecs(Seq("aa#bb"), comment = Some('#'), delimiter = None)
    assert(colspecs === Seq((0, 2)))
  }

  test("detectColspecs returns empty colspecs for empty input rather than raising") {
    // Deliberate deviation from pandas' `EmptyDataError`: Spark's own file sources treat "no
    // data to infer from" as an empty schema, not an error.
    assert(FixedWidthUtils.detectColspecs(Seq.empty, None, None) === Seq.empty)
  }

  test("a custom delimiter replaces, rather than extends, the default whitespace blank set") {
    // Mirrors pandas: `self.delimiter = "\r\n" + delimiter if delimiter else "\n\r\t "` -- once a
    // custom delimiter is given, space/tab are no longer blanks, so "b .c" is one field, not two.
    val colspecs =
      FixedWidthUtils.detectColspecs(Seq("a,b .c"), comment = None, delimiter = Some(","))
    assert(colspecs === Seq((0, 1), (2, 6)))
    assert(FixedWidthUtils.sliceLine("a,b .c", colspecs, Some(",")).toSeq === Seq("a", "b .c"))
  }

  test("sliceLine handles lines shorter than a colspec by yielding an empty string") {
    val tokens = FixedWidthUtils.sliceLine("ab", Seq((0, 2), (2, 5), (10, 12)), None)
    assert(tokens.toSeq === Seq("ab", "", ""))
  }

  test("sliceLine strips configured delimiter characters from each field") {
    val tokens = FixedWidthUtils.sliceLine("  ab  cd  ", Seq((0, 6), (6, 10)), None)
    assert(tokens.toSeq === Seq("ab", "cd"))
  }

  test("isLineEffectivelyEmpty") {
    assert(FixedWidthUtils.isLineEffectivelyEmpty(Array("", "")))
    assert(!FixedWidthUtils.isLineEffectivelyEmpty(Array("", "x")))
    // Vacuously true on a zero-length token array -- exactly why a column-pruned (or
    // zero-column) read must not call this on pruned tokens; see isLineBlank below.
    assert(FixedWidthUtils.isLineEffectivelyEmpty(Array.empty))
  }

  test("isLineBlank checks the raw line, unaffected by column pruning") {
    assert(FixedWidthUtils.isLineBlank("   ", None))
    assert(FixedWidthUtils.isLineBlank("", None))
    assert(!FixedWidthUtils.isLineBlank("a", None))
    // The whole point: a row with real data still reads as blank under isLineEffectivelyEmpty
    // once pruned down to zero (or all-blank) selected tokens, but isLineBlank correctly sees
    // the full raw line and says it isn't blank.
    assert(!FixedWidthUtils.isLineBlank("x   y", None))
    assert(FixedWidthUtils.isLineEffectivelyEmpty(Array.empty))
  }
}
