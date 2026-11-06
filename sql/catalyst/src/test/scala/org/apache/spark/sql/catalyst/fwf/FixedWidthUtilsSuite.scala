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

  test("widthsToColspecs") {
    assert(FixedWidthUtils.widthsToColspecs(Seq(5, 5, 10)) ===
      Seq((0, 5), (5, 10), (10, 20)))
    assert(FixedWidthUtils.widthsToColspecs(Seq.empty) === Seq.empty)
  }

  test("detectColspecs") {
    val rows = Seq("a   bb  ccc", "aa  b   cc ")
    val colspecs = FixedWidthUtils.detectColspecs(rows, comment = None, delimiter = None)
    assert(colspecs === Seq((0, 2), (4, 6), (8, 11)))

    assert(FixedWidthUtils.sliceLine(rows(0), colspecs, None).toSeq === Seq("a", "bb", "ccc"))
    assert(FixedWidthUtils.sliceLine(rows(1), colspecs, None).toSeq === Seq("aa", "b", "cc"))
  }

  test("detectColspecs ignores comment text") {
    val colspecs =
      FixedWidthUtils.detectColspecs(Seq("aa#bb"), comment = Some('#'), delimiter = None)
    assert(colspecs === Seq((0, 2)))
  }

  test("detectColspecs on empty input") {
    assert(FixedWidthUtils.detectColspecs(Seq.empty, None, None) === Seq.empty)
  }

  test("custom delimiter replaces default whitespace blanks") {
    val colspecs =
      FixedWidthUtils.detectColspecs(Seq("a,b .c"), comment = None, delimiter = Some(","))
    assert(colspecs === Seq((0, 1), (2, 6)))
    assert(FixedWidthUtils.sliceLine("a,b .c", colspecs, Some(",")).toSeq === Seq("a", "b .c"))
  }

  test("sliceLine short line") {
    val tokens = FixedWidthUtils.sliceLine("ab", Seq((0, 2), (2, 5), (10, 12)), None)
    assert(tokens.toSeq === Seq("ab", "", ""))
  }

  test("sliceLine strips delimiter characters") {
    val tokens = FixedWidthUtils.sliceLine("  ab  cd  ", Seq((0, 6), (6, 10)), None)
    assert(tokens.toSeq === Seq("ab", "cd"))
  }

  test("isLineEffectivelyEmpty") {
    assert(FixedWidthUtils.isLineEffectivelyEmpty(Array("", "")))
    assert(!FixedWidthUtils.isLineEffectivelyEmpty(Array("", "x")))
    assert(FixedWidthUtils.isLineEffectivelyEmpty(Array.empty))
  }

  test("isLineBlank uses the raw line") {
    assert(FixedWidthUtils.isLineBlank("   ", None))
    assert(FixedWidthUtils.isLineBlank("", None))
    assert(!FixedWidthUtils.isLineBlank("a", None))
    assert(!FixedWidthUtils.isLineBlank("x   y", None))
    assert(FixedWidthUtils.isLineEffectivelyEmpty(Array.empty))
  }
}
