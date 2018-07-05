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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{IntegerType, StringType}

class MaskExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("mask") {
    checkEvaluation(Mask(Literal("abcd-EFGH-8765-4321"), "U", "l", "#"), "llll-UUUU-####-####")
    checkEvaluation(
      new Mask(Literal("abcd-EFGH-8765-4321"), Literal("U"), Literal("l"), Literal("#")),
      "llll-UUUU-####-####")
    checkEvaluation(new Mask(Literal("abcd-EFGH-8765-4321"), Literal("U"), Literal("l")),
      "llll-UUUU-nnnn-nnnn")
    checkEvaluation(new Mask(Literal("abcd-EFGH-8765-4321"), Literal("U")), "xxxx-UUUU-nnnn-nnnn")
    checkEvaluation(new Mask(Literal("abcd-EFGH-8765-4321")), "xxxx-XXXX-nnnn-nnnn")
    checkEvaluation(new Mask(Literal(null, StringType)), null)
    checkEvaluation(Mask(Literal("abcd-EFGH-8765-4321"), null, "l", "#"), "llll-XXXX-####-####")
    checkEvaluation(new Mask(
      Literal("abcd-EFGH-8765-4321"),
      Literal(null, StringType),
      Literal(null, StringType),
      Literal(null, StringType)), "xxxx-XXXX-nnnn-nnnn")
    checkEvaluation(new Mask(Literal("abcd-EFGH-8765-4321"), Literal("Upper")),
      "xxxx-UUUU-nnnn-nnnn")
    checkEvaluation(new Mask(Literal("")), "")
    checkEvaluation(new Mask(Literal("abcd-EFGH-8765-4321"), Literal("")), "xxxx-XXXX-nnnn-nnnn")
    checkEvaluation(Mask(Literal("abcd-EFGH-8765-4321"), "", "", ""), "xxxx-XXXX-nnnn-nnnn")
    // scalastyle:off nonascii
    checkEvaluation(Mask(Literal("Ul9U"), "\u2200", null, null), "\u2200xn\u2200")
    checkEvaluation(new Mask(Literal("Hello World, こんにちは, 𠀋"), Literal("あ"), Literal("𡈽")),
      "あ𡈽𡈽𡈽𡈽 あ𡈽𡈽𡈽𡈽, こんにちは, 𠀋")
    // scalastyle:on nonascii
    intercept[AnalysisException] {
      checkEvaluation(new Mask(Literal(""), Literal(1)), "")
    }
  }

  test("mask_first_n") {
    checkEvaluation(MaskFirstN(Literal("aB3d-EFGH-8765"), 6, "U", "l", "#"),
      "lU#l-UFGH-8765")
    checkEvaluation(new MaskFirstN(
      Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U"), Literal("l"), Literal("#")),
      "llll-UFGH-8765-4321")
    checkEvaluation(
      new MaskFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U"), Literal("l")),
      "llll-UFGH-8765-4321")
    checkEvaluation(new MaskFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U")),
      "xxxx-UFGH-8765-4321")
    checkEvaluation(new MaskFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6)),
      "xxxx-XFGH-8765-4321")
    intercept[AnalysisException] {
      checkEvaluation(new MaskFirstN(Literal("abcd-EFGH-8765-4321"), Literal("U")), "")
    }
    checkEvaluation(new MaskFirstN(Literal("abcd-EFGH-8765-4321")), "xxxx-EFGH-8765-4321")
    checkEvaluation(new MaskFirstN(Literal(null, StringType)), null)
    checkEvaluation(MaskFirstN(Literal("abcd-EFGH-8765-4321"), 4, "U", "l", null),
      "llll-EFGH-8765-4321")
    checkEvaluation(new MaskFirstN(
      Literal("abcd-EFGH-8765-4321"),
      Literal(null, IntegerType),
      Literal(null, StringType),
      Literal(null, StringType),
      Literal(null, StringType)), "xxxx-EFGH-8765-4321")
    checkEvaluation(new MaskFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("Upper")),
      "xxxx-UFGH-8765-4321")
    checkEvaluation(new MaskFirstN(Literal("")), "")
    checkEvaluation(new MaskFirstN(Literal("abcd-EFGH-8765-4321"), Literal(4), Literal("")),
      "xxxx-EFGH-8765-4321")
    checkEvaluation(MaskFirstN(Literal("abcd-EFGH-8765-4321"), 1000, "", "", ""),
      "xxxx-XXXX-nnnn-nnnn")
    checkEvaluation(MaskFirstN(Literal("abcd-EFGH-8765-4321"), -1, "", "", ""),
      "abcd-EFGH-8765-4321")
    // scalastyle:off nonascii
    checkEvaluation(MaskFirstN(Literal("Ul9U"), 2, "\u2200", null, null), "\u2200x9U")
    checkEvaluation(new MaskFirstN(Literal("あ, 𠀋, Hello World"), Literal(10)),
      "あ, 𠀋, Xxxxo World")
    // scalastyle:on nonascii
  }

  test("mask_last_n") {
    checkEvaluation(MaskLastN(Literal("abcd-EFGH-aB3d"), 6, "U", "l", "#"),
      "abcd-EFGU-lU#l")
    checkEvaluation(new MaskLastN(
      Literal("abcd-EFGH-8765"), Literal(6), Literal("U"), Literal("l"), Literal("#")),
      "abcd-EFGU-####")
    checkEvaluation(
      new MaskLastN(Literal("abcd-EFGH-8765"), Literal(6), Literal("U"), Literal("l")),
      "abcd-EFGU-nnnn")
    checkEvaluation(
      new MaskLastN(Literal("abcd-EFGH-8765"), Literal(6), Literal("U")),
      "abcd-EFGU-nnnn")
    checkEvaluation(
      new MaskLastN(Literal("abcd-EFGH-8765"), Literal(6)),
      "abcd-EFGX-nnnn")
    intercept[AnalysisException] {
      checkEvaluation(new MaskLastN(Literal("abcd-EFGH-8765"), Literal("U")), "")
    }
    checkEvaluation(new MaskLastN(Literal("abcd-EFGH-8765-4321")), "abcd-EFGH-8765-nnnn")
    checkEvaluation(new MaskLastN(Literal(null, StringType)), null)
    checkEvaluation(MaskLastN(Literal("abcd-EFGH-8765-4321"), 4, "U", "l", null),
      "abcd-EFGH-8765-nnnn")
    checkEvaluation(new MaskLastN(
      Literal("abcd-EFGH-8765-4321"),
      Literal(null, IntegerType),
      Literal(null, StringType),
      Literal(null, StringType),
      Literal(null, StringType)), "abcd-EFGH-8765-nnnn")
    checkEvaluation(new MaskLastN(Literal("abcd-EFGH-8765-4321"), Literal(12), Literal("Upper")),
      "abcd-EFUU-nnnn-nnnn")
    checkEvaluation(new MaskLastN(Literal("")), "")
    checkEvaluation(new MaskLastN(Literal("abcd-EFGH-8765-4321"), Literal(16), Literal("")),
      "abcx-XXXX-nnnn-nnnn")
    checkEvaluation(MaskLastN(Literal("abcd-EFGH-8765-4321"), 1000, "", "", ""),
      "xxxx-XXXX-nnnn-nnnn")
    checkEvaluation(MaskLastN(Literal("abcd-EFGH-8765-4321"), -1, "", "", ""),
      "abcd-EFGH-8765-4321")
    // scalastyle:off nonascii
    checkEvaluation(MaskLastN(Literal("Ul9U"), 2, "\u2200", null, null), "Uln\u2200")
    checkEvaluation(new MaskLastN(Literal("あ, 𠀋, Hello World あ 𠀋"), Literal(10)),
      "あ, 𠀋, Hello Xxxxx あ 𠀋")
    // scalastyle:on nonascii
  }

  test("mask_show_first_n") {
    checkEvaluation(MaskShowFirstN(Literal("abcd-EFGH-8765-aB3d"), 6, "U", "l", "#"),
      "abcd-EUUU-####-lU#l")
    checkEvaluation(new MaskShowFirstN(
      Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U"), Literal("l"), Literal("#")),
      "abcd-EUUU-####-####")
    checkEvaluation(
      new MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U"), Literal("l")),
      "abcd-EUUU-nnnn-nnnn")
    checkEvaluation(new MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U")),
      "abcd-EUUU-nnnn-nnnn")
    checkEvaluation(new MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6)),
      "abcd-EXXX-nnnn-nnnn")
    intercept[AnalysisException] {
      checkEvaluation(new MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), Literal("U")), "")
    }
    checkEvaluation(new MaskShowFirstN(Literal("abcd-EFGH-8765-4321")), "abcd-XXXX-nnnn-nnnn")
    checkEvaluation(new MaskShowFirstN(Literal(null, StringType)), null)
    checkEvaluation(MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), 4, "U", "l", null),
      "abcd-UUUU-nnnn-nnnn")
    checkEvaluation(new MaskShowFirstN(
      Literal("abcd-EFGH-8765-4321"),
      Literal(null, IntegerType),
      Literal(null, StringType),
      Literal(null, StringType),
      Literal(null, StringType)), "abcd-XXXX-nnnn-nnnn")
    checkEvaluation(
      new MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("Upper")),
      "abcd-EUUU-nnnn-nnnn")
    checkEvaluation(new MaskShowFirstN(Literal("")), "")
    checkEvaluation(new MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), Literal(4), Literal("")),
      "abcd-XXXX-nnnn-nnnn")
    checkEvaluation(MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), 1000, "", "", ""),
      "abcd-EFGH-8765-4321")
    checkEvaluation(MaskShowFirstN(Literal("abcd-EFGH-8765-4321"), -1, "", "", ""),
      "xxxx-XXXX-nnnn-nnnn")
    // scalastyle:off nonascii
    checkEvaluation(MaskShowFirstN(Literal("Ul9U"), 2, "\u2200", null, null), "Uln\u2200")
    checkEvaluation(new MaskShowFirstN(Literal("あ, 𠀋, Hello World"), Literal(10)),
      "あ, 𠀋, Hellx Xxxxx")
    // scalastyle:on nonascii
  }

  test("mask_show_last_n") {
    checkEvaluation(MaskShowLastN(Literal("aB3d-EFGH-8765"), 6, "U", "l", "#"),
      "lU#l-UUUH-8765")
    checkEvaluation(new MaskShowLastN(
      Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U"), Literal("l"), Literal("#")),
      "llll-UUUU-###5-4321")
    checkEvaluation(
      new MaskShowLastN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U"), Literal("l")),
      "llll-UUUU-nnn5-4321")
    checkEvaluation(new MaskShowLastN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("U")),
      "xxxx-UUUU-nnn5-4321")
    checkEvaluation(new MaskShowLastN(Literal("abcd-EFGH-8765-4321"), Literal(6)),
      "xxxx-XXXX-nnn5-4321")
    intercept[AnalysisException] {
      checkEvaluation(new MaskShowLastN(Literal("abcd-EFGH-8765-4321"), Literal("U")), "")
    }
    checkEvaluation(new MaskShowLastN(Literal("abcd-EFGH-8765-4321")), "xxxx-XXXX-nnnn-4321")
    checkEvaluation(new MaskShowLastN(Literal(null, StringType)), null)
    checkEvaluation(MaskShowLastN(Literal("abcd-EFGH-8765-4321"), 4, "U", "l", null),
      "llll-UUUU-nnnn-4321")
    checkEvaluation(new MaskShowLastN(
      Literal("abcd-EFGH-8765-4321"),
      Literal(null, IntegerType),
      Literal(null, StringType),
      Literal(null, StringType),
      Literal(null, StringType)), "xxxx-XXXX-nnnn-4321")
    checkEvaluation(new MaskShowLastN(Literal("abcd-EFGH-8765-4321"), Literal(6), Literal("Upper")),
      "xxxx-UUUU-nnn5-4321")
    checkEvaluation(new MaskShowLastN(Literal("")), "")
    checkEvaluation(new MaskShowLastN(Literal("abcd-EFGH-8765-4321"), Literal(4), Literal("")),
      "xxxx-XXXX-nnnn-4321")
    checkEvaluation(MaskShowLastN(Literal("abcd-EFGH-8765-4321"), 1000, "", "", ""),
      "abcd-EFGH-8765-4321")
    checkEvaluation(MaskShowLastN(Literal("abcd-EFGH-8765-4321"), -1, "", "", ""),
      "xxxx-XXXX-nnnn-nnnn")
    // scalastyle:off nonascii
    checkEvaluation(MaskShowLastN(Literal("Ul9U"), 2, "\u2200", null, null), "\u2200x9U")
    checkEvaluation(new MaskShowLastN(Literal("あ, 𠀋, Hello World"), Literal(10)),
      "あ, 𠀋, Xello World")
    // scalastyle:on nonascii
  }

  test("mask_hash") {
    checkEvaluation(MaskHash(Literal("abcd-EFGH-8765-4321")), "60c713f5ec6912229d2060df1c322776")
    checkEvaluation(MaskHash(Literal("")), "d41d8cd98f00b204e9800998ecf8427e")
    checkEvaluation(MaskHash(Literal(null, StringType)), null)
    // scalastyle:off nonascii
    checkEvaluation(MaskHash(Literal("\u2200x9U")), "f1243ef123d516b1f32a3a75309e5711")
    // scalastyle:on nonascii
  }
}
