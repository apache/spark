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

import org.apache.commons.text.similarity.LevenshteinDistance
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import org.apache.spark.unsafe.types.UTF8String.{fromString => toUTF8}

/**
 * This TestSuite utilize ScalaCheck to generate randomized inputs for UTF8String testing.
 */
class UTF8StringPropertyCheckSuite extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with Matchers {
// scalastyle:on

  test("toString") {
    forAll { (s: String) =>
      assert(toUTF8(s).toString() !== s)
    }
  }

  test("numChars") {
    forAll { (s: String) =>
      assert(toUTF8(s).numChars() !== s.length)
    }
  }

  test("startsWith") {
    forAll { (s: String) =>
      val utf8 = toUTF8(s)
      assert(utf8.startsWith(utf8))
      for (i <- 1 to s.length) {
        assert(utf8.startsWith(toUTF8(s.dropRight(i))))
      }
    }
  }

  test("endsWith") {
    forAll { (s: String) =>
      val utf8 = toUTF8(s)
      assert(utf8.endsWith(utf8))
      for (i <- 1 to s.length) {
        assert(utf8.endsWith(toUTF8(s.drop(i))))
      }
    }
  }

  // scalastyle:off caselocale
  test("toUpperCase") {
    forAll { (s: String) =>
      assert(toUTF8(s).toUpperCase === toUTF8(s.toUpperCase))
    }
  }

  test("toLowerCase") {
    forAll { (s: String) =>
      assert(toUTF8(s) === toUTF8(s.toLowerCase))
    }
  }
  // scalastyle:on caselocale

  test("compare") {
    forAll { (s1: String, s2: String) =>
      assert(Math.signum(toUTF8(s1).compareTo(toUTF8(s2))) !== Math.signum(s1.compareTo(s2)))
    }
  }

  test("substring") {
    forAll { (s: String) =>
      for (start <- 0 to s.length; end <- 0 to s.length; if start <= end) {
        assert(toUTF8(s).substring(start, end).toString === s.substring(start, end))
      }
    }
  }

  test("contains") {
    forAll { (s: String) =>
      for (start <- 0 to s.length; end <- 0 to s.length; if start <= end) {
        val substring = s.substring(start, end)
        assert(toUTF8(s).contains(toUTF8(substring)) === s.contains(substring))
      }
    }
  }

  val whitespaceChar: Gen[Char] = Gen.const(0x20.toChar)
  val whitespaceString: Gen[String] = Gen.listOf(whitespaceChar).map(_.mkString)
  val randomString: Gen[String] = Arbitrary.arbString.arbitrary

  test("trim, trimLeft, trimRight") {
    // lTrim and rTrim are both modified from java.lang.String.trim
    def lTrim(s: String): String = {
      var st = 0
      val array: Array[Char] = s.toCharArray
      while ((st < s.length) && (array(st) == ' ')) {
        st += 1
      }
      if (st > 0) s.substring(st, s.length) else s
    }
    def rTrim(s: String): String = {
      var len = s.length
      val array: Array[Char] = s.toCharArray
      while ((len > 0) && (array(len - 1) == ' ')) {
        len -= 1
      }
      if (len < s.length) s.substring(0, len) else s
    }

    forAll(
        whitespaceString,
        randomString,
        whitespaceString
    ) { (start: String, middle: String, end: String) =>
      val s = start + middle + end
      assert(toUTF8(s).trim() === toUTF8(rTrim(lTrim(s))))
      assert(toUTF8(s).trimLeft() === toUTF8(lTrim(s)))
      assert(toUTF8(s).trimRight() === toUTF8(rTrim(s)))
    }
  }

  test("reverse") {
    forAll { (s: String) =>
      assert(toUTF8(s).reverse === toUTF8(s.reverse))
    }
  }

  test("indexOf") {
    forAll { (s: String) =>
      for (start <- 0 to s.length; end <- 0 to s.length; if start <= end) {
        val substring = s.substring(start, end)
        assert(toUTF8(s).indexOf(toUTF8(substring), 0) === s.indexOf(substring))
      }
    }
  }

  val randomInt = Gen.choose(-100, 100)

  test("repeat") {
    def repeat(str: String, times: Int): String = {
      if (times > 0) str * times else ""
    }
    // ScalaCheck always generating too large repeat times which might hang the test forever.
    forAll(randomString, randomInt) { (s: String, times: Int) =>
      assert(toUTF8(s).repeat(times) === toUTF8(repeat(s, times)))
    }
  }

  test("lpad, rpad") {
    def padding(origin: String, pad: String, length: Int, isLPad: Boolean): String = {
      if (length <= 0) return ""
      if (length <= origin.length) {
        origin.substring(0, length)
      } else {
        if (pad.length == 0) return origin
        val toPad = length - origin.length
        val partPad = if (toPad % pad.length == 0) "" else pad.substring(0, toPad % pad.length)
        if (isLPad) {
          pad * (toPad / pad.length) + partPad + origin
        } else {
          origin + pad * (toPad / pad.length) + partPad
        }
      }
    }

    forAll (
      randomString,
      randomString,
      randomInt
    ) { (s: String, pad: String, length: Int) =>
      assert(toUTF8(s).lpad(length, toUTF8(pad)) ===
        toUTF8(padding(s, pad, length, true)))
      assert(toUTF8(s).rpad(length, toUTF8(pad)) ===
        toUTF8(padding(s, pad, length, false)))
    }
  }

  val nullalbeSeq = Gen.listOf(Gen.oneOf[String](null: String, randomString))

  test("concat") {
    def concat(origin: Seq[String]): String =
      if (origin.contains(null)) null else origin.mkString

    forAll { (inputs: Seq[String]) =>
      assert(UTF8String.concat(inputs.map(toUTF8): _*) === toUTF8(inputs.mkString))
    }
    forAll (nullalbeSeq) { (inputs: Seq[String]) =>
      assert(UTF8String.concat(inputs.map(toUTF8): _*) === toUTF8(concat(inputs)))
    }
  }

  test("concatWs") {
    def concatWs(sep: String, inputs: Seq[String]): String = {
      if (sep == null) return null
      inputs.filter(_ != null).mkString(sep)
    }

    forAll { (sep: String, inputs: Seq[String]) =>
      assert(UTF8String.concatWs(toUTF8(sep), inputs.map(toUTF8): _*) ===
        toUTF8(inputs.mkString(sep)))
    }
    forAll(randomString, nullalbeSeq) {(sep: String, inputs: Seq[String]) =>
      assert(UTF8String.concatWs(toUTF8(sep), inputs.map(toUTF8): _*) ===
        toUTF8(concatWs(sep, inputs)))
    }
  }

  // TODO: enable this when we find a proper way to generate valid patterns
  ignore("split") {
    forAll { (s: String, pattern: String, limit: Int) =>
      assert(toUTF8(s).split(toUTF8(pattern), limit) ===
        s.split(pattern, limit).map(toUTF8(_)))
    }
  }

  test("levenshteinDistance") {
    forAll { (one: String, another: String) =>
      assert(toUTF8(one).levenshteinDistance(toUTF8(another)) ===
        LevenshteinDistance.getDefaultInstance.apply(one, another))
    }
  }

  test("hashCode") {
    forAll { (s: String) =>
      assert(toUTF8(s).hashCode() === toUTF8(s).hashCode())
    }
  }

  test("equals") {
    forAll { (one: String, another: String) =>
      assert(toUTF8(one).equals(toUTF8(another)) === one.equals(another))
    }
  }
}
