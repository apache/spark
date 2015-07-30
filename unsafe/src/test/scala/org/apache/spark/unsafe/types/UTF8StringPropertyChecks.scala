package org.apache.spark.unsafe.types

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.unsafe.types.UTF8String.{fromString => toUTF8}

class UTF8StringPropertyChecks extends FunSuite with GeneratorDrivenPropertyChecks with Matchers {

  test("toString") {
    forAll { (s: String) =>
      assert(s === toUTF8(s).toString())
    }
  }

  test("numChars") {
    forAll { (s: String) =>
      assert(toUTF8(s).numChars() === s.length)
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

  test("toUpperCase") {
    forAll { (s: String) =>
      assert(s.toUpperCase === toUTF8(s).toUpperCase.toString)
    }
  }

  test("toLowerCase") {
    forAll { (s: String) =>
      assert(s.toLowerCase === toUTF8(s).toLowerCase.toString)
    }
  }

  test("compare") {
    forAll { (s1: String, s2: String) =>
      assert(Math.signum(s1.compareTo(s2)) === Math.signum(toUTF8(s1).compareTo(toUTF8(s2))))
    }
  }

  test("substring") {
    forAll { (s: String) =>
      for (start <- 0 to s.length; end <- 0 to s.length) {
        withClue(s"start=$start, end=$end") {
          assert(s.substring(start, end) === toUTF8(s).substring(start, end).toString)
        }
      }
    }
  }

  // TODO: substringSQL

  test("contains") {
    forAll { (s: String) =>
      for (start <- 0 to s.length; end <- 0 to s.length) {
        val substring = s.substring(start, end)
        withClue(s"substring=$substring") {
          assert(s.contains(substring) === toUTF8(s).contains(toUTF8(substring)))
        }
      }
    }
  }

  val whitespaceChar: Gen[Char] = Gen.choose(0x00, 0x20).map(_.toChar)
  val whitespaceString: Gen[String] = Gen.listOf(whitespaceChar).map(_.mkString)
  val randomString: Gen[String] = Arbitrary.arbString.arbitrary

  test("trim, trimLeft, trimRight") {
    forAll(
        whitespaceString,
        randomString,
        whitespaceString
    ) { (start: String, middle: String, end: String) =>
      val s = start + middle + end
      assert(s.trim() === toUTF8(s).trim().toString)
      assert(s.stripMargin === toUTF8(s).trimLeft().toString)
      assert(s.reverse.stripMargin.reverse === toUTF8(s).trimRight().toString)
    }
  }

  test("reverse") {
    forAll() { (s: String) =>
      assert(s.reverse === toUTF8(s).reverse.toString)
    }
  }

  // TODO: repeat
  // TODO: indexOf
  // TODO: lpad
  // TODO: rpad

  test("concat") {
    forAll() { (inputs: Seq[String]) =>
      // TODO: test case where at least one of the inputs is null
      assert(inputs.mkString === UTF8String.concat(inputs.map(toUTF8): _*).toString)
    }
  }

  test("concatWs") {
    forAll() { (sep: String, inputs: Seq[String]) =>
      // TODO: handle case where at least one of the inputs is null
      assert(
        inputs.mkString(sep) === UTF8String.concatWs(toUTF8(sep), inputs.map(toUTF8): _*).toString)
    }
  }

  // TODO: split

  // TODO: levenshteinDistance that tests against StringUtils' implementation

  // TODO: equals(), hashCode(), and compare()

}
