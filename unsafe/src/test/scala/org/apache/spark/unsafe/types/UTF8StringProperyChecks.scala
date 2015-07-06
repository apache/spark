package org.apache.spark.unsafe.types

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.unsafe.types.UTF8String.{fromString => toUTF8}


class UTF8StringSuite2 extends FunSuite with GeneratorDrivenPropertyChecks with Matchers {

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
      assert(s.substring(0, 0) === toUTF8(s).substring(0, 0).toString)
      assert(s.substring(0, s.length) === toUTF8(s).substring(0, s.length).toString)
    }
  }
}
