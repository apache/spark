package org.apache.spark.util.collection.unsafe.sort

import org.scalatest.prop.PropertyChecks

import org.apache.spark.SparkFunSuite

class PrefixComparatorsSuite extends SparkFunSuite with PropertyChecks {

  test("String prefix comparator") {

    def testPrefixComparison(s1: String, s2: String): Unit = {
      val s1Prefix = PrefixComparators.STRING.computePrefix(s1)
      val s2Prefix = PrefixComparators.STRING.computePrefix(s2)
      val prefixComparisonResult = PrefixComparators.STRING.compare(s1Prefix, s2Prefix)
      assert(
        (prefixComparisonResult == 0) ||
        (prefixComparisonResult < 0 && s1 < s2) ||
        (prefixComparisonResult > 0 && s1 > s2))
    }

    val regressionTests = Table(
      ("s1", "s2"),
      ("abc", "世界"),
      ("你好", "世界"),
      ("你好123", "你好122")
    )

    forAll (regressionTests) { (s1: String, s2: String) => testPrefixComparison(s1, s2) }
    forAll { (s1: String, s2: String) => testPrefixComparison(s1, s2) }
  }
}
