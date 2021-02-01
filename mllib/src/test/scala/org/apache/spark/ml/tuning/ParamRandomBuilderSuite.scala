package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.scalacheck.Arbitrary._
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ParamRandomBuilderSuite extends SparkFunSuite with ScalaCheckDrivenPropertyChecks with Matchers {

  import RandomRanges._

  test("random BigInt generation does not go into infinite loop") {
    assert(randomBigInt0To(0) == BigInt(0))
  }

  test("random ints") {
    forAll { (x: Int, y: Int) =>
      val intLimit: Limits[Int] = Limits(x, y)
      val result:   Int         = intLimit.randomT()
      assert(result >= math.min(x, y) && result <= math.max(x, y))
    }
  }

}
