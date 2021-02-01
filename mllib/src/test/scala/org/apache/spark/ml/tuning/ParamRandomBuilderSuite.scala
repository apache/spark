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
      val gen:    RandomT[Int]  = RandomRanges(intLimit)
      val result: Int           = gen.randomT()
      assert(result >= math.min(x, y) && result <= math.max(x, y))
    }
  }

  def assertEvenDistribution[T: Numeric: RandomT](n: Int) = {
    val gen = implicitly[RandomT[T]]
    val ops = implicitly[Numeric[T]]
    val xs = (0 to n).map(_ => gen.randomT())
    val mean = ops.toDouble(xs.sum) / xs.length
    val squaredDiff = xs.map(x => math.pow(ops.toDouble(x) - mean, 2))
    val stdDev = math.pow(squaredDiff.sum / n - 1, 0.5)
  }

  test("random longs") {
    forAll { (x: Long, y: Long) =>
      val longLimit: Limits[Long] = Limits(x, y)
      val gen:    RandomT[Long]  = RandomRanges(longLimit)
      val result: Long         = gen.randomT()
      assert(result >= math.min(x, y) && result <= math.max(x, y))
    }
  }

}
