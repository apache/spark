package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Gen.Choose
import org.scalatest.Assertion
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

  test("random int distribution") {
    val range = 1000
    val fn: RangeToLimitsFn[Int] = { case (x, y) => Limits(x, y + 2 * range) }
    checkDistributionOf(range, fn)
  }

  test("random longs") {
    forAll { (x: Long, y: Long) =>
      val longLimit:  Limits[Long]  = Limits(x, y)
      val gen:        RandomT[Long] = RandomRanges(longLimit)
      val result:     Long          = gen.randomT()
      assert(result >= math.min(x, y) && result <= math.max(x, y))
    }
  }

  test("random long distribution") {
    val range = 1000L
    val fn: RangeToLimitsFn[Long] = { case (x, y) => Limits(x, y + 2 * range) }
    checkDistributionOf(range, fn)
  }

  type RangeToLimitsFn[T] = (T, T) => Limits[T]

  def checkDistributionOf[T: Numeric: Generator: Choose](range: T, limFn: RangeToLimitsFn[T]): Unit = {
    val ops: Numeric[T] = implicitly[Numeric[T]]
    val gen: Gen[(T, T)] = for {
      x <- Gen.choose(ops.negate(range), range)
      y <- Gen.choose(ops.negate(range), range)
    } yield (x, y)
    forAll(gen) { case (x, y) =>
      assertEvenDistribution(10000, limFn(x, y))
    }
  }

  def assertEvenDistribution[T: Numeric: Generator](n: Int, lim: Limits[T]): Assertion = {
    val gen:          RandomT[T]  = RandomRanges(lim)
    val ops:          Numeric[T]  = implicitly[Numeric[T]]
    val xs:           Seq[T]      = (0 to n).map(_ => gen.randomT())
    val mean:         Double      = ops.toDouble(xs.sum) / xs.length
    val squaredDiff:  Seq[Double] = xs.map(x => math.pow(ops.toDouble(x) - mean, 2))
    val stdDev:       Double      = math.pow(squaredDiff.sum / n - 1, 0.5)
    val halfWay:      Double      = ops.toDouble(lim.x) + ops.toDouble(lim.y) / 2
    val tolerance:    Double      = 5 * stdDev
    assert(mean > halfWay - tolerance && mean < halfWay + tolerance)
  }

}
