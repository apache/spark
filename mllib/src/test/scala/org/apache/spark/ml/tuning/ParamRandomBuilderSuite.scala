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
    val gen = for {
      x <- Gen.choose(-range, range)
      y <- Gen.choose(-range, range)
    } yield (x, y)
    forAll(gen) { case (x, y) =>
      assertEvenDistribution(10000, Limits(x, y + 2 * range))
    }
  }

  def assertEvenDistribution[T: Numeric: Generator](n: Int, lim: Limits[T]): Assertion = {
    val gen         = RandomRanges(lim)
    val ops         = implicitly[Numeric[T]]
    val xs          = (0 to n).map(_ => gen.randomT())
    val mean        = ops.toDouble(xs.sum) / xs.length
    val squaredDiff = xs.map(x => math.pow(ops.toDouble(x) - mean, 2))
    val stdDev      = math.pow(squaredDiff.sum / n - 1, 0.5)
    val halfWay     = ops.toDouble(lim.x) + ops.toDouble(lim.y) / 2
    println(s"halfWay = $halfWay, stdDev = $stdDev, squaredDiff = ${squaredDiff.sum}, lim = $lim, mean = $mean, xs = ${xs.take(10).mkString(", ")}")
    val tolerance   = 5 * stdDev
    assert(mean > halfWay - tolerance && mean < halfWay + tolerance)
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
