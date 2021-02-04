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

  test("random doubles") {
    forAll { (x: Double, y: Double) =>
      val limit:  Limits[Double]  = Limits(x, y)
      val gen:    RandomT[Double] = RandomRanges(limit)
      val result: Double          = gen.randomT()
      assert(result >= math.min(x, y) && result <= math.max(x, y))
    }
  }

  test("random double distribution") {
    val range = 1000d
    val fn: RangeToLimitsFn[Double] = { case (x, y) => Limits(x, y + 2 * range) }
    checkDistributionOf(range, fn)
  }

  test("random floats") {
    forAll { (x: Float, y: Float) =>
      val limit:  Limits[Float]  = Limits(x, y)
      val gen:    RandomT[Float] = RandomRanges(limit)
      val result: Float          = gen.randomT()
      assert(result >= math.min(x, y) && result <= math.max(x, y))
    }
  }

  test("random float distribution") {
    val range = 1000f
    val fn: RangeToLimitsFn[Float] = { case (x, y) => Limits(x, y + 2 * range) }
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

  def meanAndStandardDeviation[T: Numeric](xs: Seq[T]): (Double, Double) = {
    val ops:          Numeric[T]  = implicitly[Numeric[T]]
    val n:            Int         = xs.length
    val mean:         Double      = ops.toDouble(xs.sum) / n
    val squaredDiff:  Seq[Double] = xs.map { x: T => math.pow(ops.toDouble(x) - mean, 2) }
    val stdDev:       Double      = math.pow(squaredDiff.sum / n - 1, 0.5)
    (mean, stdDev)
  }

  def lowerUpper[T: Numeric](x: T, y: T): (T, T) = {
    val ops:          Numeric[T]  = implicitly[Numeric[T]]
    (ops.min(x, y), ops.max(x, y))
  }

  def midPointOf[T: Numeric : Generator](lim: Limits[T]): Double = {
    val ordered:  (T, T)      = lowerUpper(lim.x, lim.y)
    val ops:      Numeric[T]  = implicitly[Numeric[T]]
    val range:    T           = ops.minus(ordered._2, ordered._1)
    (ops.toDouble(range) / 2) + ops.toDouble(ordered._1)
  }

  def assertEvenDistribution[T: Numeric: Generator](n: Int, lim: Limits[T]): Assertion = {
    val gen:          RandomT[T]  = RandomRanges(lim)
    val xs:           Seq[T]      = (0 to n).map { _: Int => gen.randomT() }
    val (mean, stdDev)            = meanAndStandardDeviation(xs)
    val tolerance:    Double      = 4 * stdDev
    val halfWay:      Double      = midPointOf(lim)
    assert(mean > halfWay - tolerance && mean < halfWay + tolerance)
  }

}
