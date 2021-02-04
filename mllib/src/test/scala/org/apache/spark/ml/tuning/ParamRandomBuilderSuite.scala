package org.apache.spark.ml.tuning

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.SparkFunSuite
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
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
    checkRange[Int]
  }

  test("random int distribution") {
    checkDistributionOf(1000)
  }

  test("random longs") {
    checkRange[Long]
  }

  test("random long distribution") {
    checkDistributionOf(1000L)
  }

  test("random doubles") {
    checkRange[Double]
  }

  test("random double distribution") {
    checkDistributionOf(1000d)
  }

  test("random floats") {
    checkRange[Float]
  }

  test("random float distribution") {
    checkDistributionOf(1000f)
  }

  def checkRange[T: Numeric: Generator: Choose: TypeTag: Arbitrary]: Assertion = {
    forAll { (x: T, y: T) =>
      val ops: Numeric[T]     = implicitly[Numeric[T]]
      val limit:  Limits[T]   = Limits(x, y)
      val gen:    RandomT[T]  = RandomRanges(limit)
      val result: T           = gen.randomT()
      val ordered             = lowerUpper(x, y)
      assert(ops.gteq(result, ordered._1) && ops.lteq(result, ordered._2))
    }
  }

  def checkDistributionOf[T: Numeric: Generator: Choose](range: T): Unit = {
    val ops: Numeric[T] = implicitly[Numeric[T]]
    import ops._
    val gen: Gen[(T, T)] = for {
      x <- Gen.choose(negate(range), range)
      y <- Gen.choose(range, times(range, plus(one, one)))
    } yield (x, y)
    forAll(gen) { case (x, y) =>
      assertEvenDistribution(10000, Limits(x, y))
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
