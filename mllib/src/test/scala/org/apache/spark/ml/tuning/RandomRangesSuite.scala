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

package org.apache.spark.ml.tuning

import scala.reflect.runtime.universe.TypeTag

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen.Choose
import org.scalatest.{Assertion, Succeeded}
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.SparkFunSuite

class RandomRangesSuite extends SparkFunSuite with ScalaCheckDrivenPropertyChecks with Matchers {

  import RandomRanges._

  test("log of any base") {
    assert(logN(16, 4) == 2d)
    assert(logN(1000, 10) === (3d +- 0.000001))
    assert(logN(256, 2) == 8d)
  }

  test("random doubles in log space") {
    val gen: Gen[(Double, Double, Int)] = for {
      x <- Gen.choose(0d, Double.MaxValue)
      y <- Gen.choose(0d, Double.MaxValue)
      n <- Gen.choose(0, Int.MaxValue)
    } yield (x, y, n)
    forAll(gen) { case (x, y, n) =>
      val lower = math.min(x, y)
      val upper = math.max(x, y)
      val result = randomLog(x, y, n)
      assert(result >= lower && result <= upper)
    }
  }

  test("random BigInt generation does not go into infinite loop") {
    assert(randomBigInt0To(0) == BigInt(0))
  }

  test("random ints") {
    checkRange(Linear[Int])
  }

  test("random log ints") {
    checkRange(Log10[Int])
  }

  test("random int distribution") {
    checkDistributionOf(1000)
  }

  test("random doubles") {
    checkRange(Linear[Double])
  }

  test("random log doubles") {
    checkRange(Log10[Double])
  }

  test("random double distribution") {
    checkDistributionOf(1000d)
  }

  test("random floats") {
    checkRange(Linear[Float])
  }

  test("random log floats") {
    checkRange(Log10[Float])
  }

  test("random float distribution") {
    checkDistributionOf(1000f)
  }

  private abstract class RandomFn[T: Numeric: Generator] {
    def apply(genRandom: RandomT[T]): T = genRandom.randomT()
    def appropriate(x: T, y: T): Boolean
  }

  private def Linear[T: Numeric: Generator]: RandomFn[T] = new RandomFn {
    override def apply(genRandom: RandomT[T]): T = genRandom.randomT()
    override def appropriate(x: T, y: T): Boolean = true
  }

  private def Log10[T: Numeric: Generator]: RandomFn[T] = new RandomFn {
    override def apply(genRandom: RandomT[T]): T = genRandom.randomTLog(10)
    val ops: Numeric[T] = implicitly[Numeric[T]]
    override def appropriate(x: T, y: T): Boolean = {
      ops.gt(x, ops.zero) && ops.gt(y, ops.zero) && x != y
    }
  }

  private def checkRange[T: Numeric: Generator: Choose: TypeTag: Arbitrary]
  (rand: RandomFn[T]): Assertion =
    forAll { (x: T, y: T) =>
      if (rand.appropriate(x, y)) {
        val ops: Numeric[T] = implicitly[Numeric[T]]
        val limit: Limits[T] = Limits(x, y)
        val gen: RandomT[T] = RandomRanges(limit)
        val result: T = rand(gen)
        val ordered: (T, T) = lowerUpper(x, y)
        assert(ops.gteq(result, ordered._1) && ops.lteq(result, ordered._2))
      } else Succeeded
    }

  private def checkDistributionOf[T: Numeric: Generator: Choose](range: T): Unit = {
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

  private def meanAndStandardDeviation[T: Numeric](xs: Seq[T]): (Double, Double) = {
    val ops: Numeric[T] = implicitly[Numeric[T]]
    val n: Int = xs.length
    val mean: Double = ops.toDouble(xs.sum) / n
    val squaredDiff: Seq[Double] = xs.map { x: T => math.pow(ops.toDouble(x) - mean, 2) }
    val stdDev: Double = math.pow(squaredDiff.sum / n - 1, 0.5)
    (mean, stdDev)
  }

  private def lowerUpper[T: Numeric](x: T, y: T): (T, T) = {
    val ops: Numeric[T] = implicitly[Numeric[T]]
    (ops.min(x, y), ops.max(x, y))
  }

  private def midPointOf[T: Numeric : Generator](lim: Limits[T]): Double = {
    val ordered: (T, T) = lowerUpper(lim.x, lim.y)
    val ops: Numeric[T] = implicitly[Numeric[T]]
    val range: T = ops.minus(ordered._2, ordered._1)
    (ops.toDouble(range) / 2) + ops.toDouble(ordered._1)
  }

  private def assertEvenDistribution[T: Numeric: Generator](n: Int, lim: Limits[T]): Assertion = {
    val gen: RandomT[T] = RandomRanges(lim)
    val xs: Seq[T] = (0 to n).map { _: Int => gen.randomT() }
    val (mean, stdDev) = meanAndStandardDeviation(xs)
    val tolerance: Double = 4 * stdDev
    val halfWay: Double = midPointOf(lim)
    assert(mean > halfWay - tolerance && mean < halfWay + tolerance)
  }

}
