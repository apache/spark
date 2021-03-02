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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning.RandomRanges._

case class Limits[T: Numeric](x: T, y: T)

private[ml] abstract class RandomT[T: Numeric] {
  def randomT(): T
  def randomTLog(n: Int): T
}

abstract class Generator[T: Numeric] {
  def apply(lim: Limits[T]): RandomT[T]
}

object RandomRanges {

  private val rnd = new scala.util.Random

  private[tuning] def randomBigInt0To(x: BigInt): BigInt = {
    var randVal = BigInt(x.bitLength, rnd)
    while (randVal > x) {
      randVal = BigInt(x.bitLength, rnd)
    }
    randVal
  }

  private[ml] def bigIntBetween(lower: BigInt, upper: BigInt): BigInt = {
    val diff: BigInt = upper - lower
    randomBigInt0To(diff) + lower
  }

  private def randomBigDecimalBetween(lower: BigDecimal, upper: BigDecimal): BigDecimal = {
    val zeroCenteredRnd: BigDecimal = BigDecimal(rnd.nextDouble() - 0.5)
    val range: BigDecimal = upper - lower
    val halfWay: BigDecimal = lower + range / 2
    (zeroCenteredRnd * range) + halfWay
  }

  implicit object DoubleGenerator extends Generator[Double] {
    def apply(limits: Limits[Double]): RandomT[Double] = new RandomT[Double] {
      import limits._
      val lower: Double = math.min(x, y)
      val upper: Double = math.max(x, y)

      override def randomTLog(n: Int): Double =
        RandomRanges.randomLog(lower, upper, n)

      override def randomT(): Double =
        randomBigDecimalBetween(BigDecimal(lower), BigDecimal(upper)).doubleValue
    }
  }

  implicit object FloatGenerator extends Generator[Float] {
    def apply(limits: Limits[Float]): RandomT[Float] = new RandomT[Float] {
      import limits._
      val lower: Float = math.min(x, y)
      val upper: Float = math.max(x, y)

      override def randomTLog(n: Int): Float =
        RandomRanges.randomLog(lower, upper, n).toFloat

      override def randomT(): Float =
        randomBigDecimalBetween(BigDecimal(lower), BigDecimal(upper)).floatValue
    }
  }

  implicit object IntGenerator extends Generator[Int] {
    def apply(limits: Limits[Int]): RandomT[Int] = new RandomT[Int] {
      import limits._
      val lower: Int = math.min(x, y)
      val upper: Int = math.max(x, y)

      override def randomTLog(n: Int): Int =
        RandomRanges.randomLog(lower, upper, n).toInt

      override def randomT(): Int =
        bigIntBetween(BigInt(lower), BigInt(upper)).intValue
    }
  }

  private[ml] def logN(x: Double, base: Int): Double = math.log(x) / math.log(base)

  private[ml] def randomLog(lower: Double, upper: Double, n: Int): Double = {
    val logLower: Double = logN(lower, n)
    val logUpper: Double = logN(upper, n)
    val logLimits: Limits[Double] = Limits(logLower, logUpper)
    val rndLogged: RandomT[Double] = RandomRanges(logLimits)
    math.pow(n, rndLogged.randomT())
  }

  private[ml] def apply[T: Generator](lim: Limits[T])(implicit t: Generator[T]): RandomT[T] = t(lim)

}

/**
 * "For any distribution over a sample space with a finite maximum, the maximum of 60 random
 * observations lies within the top 5% of the true maximum, with 95% probability"
 * - Evaluating Machine Learning Models by Alice Zheng
 * https://www.oreilly.com/library/view/evaluating-machine-learning/9781492048756/ch04.html
 *
 * Note: if you want more sophisticated hyperparameter tuning, consider Python libraries
 * such as Hyperopt.
 */
@Since("3.2.0")
class ParamRandomBuilder extends ParamGridBuilder {
  def addRandom[T: Generator](param: Param[T], lim: Limits[T], n: Int): this.type = {
    val gen: RandomT[T] = RandomRanges(lim)
    addGrid(param, (1 to n).map { _: Int => gen.randomT() })
  }

  def addLog10Random[T: Generator](param: Param[T], lim: Limits[T], n: Int): this.type =
    addLogRandom(param, lim, n, 10)

  private def addLogRandom[T: Generator](param: Param[T], lim: Limits[T],
                                         n: Int, base: Int): this.type = {
    val gen: RandomT[T] = RandomRanges(lim)
    addGrid(param, (1 to n).map { _: Int => gen.randomTLog(base) })
  }

  // specialized versions for Java.

  def addRandom(param: DoubleParam, x: Double, y: Double, n: Int): this.type =
    addRandom(param, Limits(x, y), n)(DoubleGenerator)

  def addLog10Random(param: DoubleParam, x: Double, y: Double, n: Int): this.type =
    addLogRandom(param, Limits(x, y), n, 10)(DoubleGenerator)

  def addRandom(param: FloatParam, x: Float, y: Float, n: Int): this.type =
    addRandom(param, Limits(x, y), n)(FloatGenerator)

  def addLog10Random(param: FloatParam, x: Float, y: Float, n: Int): this.type =
    addLogRandom(param, Limits(x, y), n, 10)(FloatGenerator)

  def addRandom(param: IntParam, x: Int, y: Int, n: Int): this.type =
    addRandom(param, Limits(x, y), n)(IntGenerator)

  def addLog10Random(param: IntParam, x: Int, y: Int, n: Int): this.type =
    addLogRandom(param, Limits(x, y), n, 10)(IntGenerator)

}
