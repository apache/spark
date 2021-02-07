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

import org.apache.spark.ml.param._

case class Limits[T: Numeric](x: T, y: T)

abstract class RandomT[T: Numeric] {
  def randomT(): T
}

abstract class Generator[T: Numeric] {
  def apply(lim: Limits[T]): RandomT[T]
}

object RandomRanges {

  val rnd = new scala.util.Random

  private[tuning] def randomBigInt0To(x: BigInt): BigInt = {
    var randVal = BigInt(x.bitLength, rnd)
    while (randVal > x) {
      randVal = BigInt(x.bitLength, rnd)
    }
    randVal
  }

  def bigIntBetween(lower: BigInt, upper: BigInt): BigInt = {
    val diff: BigInt = upper - lower
    randomBigInt0To(diff) + lower
  }

  private def randomBigIntIn(lower: BigDecimal, upper: BigDecimal): BigDecimal = {
    val zeroCenteredRnd: BigDecimal = BigDecimal(rnd.nextDouble() - 0.5)
    val range: BigDecimal = upper - lower
    val halfWay: BigDecimal = lower + range / 2
    (zeroCenteredRnd * range) + halfWay
  }

  implicit object DoubleGenerator extends Generator[Double] {
    def apply(limits: Limits[Double]): RandomT[Double] = new RandomT[Double] {
      override def randomT(): Double = {
        import limits._
        randomBigIntIn(BigDecimal(math.min(x, y)), BigDecimal(math.max(x, y))).doubleValue()
      }
    }
  }

  implicit object FloatGenerator extends Generator[Float] {
    def apply(limits: Limits[Float]): RandomT[Float] = new RandomT[Float] {
      override def randomT(): Float = {
        import limits._
        randomBigIntIn(BigDecimal(math.min(x, y)), BigDecimal(math.max(x, y))).floatValue()
      }
    }
  }

  implicit object IntGenerator extends Generator[Int] {
    def apply(limits: Limits[Int]): RandomT[Int] = new RandomT[Int] {
      override def randomT(): Int = {
        import limits._
        bigIntBetween(BigInt(math.min(x, y)), BigInt(math.max(x, y))).intValue()
      }
    }
  }

  implicit object LongGenerator extends Generator[Long] {
    def apply(limits: Limits[Long]): RandomT[Long] = new RandomT[Long] {
      override def randomT(): Long = {
        import limits._
        bigIntBetween(BigInt(math.min(x, y)), BigInt(math.max(x, y))).longValue()
      }
    }
  }

  def apply[T: Generator](lim: Limits[T])(implicit t: Generator[T]): RandomT[T] = t(lim)

}

/**
 * "For any distribution over a sample space with a finite maximum, the maximum of 60 random
 * observations lies within the top 5% of the true maximum, with 95% probability"
 * - Evaluating Machine Learning Models by Alice Zheng
 * https://www.oreilly.com/library/view/evaluating-machine-learning/9781492048756/ch04.html
 */
class ParamRandomBuilder extends ParamGridBuilder {

  def addRandom[T: Generator](param: Param[T], lim: Limits[T], n: Int): this.type = {
    val gen: RandomT[T] = RandomRanges(lim)
    addGrid(param, (1 to n).map { _: Int => gen.randomT() })
  }

}
