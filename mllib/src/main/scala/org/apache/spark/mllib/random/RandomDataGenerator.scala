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

package org.apache.spark.mllib.random

import org.apache.commons.math3.distribution._

import org.apache.spark.annotation.{Since, DeveloperApi}
import org.apache.spark.util.random.{XORShiftRandom, Pseudorandom}

/**
 * :: DeveloperApi ::
 * Trait for random data generators that generate i.i.d. data.
 */
@DeveloperApi
@Since("1.1.0")
trait RandomDataGenerator[T] extends Pseudorandom with Serializable {

  /**
   * Returns an i.i.d. sample as a generic type from an underlying distribution.
   */
  @Since("1.1.0")
  def nextValue(): T

  /**
   * Returns a copy of the RandomDataGenerator with a new instance of the rng object used in the
   * class when applicable for non-locking concurrent usage.
   */
  @Since("1.1.0")
  def copy(): RandomDataGenerator[T]
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from U[0.0, 1.0]
 */
@DeveloperApi
@Since("1.1.0")
class UniformGenerator extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  @Since("1.1.0")
  override def nextValue(): Double = {
    random.nextDouble()
  }

  @Since("1.1.0")
  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  @Since("1.1.0")
  override def copy(): UniformGenerator = new UniformGenerator()
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the standard normal distribution.
 */
@DeveloperApi
@Since("1.1.0")
class StandardNormalGenerator extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  @Since("1.1.0")
  override def nextValue(): Double = {
      random.nextGaussian()
  }

  @Since("1.1.0")
  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  @Since("1.1.0")
  override def copy(): StandardNormalGenerator = new StandardNormalGenerator()
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the Poisson distribution with the given mean.
 *
 * @param mean mean for the Poisson distribution.
 */
@DeveloperApi
@Since("1.1.0")
class PoissonGenerator @Since("1.1.0") (
    @Since("1.1.0") val mean: Double) extends RandomDataGenerator[Double] {

  private val rng = new PoissonDistribution(mean)

  @Since("1.1.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.1.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.1.0")
  override def copy(): PoissonGenerator = new PoissonGenerator(mean)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the exponential distribution with the given mean.
 *
 * @param mean mean for the exponential distribution.
 */
@DeveloperApi
@Since("1.3.0")
class ExponentialGenerator @Since("1.3.0") (
    @Since("1.3.0") val mean: Double) extends RandomDataGenerator[Double] {

  private val rng = new ExponentialDistribution(mean)

  @Since("1.3.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.3.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.3.0")
  override def copy(): ExponentialGenerator = new ExponentialGenerator(mean)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the gamma distribution with the given shape and scale.
 *
 * @param shape shape for the gamma distribution.
 * @param scale scale for the gamma distribution
 */
@DeveloperApi
@Since("1.3.0")
class GammaGenerator @Since("1.3.0") (
    @Since("1.3.0") val shape: Double,
    @Since("1.3.0") val scale: Double) extends RandomDataGenerator[Double] {

  private val rng = new GammaDistribution(shape, scale)

  @Since("1.3.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.3.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.3.0")
  override def copy(): GammaGenerator = new GammaGenerator(shape, scale)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the log normal distribution with the
 * given mean and standard deviation.
 *
 * @param mean mean for the log normal distribution.
 * @param std standard deviation for the log normal distribution
 */
@DeveloperApi
@Since("1.3.0")
class LogNormalGenerator @Since("1.3.0") (
    @Since("1.3.0") val mean: Double,
    @Since("1.3.0") val std: Double) extends RandomDataGenerator[Double] {

  private val rng = new LogNormalDistribution(mean, std)

  @Since("1.3.0")
  override def nextValue(): Double = rng.sample()

  @Since("1.3.0")
  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  @Since("1.3.0")
  override def copy(): LogNormalGenerator = new LogNormalGenerator(mean, std)
}

/**
 * :: DeveloperApi ::
 * Generates i.i.d. samples from the Weibull distribution with the
 * given shape and scale parameter.
 *
 * @param alpha shape parameter for the Weibull distribution.
 * @param beta scale parameter for the Weibull distribution.
 */
@DeveloperApi
class WeibullGenerator(
    val alpha: Double,
    val beta: Double) extends RandomDataGenerator[Double] {

  private val rng = new WeibullDistribution(alpha, beta)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): WeibullGenerator = new WeibullGenerator(alpha, beta)
}
