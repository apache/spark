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

package org.apache.spark.util.random

import java.util.Random

import scala.reflect.ClassTag

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A pseudorandom sampler. It is possible to change the sampled item type. For example, we might
 * want to add weights for stratified sampling or importance sampling. Should only use
 * transformations that are tied to the sampler and cannot be applied after sampling.
 *
 * @tparam T item type
 * @tparam U sampled item type
 */
@DeveloperApi
trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {

  /** take a random sample */
  def sample(items: Iterator[T]): Iterator[U] =
    items.filter(_ => sample() > 0).asInstanceOf[Iterator[U]]

  /**
   * Whether to sample the next item or not.
   * Return how many times the next item will be sampled. Return 0 if it is not sampled.
   */
  def sample(): Int

  /** return a copy of the RandomSampler object */
  override def clone: RandomSampler[T, U] =
    throw new UnsupportedOperationException("clone() is not implemented.")
}

private[spark]
object RandomSampler {
  /** Default random number generator used by random samplers. */
  def newDefaultRNG: Random = new XORShiftRandom

  /**
   * Default maximum gap-sampling fraction.
   * For sampling fractions <= this value, the gap sampling optimization will be applied.
   * Above this value, it is assumed that "traditional" Bernoulli sampling is faster.  The
   * optimal value for this will depend on the RNG.  More expensive RNGs will tend to make
   * the optimal value higher.  The most reliable way to determine this value for a new RNG
   * is to experiment.  When tuning for a new RNG, I would expect a value of 0.5 to be close
   * in most cases, as an initial guess.
   */
  val defaultMaxGapSamplingFraction = 0.4

  /**
   * Default epsilon for floating point numbers sampled from the RNG.
   * The gap-sampling compute logic requires taking log(x), where x is sampled from an RNG.
   * To guard against errors from taking log(0), a positive epsilon lower bound is applied.
   * A good value for this parameter is at or near the minimum positive floating
   * point value returned by "nextDouble()" (or equivalent), for the RNG being used.
   */
  val rngEpsilon = 5e-11

  /**
   * Sampling fraction arguments may be results of computation, and subject to floating
   * point jitter.  I check the arguments with this epsilon slop factor to prevent spurious
   * warnings for cases such as summing some numbers to get a sampling fraction of 1.000000001
   */
  val roundingEpsilon = 1e-6
}

/**
 * :: DeveloperApi ::
 * A sampler based on Bernoulli trials for partitioning a data sequence.
 *
 * @param lb lower bound of the acceptance range
 * @param ub upper bound of the acceptance range
 * @param complement whether to use the complement of the range specified, default to false
 * @tparam T item type
 */
@DeveloperApi
class BernoulliCellSampler[T](lb: Double, ub: Double, complement: Boolean = false)
  extends RandomSampler[T, T] {

  /** epsilon slop to avoid failure from floating point jitter. */
  require(
    lb <= (ub + RandomSampler.roundingEpsilon),
    s"Lower bound ($lb) must be <= upper bound ($ub)")
  require(
    lb >= (0.0 - RandomSampler.roundingEpsilon),
    s"Lower bound ($lb) must be >= 0.0")
  require(
    ub <= (1.0 + RandomSampler.roundingEpsilon),
    s"Upper bound ($ub) must be <= 1.0")

  private val rng: Random = new XORShiftRandom

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  override def sample(): Int = {
    if (ub - lb <= 0.0) {
      if (complement) 1 else 0
    } else {
      val x = rng.nextDouble()
      val n = if ((x >= lb) && (x < ub)) 1 else 0
      if (complement) 1 - n else n
    }
  }

  /**
   *  Return a sampler that is the complement of the range specified of the current sampler.
   */
  def cloneComplement(): BernoulliCellSampler[T] =
    new BernoulliCellSampler[T](lb, ub, !complement)

  override def clone: BernoulliCellSampler[T] = new BernoulliCellSampler[T](lb, ub, complement)
}


/**
 * :: DeveloperApi ::
 * A sampler based on Bernoulli trials.
 *
 * @param fraction the sampling fraction, aka Bernoulli sampling probability
 * @tparam T item type
 */
@DeveloperApi
class BernoulliSampler[T: ClassTag](fraction: Double) extends RandomSampler[T, T] {

  /** epsilon slop to avoid failure from floating point jitter */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon)
      && fraction <= (1.0 + RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be on interval [0, 1]")

  private val rng: Random = RandomSampler.newDefaultRNG

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  private lazy val gapSampling: GapSampling =
    new GapSampling(fraction, rng, RandomSampler.rngEpsilon)

  override def sample(): Int = {
    if (fraction <= 0.0) {
      0
    } else if (fraction >= 1.0) {
      1
    } else if (fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
      gapSampling.sample()
    } else {
      if (rng.nextDouble() <= fraction) {
        1
      } else {
        0
      }
    }
  }

  override def clone: BernoulliSampler[T] = new BernoulliSampler[T](fraction)
}


/**
 * :: DeveloperApi ::
 * A sampler for sampling with replacement, based on values drawn from Poisson distribution.
 *
 * @param fraction the sampling fraction (with replacement)
 * @param useGapSamplingIfPossible if true, use gap sampling when sampling ratio is low.
 * @tparam T item type
 */
@DeveloperApi
class PoissonSampler[T](
    fraction: Double,
    useGapSamplingIfPossible: Boolean) extends RandomSampler[T, T] {

  def this(fraction: Double) = this(fraction, useGapSamplingIfPossible = true)

  /** Epsilon slop to avoid failure from floating point jitter. */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be >= 0")

  // PoissonDistribution throws an exception when fraction <= 0
  // If fraction is <= 0, Iterator.empty is used below, so we can use any placeholder value.
  private val rng = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
  private val rngGap = RandomSampler.newDefaultRNG

  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
    rngGap.setSeed(seed)
  }

  private lazy val gapSamplingReplacement =
    new GapSamplingReplacement(fraction, rngGap, RandomSampler.rngEpsilon)

  override def sample(): Int = {
    if (fraction <= 0.0) {
      0
    } else if (useGapSamplingIfPossible &&
               fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
      gapSamplingReplacement.sample()
    } else {
      rng.sample()
    }
  }

  override def sample(items: Iterator[T]): Iterator[T] = {
    if (fraction <= 0.0) {
      Iterator.empty
    } else {
      val useGapSampling = useGapSamplingIfPossible &&
        fraction <= RandomSampler.defaultMaxGapSamplingFraction

      items.flatMap { item =>
        val count = if (useGapSampling) gapSamplingReplacement.sample() else rng.sample()
        if (count == 0) Iterator.empty else Iterator.fill(count)(item)
      }
    }
  }

  override def clone: PoissonSampler[T] = new PoissonSampler[T](fraction, useGapSamplingIfPossible)
}


private[spark]
class GapSampling(
    f: Double,
    rng: Random = RandomSampler.newDefaultRNG,
    epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {

  require(f > 0.0  &&  f < 1.0, s"Sampling fraction ($f) must reside on open interval (0, 1)")
  require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")

  private val lnq = math.log1p(-f)

  /** Return 1 if the next item should be sampled. Otherwise, return 0. */
  def sample(): Int = {
    if (countForDropping > 0) {
      countForDropping -= 1
      0
    } else {
      advance()
      1
    }
  }

  private var countForDropping: Int = 0

  /**
   * Decide the number of elements that won't be sampled,
   * according to geometric dist P(k) = (f)(1-f)^k.
   */
  private def advance(): Unit = {
    val u = math.max(rng.nextDouble(), epsilon)
    countForDropping = (math.log(u) / lnq).toInt
  }

  /** advance to first sample as part of object construction. */
  advance()
  // Attempting to invoke this closer to the top with other object initialization
  // was causing it to break in strange ways, so I'm invoking it last, which seems to
  // work reliably.
}


private[spark]
class GapSamplingReplacement(
    val f: Double,
    val rng: Random = RandomSampler.newDefaultRNG,
    epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {

  require(f > 0.0, s"Sampling fraction ($f) must be > 0")
  require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")

  protected val q = math.exp(-f)

  /**
   * Sample from Poisson distribution, conditioned such that the sampled value is >= 1.
   * This is an adaptation from the algorithm for Generating Poisson distributed random variables:
   * http://en.wikipedia.org/wiki/Poisson_distribution
   */
  protected def poissonGE1: Int = {
    // simulate that the standard poisson sampling
    // gave us at least one iteration, for a sample of >= 1
    var pp = q + ((1.0 - q) * rng.nextDouble())
    var r = 1

    // now continue with standard poisson sampling algorithm
    pp *= rng.nextDouble()
    while (pp > q) {
      r += 1
      pp *= rng.nextDouble()
    }
    r
  }
  private var countForDropping: Int = 0

  def sample(): Int = {
    if (countForDropping > 0) {
      countForDropping -= 1
      0
    } else {
      val r = poissonGE1
      advance()
      r
    }
  }

  /**
   * Skip elements with replication factor zero (i.e. elements that won't be sampled).
   * Samples 'k' from geometric distribution  P(k) = (1-q)(q)^k, where q = e^(-f), that is
   * q is the probability of Poisson(0; f)
   */
  private def advance(): Unit = {
    val u = math.max(rng.nextDouble(), epsilon)
    countForDropping = (math.log(u) / (-f)).toInt
  }

  /** advance to first sample as part of object construction. */
  advance()
  // Attempting to invoke this closer to the top with other object initialization
  // was causing it to break in strange ways, so I'm invoking it last, which seems to
  // work reliably.
}
