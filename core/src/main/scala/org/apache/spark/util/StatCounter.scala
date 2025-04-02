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

package org.apache.spark.util

import org.apache.spark.annotation.Since

/**
 * A class for tracking the statistics of a set of numbers (count, mean and variance) in a
 * numerically robust way. Includes support for merging two StatCounters. Based on Welford
 * and Chan's <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance">
 * algorithms</a> for running variance.
 *
 * @constructor Initialize the StatCounter with the given values.
 */
class StatCounter(values: IterableOnce[Double]) extends Serializable {
  private var n: Long = 0     // Running count of our values
  private var mu: Double = 0  // Running mean of our values
  private var m2: Double = 0  // Running variance numerator (sum of (x - mean)^2)
  private var maxValue: Double = Double.NegativeInfinity // Running max of our values
  private var minValue: Double = Double.PositiveInfinity // Running min of our values

  merge(values)

  /** Initialize the StatCounter with no values. */
  def this() = this(Nil)

  /** Add a value into this StatCounter, updating the internal statistics. */
  def merge(value: Double): StatCounter = {
    val delta = value - mu
    n += 1
    mu += delta / n
    m2 += delta * (value - mu)
    maxValue = math.max(maxValue, value)
    minValue = math.min(minValue, value)
    this
  }

  /** Add multiple values into this StatCounter, updating the internal statistics. */
  def merge(values: IterableOnce[Double]): StatCounter = {
    values.iterator.foreach(v => merge(v))
    this
  }

  /** Merge another StatCounter into this one, adding up the internal statistics. */
  def merge(other: StatCounter): StatCounter = {
    if (other == this) {
      merge(other.copy())  // Avoid overwriting fields in a weird order
    } else {
      if (n == 0) {
        mu = other.mu
        m2 = other.m2
        n = other.n
        maxValue = other.maxValue
        minValue = other.minValue
      } else if (other.n != 0) {
        val delta = other.mu - mu
        if (other.n * 10 < n) {
          mu = mu + (delta * other.n) / (n + other.n)
        } else if (n * 10 < other.n) {
          mu = other.mu - (delta * n) / (n + other.n)
        } else {
          mu = (mu * n + other.mu * other.n) / (n + other.n)
        }
        m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n)
        n += other.n
        maxValue = math.max(maxValue, other.maxValue)
        minValue = math.min(minValue, other.minValue)
      }
      this
    }
  }

  /** Clone this StatCounter */
  def copy(): StatCounter = {
    val other = new StatCounter
    other.n = n
    other.mu = mu
    other.m2 = m2
    other.maxValue = maxValue
    other.minValue = minValue
    other
  }

  def count: Long = n

  def mean: Double = mu

  def sum: Double = n * mu

  def max: Double = maxValue

  def min: Double = minValue

  /** Return the population variance of the values. */
  def variance: Double = popVariance

  /**
   * Return the population variance of the values.
   */
  @Since("2.1.0")
  def popVariance: Double = {
    if (n == 0) {
      Double.NaN
    } else {
      m2 / n
    }
  }

  /**
   * Return the sample variance, which corrects for bias in estimating the variance by dividing
   * by N-1 instead of N.
   */
  def sampleVariance: Double = {
    if (n <= 1) {
      Double.NaN
    } else {
      m2 / (n - 1)
    }
  }

  /** Return the population standard deviation of the values. */
  def stdev: Double = popStdev

  /**
   * Return the population standard deviation of the values.
   */
  @Since("2.1.0")
  def popStdev: Double = math.sqrt(popVariance)

  /**
   * Return the sample standard deviation of the values, which corrects for bias in estimating the
   * variance by dividing by N-1 instead of N.
   */
  def sampleStdev: Double = math.sqrt(sampleVariance)

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f, max: %f, min: %f)".format(count, mean, stdev, max, min)
  }
}

object StatCounter {
  /** Build a StatCounter from a list of values. */
  def apply(values: IterableOnce[Double]): StatCounter = new StatCounter(values)

  /** Build a StatCounter from a list of values passed as variable-length arguments. */
  def apply(values: Double*): StatCounter = new StatCounter(values)
}
