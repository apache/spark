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

import scala.util.Random

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import org.apache.spark.util.random.Pseudorandom

/**
 * Trait for random number generators that generate i.i.d values from a distribution
 */
trait DistributionGenerator extends Pseudorandom with Cloneable {

  /**
   * @return An i.i.d sample as a Double from an underlying distribution
   */
  def nextValue(): Double

  /**
   * @return A copy of the current DistributionGenerator object
   */
  def clone(): DistributionGenerator

}

class NormalGenerator(val mean: Double = 0.0, val stddev: Double = 1.0)
  extends DistributionGenerator {

  require(stddev >= 0.0, "Standard deviation cannot be negative.")

  private val random = new Random()

  /**
   * @return An i.i.d sample as a Double from the Normal distribution
   */
  override def nextValue(): Double = random.nextGaussian()

  /** Set random seed. */
  override def setSeed(seed: Long) = random.setSeed(seed)
}

class PoissonGenerator(val lambda: Double = 0.0) extends DistributionGenerator {

  private var rng = new Poisson(lambda, new DRand)
  /**
   * @return An i.i.d sample as a Double from the Poisson distribution
   */
  override def nextValue(): Double = rng.nextDouble()

  /** Set random seed. */
  override def setSeed(seed: Long) {
    rng = new Poisson(lambda, new DRand(seed.toInt))
  }
}