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

package org.apache.spark.mllib.stat

import cern.jet.random.engine.{DRand}
import cern.jet.random.Poisson
import org.apache.spark.SparkContext
import org.apache.spark.mllib.rdd.RandomRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.mllib.linalg.Vector

import scala.util.Random

class RandomRDDGenerators(sc: SparkContext) {
// made it into a class instead of an object so the sc only needs to be set once

  def normalRDD(numDataPoints: Long,
      numPartitions: Int = 1,
      mean: Double,
      variance: Double,
      seed: Long = Utils.random.nextLong): RDD[Double] = {
    val normal = new NormalDistribution(mean, variance)
    randomRDD(numDataPoints, numPartitions, normal, seed)
  }

  def poissonRDD(numDataPoints: Long,
                 numPartitions: Int = 1,
                 mean: Double,
                 seed: Long = Utils.random.nextLong) : RDD[Double] = {
    val poisson = new PoissonDistribution(mean)
    randomRDD(numDataPoints, numPartitions, poisson, seed)
  }

  def randomRDD(numDataPoints: Long,
                numPartitions: Int = 1,
                distribution: Distribution,
                seed: Long = Utils.random.nextLong) : RDD[Double] = {
    new RandomRDD(sc, numPartitions, numDataPoints, distribution, seed)
  }

  def normalVectorRDD(numRows: Int,
                      numColumns: Int,
                      numPartitions: Int = 1,
                      mean: Double,
                      variance: Double,
                      seed: Long = Utils.random.nextLong): RDD[Vector] = ???

  def poissonVectorRDD(numRows: Int,
                       numColumns: Int,
                       numPartitions: Int = 1,
                       mean: Double,
                       seed: Long = Utils.random.nextLong): RDD[Vector] = ???

  def randomVectorRDD(numRows: Int,
                      numColumns: Int,
                      numPartitions: Int = 1,
                      rng: Distribution,
                      seed: Long = Utils.random.nextLong): RDD[Vector] = ???

}

trait Distribution {

  /**
   * Get a randomly generated value from the distribution
   */
  def nextDouble(): Double

  /**
   * Set the seed for the underlying random number generator
   */
  def setSeed(seed: Long): Unit

  /**
   * Make a copy of this distribution object.
   *
   * This is essential because most random number generator implementations are locking,
   * but we need to be able to invoke nextDouble in parallel for each partition.
   */
  def copy(): Distribution
}

class NormalDistribution(val mean: Double, val variance: Double) extends Distribution {

  require(variance >= 0.0, "Variance cannot be negative.")

  private val random = new Random()
  private val stddev = math.sqrt(variance)

  /**
   * Get a randomly generated value from the distribution
   */
  override def nextDouble(): Double = random.nextGaussian() * stddev + mean

  /**
   * Set the seed for the underlying random number generator
   */
  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  /**
   * Make a copy of this distribution object.
   *
   * This is essential because most random number generator implementations are locking,
   * but we need to be able to invoke nextDouble in parallel for each partition.
   */
  override def copy(): Distribution = new NormalDistribution(mean, variance)
}


class PoissonDistribution(val mean: Double) extends Distribution {

  private var random = new DRand()
  private var poisson = new Poisson(mean, random)

  /**
   * Get a randomly generated value from the distribution
   */
  override def nextDouble(): Double = poisson.nextDouble()

  /**
   * Set the seed for the underlying random number generator
   */
  override def setSeed(seed: Long): Unit = {
    //This is kind of questionable. Better suggestions?
    random = new DRand(seed.toInt)
    poisson = new Poisson(mean, random)
  }

  /**
   * Make a copy of this distribution object.
   *
   * This is essential because most random number generator implementations are locking,
   * but we need to be able to invoke nextDouble in parallel for each partition.
   */
  override def copy(): Distribution = new PoissonDistribution(mean)
}

// alternative "in-house" implementation of Poisson without using colt
class PoissonDistributionNative(val mean: Double) extends Distribution {

  private val random = new Random()

  /**
   * Get a randomly generated value from the distribution
   */
  override def nextDouble(): Double = {
    var k = 0
    var p = random.nextDouble()
    val target = math.exp(-mean)
    while (p > target) {
      p *= random.nextDouble()
      k += 1
    }
    k
  }

  /**
   * Set the seed for the underlying random number generator
   */
  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  /**
   * Make a copy of this distribution object.
   *
   * This is essential because most random number generator implementations are locking,
   * but we need to be able to invoke nextDouble in parallel for each partition.
   */
  override def copy(): Distribution = new PoissonDistributionNative(mean)
}