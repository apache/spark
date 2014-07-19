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

import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.{DistributionGenerator, PoissonGenerator, NormalGenerator}
import org.apache.spark.mllib.rdd.RandomRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.mllib.linalg.Vector

class RandomRDDGenerators(sc: SparkContext) {
// made it into a class instead of an object so the sc only needs to be set once

  def normalRDD(numDataPoints: Long,
      numPartitions: Int = 1,
      mean: Double,
      variance: Double,
      seed: Long = Utils.random.nextLong): RDD[Double] = {
    val normal = new NormalGenerator()(mean, variance)
    randomRDD(numDataPoints, numPartitions, normal, seed)
  }

  def poissonRDD(numDataPoints: Long,
                 numPartitions: Int = 1,
                 mean: Double,
                 seed: Long = Utils.random.nextLong) : RDD[Double] = {
    val poisson = new PoissonGenerator(mean)
    randomRDD(numDataPoints, numPartitions, poisson, seed)
  }

  def randomRDD(numDataPoints: Long,
                numPartitions: Int = 1,
                distribution: DistributionGenerator,
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
                      rng: DistributionGenerator,
                      seed: Long = Utils.random.nextLong): RDD[Vector] = ???

}