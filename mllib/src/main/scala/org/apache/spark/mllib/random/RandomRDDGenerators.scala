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

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.rdd.{RandomVectorRDD, RandomRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

// TODO add Scaladocs once API fully approved
// Alternatively, we can use the generator pattern to set numPartitions, seed, etc instead to bring
// down the number of methods here.
object RandomRDDGenerators {

  def uniformRDD(sc: SparkContext, size: Long, numPartitions: Int, seed: Long): RDD[Double] = {
    val uniform = new UniformGenerator()
    randomRDD(sc, size, numPartitions, uniform, seed)
  }

  def uniformRDD(sc: SparkContext, size: Long, seed: Long): RDD[Double] = {
    uniformRDD(sc, size, sc.defaultParallelism, seed)
  }

  def uniformRDD(sc: SparkContext, size: Long, numPartitions: Int): RDD[Double] = {
    uniformRDD(sc, size, numPartitions, Utils.random.nextLong)
  }

  def uniformRDD(sc: SparkContext, size: Long): RDD[Double] = {
    uniformRDD(sc, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  def normalRDD(sc: SparkContext, size: Long, numPartitions: Int, seed: Long): RDD[Double] = {
    val normal = new StandardNormalGenerator()
    randomRDD(sc, size, numPartitions, normal, seed)
  }

  def normalRDD(sc: SparkContext, size: Long, seed: Long): RDD[Double] = {
    normalRDD(sc, size, sc.defaultParallelism, seed)
  }

  def normalRDD(sc: SparkContext, size: Long, numPartitions: Int): RDD[Double] = {
    normalRDD(sc, size, numPartitions, Utils.random.nextLong)
  }

  def normalRDD(sc: SparkContext, size: Long): RDD[Double] = {
    normalRDD(sc, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  def poissonRDD(sc: SparkContext,
      size: Long,
      numPartitions: Int,
      mean: Double,
      seed: Long): RDD[Double] = {
    val poisson = new PoissonGenerator(mean)
    randomRDD(sc, size, numPartitions, poisson, seed)
  }

  def poissonRDD(sc: SparkContext, size: Long, mean: Double, seed: Long): RDD[Double] = {
    poissonRDD(sc, size, sc.defaultParallelism, mean, seed)
  }

  def poissonRDD(sc: SparkContext, size: Long, numPartitions: Int, mean: Double): RDD[Double] = {
    poissonRDD(sc, size, numPartitions, mean, Utils.random.nextLong)
  }

  def poissonRDD(sc: SparkContext, size: Long, mean: Double): RDD[Double] = {
    poissonRDD(sc, size, sc.defaultParallelism, mean, Utils.random.nextLong)
  }

  def randomRDD(sc: SparkContext,
      size: Long,
      numPartitions: Int,
      distribution: DistributionGenerator,
      seed: Long): RDD[Double] = {
    new RandomRDD(sc, size, numPartitions, distribution, seed)
  }

  def randomRDD(sc: SparkContext,
      size: Long,
      distribution: DistributionGenerator,
      seed: Long): RDD[Double] = {
    randomRDD(sc, size, sc.defaultParallelism, distribution, seed)
  }

  def randomRDD(sc: SparkContext,
      size: Long,
      numPartitions: Int,
      distribution: DistributionGenerator): RDD[Double] = {
    randomRDD(sc, size, numPartitions, distribution, Utils.random.nextLong)
  }

  def randomRDD(sc: SparkContext,
      size: Long,
      distribution: DistributionGenerator): RDD[Double] = {
    randomRDD(sc, size, sc.defaultParallelism, distribution, Utils.random.nextLong)
  }

  // TODO Generator RDD[Vector] from multivariate distribution

  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val uniform = new UniformGenerator()
    randomVectorRDD(sc, numRows, numColumns, numPartitions, uniform, seed)
  }

  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      seed: Long): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, seed)
  }

  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numColumns, numPartitions, Utils.random.nextLong)
  }

  def uniformVectorRDD(sc: SparkContext, numRows: Long, numColumns: Int): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, Utils.random.nextLong)
  }

  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val uniform = new StandardNormalGenerator()
    randomVectorRDD(sc, numRows, numColumns, numPartitions, uniform, seed)
  }

  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      seed: Long): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, seed)
  }

  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numColumns, numPartitions, Utils.random.nextLong)
  }

  def normalVectorRDD(sc: SparkContext, numRows: Long, numColumns: Int): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, Utils.random.nextLong)
  }

  def poissonVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      mean: Double,
      seed: Long): RDD[Vector] = {
    val poisson = new PoissonGenerator(mean)
    randomVectorRDD(sc, numRows, numColumns, numPartitions, poisson, seed)
  }

  def poissonVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      mean: Double,
      seed: Long): RDD[Vector] = {
    poissonVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, mean, seed)
  }

  def poissonVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      mean: Double): RDD[Vector] = {
    poissonVectorRDD(sc, numRows, numColumns, numPartitions, mean, Utils.random.nextLong)
  }

  def poissonVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      mean: Double): RDD[Vector] = {
    val poisson = new PoissonGenerator(mean)
    randomVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, poisson, Utils.random.nextLong)
  }

  def randomVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      rng: DistributionGenerator,
      seed: Long): RDD[Vector] = {
    new RandomVectorRDD(sc, numRows, numColumns, numPartitions, rng, seed)
  }

  def randomVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      rng: DistributionGenerator,
      seed: Long): RDD[Vector] = {
    randomVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, rng, seed)
  }

  def randomVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      rng: DistributionGenerator): RDD[Vector] = {
    randomVectorRDD(sc, numRows, numColumns, numPartitions, rng, Utils.random.nextLong)
  }

  def randomVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      rng: DistributionGenerator): RDD[Vector] = {
    randomVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, rng, Utils.random.nextLong)
  }
}
