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

/**
 * Generator methods for creating RDDs comprised of i.i.d samples from some distribution.
 *
 * TODO Generate RDD[Vector] from multivariate distributions.
 */
object RandomRDDGenerators {

  /**
   * Generates an RDD comprised of i.i.d samples from the uniform distribution on [0.0, 1.0].
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples ~ U[0.0, 1.0].
   */
  def uniformRDD(sc: SparkContext, size: Long, numPartitions: Int, seed: Long): RDD[Double] = {
    val uniform = new UniformGenerator()
    randomRDD(sc, uniform,  size, numPartitions, seed)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the uniform distribution on [0.0, 1.0].
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ U[0.0, 1.0].
   */
  def uniformRDD(sc: SparkContext, size: Long, numPartitions: Int): RDD[Double] = {
    uniformRDD(sc, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the uniform distribution on [0.0, 1.0].
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ U[0.0, 1.0].
   */
  def uniformRDD(sc: SparkContext, size: Long): RDD[Double] = {
    uniformRDD(sc, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples ~ N(0.0, 1.0).
   */
  def normalRDD(sc: SparkContext, size: Long, numPartitions: Int, seed: Long): RDD[Double] = {
    val normal = new StandardNormalGenerator()
    randomRDD(sc, normal, size, numPartitions, seed)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ N(0.0, 1.0).
   */
  def normalRDD(sc: SparkContext, size: Long, numPartitions: Int): RDD[Double] = {
    normalRDD(sc, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the standard normal distribution.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ N(0.0, 1.0).
   */
  def normalRDD(sc: SparkContext, size: Long): RDD[Double] = {
    normalRDD(sc, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples ~ Pois(mean).
   */
  def poissonRDD(sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val poisson = new PoissonGenerator(mean)
    randomRDD(sc, poisson, size, numPartitions, seed)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ Pois(mean).
   */
  def poissonRDD(sc: SparkContext, mean: Double, size: Long, numPartitions: Int): RDD[Double] = {
    poissonRDD(sc, mean, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples from the Poisson distribution with the input mean.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ Pois(mean).
   */
  def poissonRDD(sc: SparkContext, mean: Double, size: Long): RDD[Double] = {
    poissonRDD(sc, mean, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples produced by the input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples produced by generator.
   */
  def randomRDD(sc: SparkContext,
      generator: DistributionGenerator,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    new RandomRDD(sc, size, numPartitions, generator, seed)
  }

  /**
   * Generates an RDD comprised of i.i.d samples produced by the input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples produced by generator.
   */
  def randomRDD(sc: SparkContext,
      generator: DistributionGenerator,
      size: Long,
      numPartitions: Int): RDD[Double] = {
    randomRDD(sc, generator, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD comprised of i.i.d samples produced by the input DistributionGenerator.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples produced by generator.
   */
  def randomRDD(sc: SparkContext,
      generator: DistributionGenerator,
      size: Long): RDD[Double] = {
    randomRDD(sc, generator, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * uniform distribution on [0.0 1.0].
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ U[0.0, 1.0].
   */
  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val uniform = new UniformGenerator()
    randomVectorRDD(sc, uniform, numRows, numColumns, numPartitions, seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * uniform distribution on [0.0 1.0].
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ U[0.0, 1.0].
   */
  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numColumns, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * uniform distribution on [0.0 1.0].
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ U[0.0, 1.0].
   */
  def uniformVectorRDD(sc: SparkContext, numRows: Long, numColumns: Int): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ N(0.0, 1.0).
   */
  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val uniform = new StandardNormalGenerator()
    randomVectorRDD(sc, uniform, numRows, numColumns, numPartitions, seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ N(0.0, 1.0).
   */
  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numColumns, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * standard normal distribution.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ N(0.0, 1.0).
   */
  def normalVectorRDD(sc: SparkContext, numRows: Long, numColumns: Int): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numColumns, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ Pois(mean).
   */
  def poissonVectorRDD(sc: SparkContext,
      mean: Double,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val poisson = new PoissonGenerator(mean)
    randomVectorRDD(sc, poisson, numRows, numColumns, numPartitions, seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ Pois(mean).
   */
  def poissonVectorRDD(sc: SparkContext,
      mean: Double,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int): RDD[Vector] = {
    poissonVectorRDD(sc, mean, numRows, numColumns, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples drawn from the
   * Poisson distribution with the input mean.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ Pois(mean).
   */
  def poissonVectorRDD(sc: SparkContext,
      mean: Double,
      numRows: Long,
      numColumns: Int): RDD[Vector] = {
    poissonVectorRDD(sc, mean, numRows, numColumns, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples produced by the
   * input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d samples produced by generator.
   */
  def randomVectorRDD(sc: SparkContext,
      generator: DistributionGenerator,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    new RandomVectorRDD(sc, numRows, numColumns, numPartitions, generator, seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples produced by the
   * input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d samples produced by generator.
   */
  def randomVectorRDD(sc: SparkContext,
      generator: DistributionGenerator,
      numRows: Long,
      numColumns: Int,
      numPartitions: Int): RDD[Vector] = {
    randomVectorRDD(sc, generator, numRows, numColumns, numPartitions, Utils.random.nextLong)
  }

  /**
   * Generates an RDD[Vector] with vectors containing i.i.d samples produced by the
   * input DistributionGenerator.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numColumns Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d samples produced by generator.
   */
  def randomVectorRDD(sc: SparkContext,
      generator: DistributionGenerator,
      numRows: Long,
      numColumns: Int): RDD[Vector] = {
    randomVectorRDD(sc, generator, numRows, numColumns,
      sc.defaultParallelism, Utils.random.nextLong)
  }
}
