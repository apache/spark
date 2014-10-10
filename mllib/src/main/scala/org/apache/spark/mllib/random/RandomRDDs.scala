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

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.rdd.{RandomVectorRDD, RandomRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * Generator methods for creating RDDs comprised of i.i.d. samples from some distribution.
 */
@Experimental
object RandomRDDs {

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the uniform distribution on [0.0, 1.0].
   *
   * To transform the distribution in the generated RDD from U[0.0, 1.0] to U[a, b], use
   * `RandomRDDGenerators.uniformRDD(sc, n, p, seed).map(v => a + (b - a) * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples ~ U[0.0, 1.0].
   */
  @Experimental
  def uniformRDD(sc: SparkContext, size: Long, numPartitions: Int, seed: Long): RDD[Double] = {
    val uniform = new UniformGenerator()
    randomRDD(sc, uniform,  size, numPartitions, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the uniform distribution on [0.0, 1.0].
   *
   * To transform the distribution in the generated RDD from U[0.0, 1.0] to U[a, b], use
   * `RandomRDDGenerators.uniformRDD(sc, n, p).map(v => a + (b - a) * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ U[0.0, 1.0].
   */
  @Experimental
  def uniformRDD(sc: SparkContext, size: Long, numPartitions: Int): RDD[Double] = {
    uniformRDD(sc, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the uniform distribution on [0.0, 1.0].
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * To transform the distribution in the generated RDD from U[0.0, 1.0] to U[a, b], use
   * `RandomRDDGenerators.uniformRDD(sc, n).map(v => a + (b - a) * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ U[0.0, 1.0].
   */
  @Experimental
  def uniformRDD(sc: SparkContext, size: Long): RDD[Double] = {
    uniformRDD(sc, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the standard normal distribution.
   *
   * To transform the distribution in the generated RDD from standard normal to some other normal
   * N(mean, sigma), use `RandomRDDGenerators.normalRDD(sc, n, p, seed).map(v => mean + sigma * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples ~ N(0.0, 1.0).
   */
  @Experimental
  def normalRDD(sc: SparkContext, size: Long, numPartitions: Int, seed: Long): RDD[Double] = {
    val normal = new StandardNormalGenerator()
    randomRDD(sc, normal, size, numPartitions, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the standard normal distribution.
   *
   * To transform the distribution in the generated RDD from standard normal to some other normal
   * N(mean, sigma), use `RandomRDDGenerators.normalRDD(sc, n, p).map(v => mean + sigma * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ N(0.0, 1.0).
   */
  @Experimental
  def normalRDD(sc: SparkContext, size: Long, numPartitions: Int): RDD[Double] = {
    normalRDD(sc, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the standard normal distribution.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * To transform the distribution in the generated RDD from standard normal to some other normal
   * N(mean, sigma), use `RandomRDDGenerators.normalRDD(sc, n).map(v => mean + sigma * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ N(0.0, 1.0).
   */
  @Experimental
  def normalRDD(sc: SparkContext, size: Long): RDD[Double] = {
    normalRDD(sc, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples ~ Pois(mean).
   */
  @Experimental
  def poissonRDD(sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val poisson = new PoissonGenerator(mean)
    randomRDD(sc, poisson, size, numPartitions, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ Pois(mean).
   */
  @Experimental
  def poissonRDD(sc: SparkContext, mean: Double, size: Long, numPartitions: Int): RDD[Double] = {
    poissonRDD(sc, mean, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples from the Poisson distribution with the input mean.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples ~ Pois(mean).
   */
  @Experimental
  def poissonRDD(sc: SparkContext, mean: Double, size: Long): RDD[Double] = {
    poissonRDD(sc, mean, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples produced by the input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Double] comprised of i.i.d. samples produced by generator.
   */
  @Experimental
  def randomRDD[T: ClassTag](sc: SparkContext,
      generator: RandomDataGenerator[T],
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[T] = {
    new RandomRDD[T](sc, size, numPartitions, generator, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples produced by the input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Double] comprised of i.i.d. samples produced by generator.
   */
  @Experimental
  def randomRDD[T: ClassTag](sc: SparkContext,
      generator: RandomDataGenerator[T],
      size: Long,
      numPartitions: Int): RDD[T] = {
    randomRDD[T](sc, generator, size, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD comprised of i.i.d. samples produced by the input DistributionGenerator.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @return RDD[Double] comprised of i.i.d. samples produced by generator.
   */
  @Experimental
  def randomRDD[T: ClassTag](sc: SparkContext,
      generator: RandomDataGenerator[T],
      size: Long): RDD[T] = {
    randomRDD[T](sc, generator, size, sc.defaultParallelism, Utils.random.nextLong)
  }

  // TODO Generate RDD[Vector] from multivariate distributions.

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * uniform distribution on [0.0 1.0].
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ U[0.0, 1.0].
   */
  @Experimental
  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val uniform = new UniformGenerator()
    randomVectorRDD(sc, uniform, numRows, numCols, numPartitions, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * uniform distribution on [0.0 1.0].
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ U[0.0, 1.0].
   */
  @Experimental
  def uniformVectorRDD(sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numCols, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * uniform distribution on [0.0 1.0].
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ U[0.0, 1.0].
   */
  @Experimental
  def uniformVectorRDD(sc: SparkContext, numRows: Long, numCols: Int): RDD[Vector] = {
    uniformVectorRDD(sc, numRows, numCols, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ N(0.0, 1.0).
   */
  @Experimental
  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val uniform = new StandardNormalGenerator()
    randomVectorRDD(sc, uniform, numRows, numCols, numPartitions, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ N(0.0, 1.0).
   */
  @Experimental
  def normalVectorRDD(sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numCols, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * standard normal distribution.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ N(0.0, 1.0).
   */
  @Experimental
  def normalVectorRDD(sc: SparkContext, numRows: Long, numCols: Int): RDD[Vector] = {
    normalVectorRDD(sc, numRows, numCols, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ Pois(mean).
   */
  @Experimental
  def poissonVectorRDD(sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    val poisson = new PoissonGenerator(mean)
    randomVectorRDD(sc, poisson, numRows, numCols, numPartitions, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ Pois(mean).
   */
  @Experimental
  def poissonVectorRDD(sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): RDD[Vector] = {
    poissonVectorRDD(sc, mean, numRows, numCols, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples drawn from the
   * Poisson distribution with the input mean.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d. samples ~ Pois(mean).
   */
  @Experimental
  def poissonVectorRDD(sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int): RDD[Vector] = {
    poissonVectorRDD(sc, mean, numRows, numCols, sc.defaultParallelism, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples produced by the
   * input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d. samples produced by generator.
   */
  @Experimental
  def randomVectorRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): RDD[Vector] = {
    new RandomVectorRDD(sc, numRows, numCols, numPartitions, generator, seed)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples produced by the
   * input DistributionGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @return RDD[Vector] with vectors containing i.i.d. samples produced by generator.
   */
  @Experimental
  def randomVectorRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int): RDD[Vector] = {
    randomVectorRDD(sc, generator, numRows, numCols, numPartitions, Utils.random.nextLong)
  }

  /**
   * :: Experimental ::
   * Generates an RDD[Vector] with vectors containing i.i.d. samples produced by the
   * input DistributionGenerator.
   * sc.defaultParallelism used for the number of partitions in the RDD.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator DistributionGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @return RDD[Vector] with vectors containing i.i.d. samples produced by generator.
   */
  @Experimental
  def randomVectorRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int): RDD[Vector] = {
    randomVectorRDD(sc, generator, numRows, numCols,
      sc.defaultParallelism, Utils.random.nextLong)
  }
}
