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
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.rdd.{RandomRDD, RandomVectorRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Generator methods for creating RDDs comprised of `i.i.d.` samples from some distribution.
 */
@Since("1.1.0")
object RandomRDDs {

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the uniform distribution `U(0.0, 1.0)`.
   *
   * To transform the distribution in the generated RDD from `U(0.0, 1.0)` to `U(a, b)`, use
   * `RandomRDDs.uniformRDD(sc, n, p, seed).map(v => a + (b - a) * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ `U(0.0, 1.0)`.
   */
  @Since("1.1.0")
  def uniformRDD(
      sc: SparkContext,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val uniform = new UniformGenerator()
    randomRDD(sc, uniform, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#uniformRDD]].
   */
  @Since("1.1.0")
  def uniformJavaRDD(
      jsc: JavaSparkContext,
      size: Long,
      numPartitions: Int,
      seed: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(uniformRDD(jsc.sc, size, numPartitions, seed))
  }

  /**
   * [[RandomRDDs#uniformJavaRDD]] with the default seed.
   */
  @Since("1.1.0")
  def uniformJavaRDD(jsc: JavaSparkContext, size: Long, numPartitions: Int): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(uniformRDD(jsc.sc, size, numPartitions))
  }

  /**
   * [[RandomRDDs#uniformJavaRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.1.0")
  def uniformJavaRDD(jsc: JavaSparkContext, size: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(uniformRDD(jsc.sc, size))
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the standard normal distribution.
   *
   * To transform the distribution in the generated RDD from standard normal to some other normal
   * `N(mean, sigma^2^)`, use `RandomRDDs.normalRDD(sc, n, p, seed).map(v => mean + sigma * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ N(0.0, 1.0).
   */
  @Since("1.1.0")
  def normalRDD(
      sc: SparkContext,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val normal = new StandardNormalGenerator()
    randomRDD(sc, normal, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#normalRDD]].
   */
  @Since("1.1.0")
  def normalJavaRDD(
      jsc: JavaSparkContext,
      size: Long,
      numPartitions: Int,
      seed: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(normalRDD(jsc.sc, size, numPartitions, seed))
  }

  /**
   * [[RandomRDDs#normalJavaRDD]] with the default seed.
   */
  @Since("1.1.0")
  def normalJavaRDD(jsc: JavaSparkContext, size: Long, numPartitions: Int): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(normalRDD(jsc.sc, size, numPartitions))
  }

  /**
   * [[RandomRDDs#normalJavaRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.1.0")
  def normalJavaRDD(jsc: JavaSparkContext, size: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(normalRDD(jsc.sc, size))
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the Poisson distribution with the input
   * mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  @Since("1.1.0")
  def poissonRDD(
      sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val poisson = new PoissonGenerator(mean)
    randomRDD(sc, poisson, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#poissonRDD]].
   */
  @Since("1.1.0")
  def poissonJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(poissonRDD(jsc.sc, mean, size, numPartitions, seed))
  }

  /**
   * [[RandomRDDs#poissonJavaRDD]] with the default seed.
   */
  @Since("1.1.0")
  def poissonJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(poissonRDD(jsc.sc, mean, size, numPartitions))
  }

  /**
   * [[RandomRDDs#poissonJavaRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.1.0")
  def poissonJavaRDD(jsc: JavaSparkContext, mean: Double, size: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(poissonRDD(jsc.sc, mean, size))
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the exponential distribution with
   * the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or 1 / lambda, for the exponential distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  @Since("1.3.0")
  def exponentialRDD(
      sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val exponential = new ExponentialGenerator(mean)
    randomRDD(sc, exponential, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#exponentialRDD]].
   */
  @Since("1.3.0")
  def exponentialJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(exponentialRDD(jsc.sc, mean, size, numPartitions, seed))
  }

  /**
   * [[RandomRDDs#exponentialJavaRDD]] with the default seed.
   */
  @Since("1.3.0")
  def exponentialJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(exponentialRDD(jsc.sc, mean, size, numPartitions))
  }

  /**
   * [[RandomRDDs#exponentialJavaRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.3.0")
  def exponentialJavaRDD(jsc: JavaSparkContext, mean: Double, size: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(exponentialRDD(jsc.sc, mean, size))
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the gamma distribution with the input
   *  shape and scale.
   *
   * @param sc SparkContext used to create the RDD.
   * @param shape shape parameter (> 0) for the gamma distribution
   * @param scale scale parameter (> 0) for the gamma distribution
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  @Since("1.3.0")
  def gammaRDD(
      sc: SparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val gamma = new GammaGenerator(shape, scale)
    randomRDD(sc, gamma, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#gammaRDD]].
   */
  @Since("1.3.0")
  def gammaJavaRDD(
      jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(gammaRDD(jsc.sc, shape, scale, size, numPartitions, seed))
  }

  /**
   * [[RandomRDDs#gammaJavaRDD]] with the default seed.
   */
  @Since("1.3.0")
  def gammaJavaRDD(
      jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(gammaRDD(jsc.sc, shape, scale, size, numPartitions))
  }

  /**
   * [[RandomRDDs#gammaJavaRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.3.0")
  def gammaJavaRDD(
      jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(gammaRDD(jsc.sc, shape, scale, size))
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the log normal distribution with the input
   *  mean and standard deviation
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean mean for the log normal distribution
   * @param std standard deviation for the log normal distribution
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  @Since("1.3.0")
  def logNormalRDD(
      sc: SparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val logNormal = new LogNormalGenerator(mean, std)
    randomRDD(sc, logNormal, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#logNormalRDD]].
   */
  @Since("1.3.0")
  def logNormalJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(logNormalRDD(jsc.sc, mean, std, size, numPartitions, seed))
  }

  /**
   * [[RandomRDDs#logNormalJavaRDD]] with the default seed.
   */
  @Since("1.3.0")
  def logNormalJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(logNormalRDD(jsc.sc, mean, std, size, numPartitions))
  }

  /**
   * [[RandomRDDs#logNormalJavaRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.3.0")
  def logNormalJavaRDD(
      jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(logNormalRDD(jsc.sc, mean, std, size))
  }


  /**
   * :: DeveloperApi ::
   * Generates an RDD comprised of `i.i.d.` samples produced by the input RandomDataGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator RandomDataGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[T] comprised of `i.i.d.` samples produced by generator.
   */
  @DeveloperApi
  @Since("1.1.0")
  def randomRDD[T: ClassTag](
      sc: SparkContext,
      generator: RandomDataGenerator[T],
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[T] = {
    new RandomRDD[T](sc, size, numPartitionsOrDefault(sc, numPartitions), generator, seed)
  }

  /**
   * :: DeveloperApi ::
   * Generates an RDD comprised of `i.i.d.` samples produced by the input RandomDataGenerator.
   *
   * @param jsc JavaSparkContext used to create the RDD.
   * @param generator RandomDataGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[T] comprised of `i.i.d.` samples produced by generator.
   */
  @DeveloperApi
  @Since("1.6.0")
  def randomJavaRDD[T](
      jsc: JavaSparkContext,
      generator: RandomDataGenerator[T],
      size: Long,
      numPartitions: Int,
      seed: Long): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    val rdd = randomRDD(jsc.sc, generator, size, numPartitions, seed)
    JavaRDD.fromRDD(rdd)
  }

  /**
   * :: DeveloperApi ::
   * [[RandomRDDs#randomJavaRDD]] with the default seed.
   */
  @DeveloperApi
  @Since("1.6.0")
  def randomJavaRDD[T](
    jsc: JavaSparkContext,
    generator: RandomDataGenerator[T],
    size: Long,
    numPartitions: Int): JavaRDD[T] = {
    randomJavaRDD(jsc, generator, size, numPartitions, Utils.random.nextLong())
  }

  /**
   * :: DeveloperApi ::
   * [[RandomRDDs#randomJavaRDD]] with the default seed & numPartitions
   */
  @DeveloperApi
  @Since("1.6.0")
  def randomJavaRDD[T](
      jsc: JavaSparkContext,
      generator: RandomDataGenerator[T],
      size: Long): JavaRDD[T] = {
    randomJavaRDD(jsc, generator, size, 0)
  }

  // TODO Generate RDD[Vector] from multivariate distributions.

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * uniform distribution on `U(0.0, 1.0)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD.
   * @param seed Seed for the RNG that generates the seed for the generator in each partition.
   * @return RDD[Vector] with vectors containing i.i.d samples ~ `U(0.0, 1.0)`.
   */
  @Since("1.1.0")
  def uniformVectorRDD(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val uniform = new UniformGenerator()
    randomVectorRDD(sc, uniform, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#uniformVectorRDD]].
   */
  @Since("1.1.0")
  def uniformJavaVectorRDD(
      jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    uniformVectorRDD(jsc.sc, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * [[RandomRDDs#uniformJavaVectorRDD]] with the default seed.
   */
  @Since("1.1.0")
  def uniformJavaVectorRDD(
      jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    uniformVectorRDD(jsc.sc, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * [[RandomRDDs#uniformJavaVectorRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.1.0")
  def uniformJavaVectorRDD(
      jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    uniformVectorRDD(jsc.sc, numRows, numCols).toJavaRDD()
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * standard normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ `N(0.0, 1.0)`.
   */
  @Since("1.1.0")
  def normalVectorRDD(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val normal = new StandardNormalGenerator()
    randomVectorRDD(sc, normal, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#normalVectorRDD]].
   */
  @Since("1.1.0")
  def normalJavaVectorRDD(
      jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    normalVectorRDD(jsc.sc, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * [[RandomRDDs#normalJavaVectorRDD]] with the default seed.
   */
  @Since("1.1.0")
  def normalJavaVectorRDD(
      jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    normalVectorRDD(jsc.sc, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * [[RandomRDDs#normalJavaVectorRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.1.0")
  def normalJavaVectorRDD(
      jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    normalVectorRDD(jsc.sc, numRows, numCols).toJavaRDD()
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from a
   * log normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean of the log normal distribution.
   * @param std Standard deviation of the log normal distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples.
   */
  @Since("1.3.0")
  def logNormalVectorRDD(
      sc: SparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val logNormal = new LogNormalGenerator(mean, std)
    randomVectorRDD(sc, logNormal, numRows, numCols,
      numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#logNormalVectorRDD]].
   */
  @Since("1.3.0")
  def logNormalJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    logNormalVectorRDD(jsc.sc, mean, std, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * [[RandomRDDs#logNormalJavaVectorRDD]] with the default seed.
   */
  @Since("1.3.0")
  def logNormalJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    logNormalVectorRDD(jsc.sc, mean, std, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * [[RandomRDDs#logNormalJavaVectorRDD]] with the default number of partitions and
   * the default seed.
   */
  @Since("1.3.0")
  def logNormalJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    logNormalVectorRDD(jsc.sc, mean, std, numRows, numCols).toJavaRDD()
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Pois(mean).
   */
  @Since("1.1.0")
  def poissonVectorRDD(
      sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val poisson = new PoissonGenerator(mean)
    randomVectorRDD(sc, poisson, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#poissonVectorRDD]].
   */
  @Since("1.1.0")
  def poissonJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    poissonVectorRDD(jsc.sc, mean, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * [[RandomRDDs#poissonJavaVectorRDD]] with the default seed.
   */
  @Since("1.1.0")
  def poissonJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    poissonVectorRDD(jsc.sc, mean, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * [[RandomRDDs#poissonJavaVectorRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.1.0")
  def poissonJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    poissonVectorRDD(jsc.sc, mean, numRows, numCols).toJavaRDD()
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * exponential distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or 1 / lambda, for the Exponential distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Exp(mean).
   */
  @Since("1.3.0")
  def exponentialVectorRDD(
      sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val exponential = new ExponentialGenerator(mean)
    randomVectorRDD(sc, exponential, numRows, numCols,
      numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#exponentialVectorRDD]].
   */
  @Since("1.3.0")
  def exponentialJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    exponentialVectorRDD(jsc.sc, mean, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * [[RandomRDDs#exponentialJavaVectorRDD]] with the default seed.
   */
  @Since("1.3.0")
  def exponentialJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    exponentialVectorRDD(jsc.sc, mean, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * [[RandomRDDs#exponentialJavaVectorRDD]] with the default number of partitions
   * and the default seed.
   */
  @Since("1.3.0")
  def exponentialJavaVectorRDD(
      jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    exponentialVectorRDD(jsc.sc, mean, numRows, numCols).toJavaRDD()
  }


  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * gamma distribution with the input shape and scale.
   *
   * @param sc SparkContext used to create the RDD.
   * @param shape shape parameter (> 0) for the gamma distribution.
   * @param scale scale parameter (> 0) for the gamma distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Exp(mean).
   */
  @Since("1.3.0")
  def gammaVectorRDD(
      sc: SparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val gamma = new GammaGenerator(shape, scale)
    randomVectorRDD(sc, gamma, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Java-friendly version of [[RandomRDDs#gammaVectorRDD]].
   */
  @Since("1.3.0")
  def gammaJavaVectorRDD(
      jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    gammaVectorRDD(jsc.sc, shape, scale, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * [[RandomRDDs#gammaJavaVectorRDD]] with the default seed.
   */
  @Since("1.3.0")
  def gammaJavaVectorRDD(
      jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    gammaVectorRDD(jsc.sc, shape, scale, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * [[RandomRDDs#gammaJavaVectorRDD]] with the default number of partitions and the default seed.
   */
  @Since("1.3.0")
  def gammaJavaVectorRDD(
      jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    gammaVectorRDD(jsc.sc, shape, scale, numRows, numCols).toJavaRDD()
  }


  /**
   * :: DeveloperApi ::
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples produced by the
   * input RandomDataGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator RandomDataGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples produced by generator.
   */
  @DeveloperApi
  @Since("1.1.0")
  def randomVectorRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    new RandomVectorRDD(
      sc, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), generator, seed)
  }

  /**
   * :: DeveloperApi ::
   * Java-friendly version of [[RandomRDDs#randomVectorRDD]].
   */
  @DeveloperApi
  @Since("1.6.0")
  def randomJavaVectorRDD(
      jsc: JavaSparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long): JavaRDD[Vector] = {
    randomVectorRDD(jsc.sc, generator, numRows, numCols, numPartitions, seed).toJavaRDD()
  }

  /**
   * :: DeveloperApi ::
   * [[RandomRDDs#randomJavaVectorRDD]] with the default seed.
   */
  @DeveloperApi
  @Since("1.6.0")
  def randomJavaVectorRDD(
      jsc: JavaSparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int): JavaRDD[Vector] = {
    randomVectorRDD(jsc.sc, generator, numRows, numCols, numPartitions).toJavaRDD()
  }

  /**
   * :: DeveloperApi ::
   * [[RandomRDDs#randomJavaVectorRDD]] with the default number of partitions and the default seed.
   */
  @DeveloperApi
  @Since("1.6.0")
  def randomJavaVectorRDD(
      jsc: JavaSparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int): JavaRDD[Vector] = {
    randomVectorRDD(jsc.sc, generator, numRows, numCols).toJavaRDD()
  }

  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions else sc.defaultMinPartitions
  }
}
