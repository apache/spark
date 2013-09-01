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

package org.apache.spark.api.java

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.util.StatCounter
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.storage.StorageLevel
import java.lang.Double
import org.apache.spark.Partitioner

class JavaDoubleRDD(val srdd: RDD[scala.Double]) extends JavaRDDLike[Double, JavaDoubleRDD] {

  override val classManifest: ClassManifest[Double] = implicitly[ClassManifest[Double]]

  override val rdd: RDD[Double] = srdd.map(x => Double.valueOf(x))

  override def wrapRDD(rdd: RDD[Double]): JavaDoubleRDD =
    new JavaDoubleRDD(rdd.map(_.doubleValue))

  // Common RDD functions

  import JavaDoubleRDD.fromRDD

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaDoubleRDD = fromRDD(srdd.cache())

  /** 
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): JavaDoubleRDD = fromRDD(srdd.persist(newLevel))

  // first() has to be overriden here in order for its return type to be Double instead of Object.
  override def first(): Double = srdd.first()

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaDoubleRDD = fromRDD(srdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int): JavaDoubleRDD = fromRDD(srdd.distinct(numPartitions))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[Double, java.lang.Boolean]): JavaDoubleRDD =
    fromRDD(srdd.filter(x => f(x).booleanValue()))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int): JavaDoubleRDD = fromRDD(srdd.coalesce(numPartitions))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean): JavaDoubleRDD =
    fromRDD(srdd.coalesce(numPartitions, shuffle))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   * 
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtract(other: JavaDoubleRDD): JavaDoubleRDD =
    fromRDD(srdd.subtract(other))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaDoubleRDD, numPartitions: Int): JavaDoubleRDD =
    fromRDD(srdd.subtract(other, numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaDoubleRDD, p: Partitioner): JavaDoubleRDD =
    fromRDD(srdd.subtract(other, p))

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaDoubleRDD =
    fromRDD(srdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaDoubleRDD): JavaDoubleRDD = fromRDD(srdd.union(other.srdd))

  // Double RDD functions

  /** Add up the elements in this RDD. */
  def sum(): Double = srdd.sum()

  /**
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and count
   * of the RDD's elements in one operation.
   */
  def stats(): StatCounter = srdd.stats()

  /** Compute the mean of this RDD's elements. */
  def mean(): Double = srdd.mean()

  /** Compute the variance of this RDD's elements. */
  def variance(): Double = srdd.variance()

  /** Compute the standard deviation of this RDD's elements. */
  def stdev(): Double = srdd.stdev()

  /**
   * Compute the sample standard deviation of this RDD's elements (which corrects for bias in
   * estimating the standard deviation by dividing by N-1 instead of N).
   */
  def sampleStdev(): Double = srdd.sampleStdev()

  /**
   * Compute the sample variance of this RDD's elements (which corrects for bias in
   * estimating the standard variance by dividing by N-1 instead of N).
   */
  def sampleVariance(): Double = srdd.sampleVariance()

  /** Return the approximate mean of the elements in this RDD. */
  def meanApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    srdd.meanApprox(timeout, confidence)

  /** (Experimental) Approximate operation to return the mean within a timeout. */
  def meanApprox(timeout: Long): PartialResult[BoundedDouble] = srdd.meanApprox(timeout)

  /** (Experimental) Approximate operation to return the sum within a timeout. */
  def sumApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    srdd.sumApprox(timeout, confidence)

  /** (Experimental) Approximate operation to return the sum within a timeout. */
  def sumApprox(timeout: Long): PartialResult[BoundedDouble] = srdd.sumApprox(timeout)
}

object JavaDoubleRDD {
  def fromRDD(rdd: RDD[scala.Double]): JavaDoubleRDD = new JavaDoubleRDD(rdd)

  implicit def toRDD(rdd: JavaDoubleRDD): RDD[scala.Double] = rdd.srdd
}
