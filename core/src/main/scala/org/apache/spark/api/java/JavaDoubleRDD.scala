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

import java.lang.{Double => JDouble}

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter
import org.apache.spark.util.Utils

class JavaDoubleRDD(val srdd: RDD[scala.Double])
  extends AbstractJavaRDDLike[JDouble, JavaDoubleRDD] {

  override val classTag: ClassTag[JDouble] = implicitly[ClassTag[JDouble]]

  override val rdd: RDD[JDouble] = srdd.map(x => JDouble.valueOf(x))

  override def wrapRDD(rdd: RDD[JDouble]): JavaDoubleRDD =
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

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   * This method blocks until all blocks are deleted.
   */
  def unpersist(): JavaDoubleRDD = fromRDD(srdd.unpersist())

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   */
  def unpersist(blocking: Boolean): JavaDoubleRDD = fromRDD(srdd.unpersist(blocking))

  // first() has to be overriden here in order for its return type to be Double instead of Object.
  override def first(): JDouble = srdd.first()

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
  def filter(f: JFunction[JDouble, java.lang.Boolean]): JavaDoubleRDD =
    fromRDD(srdd.filter(x => f.call(x).booleanValue()))

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
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int): JavaDoubleRDD = fromRDD(srdd.repartition(numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
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
  def sample(withReplacement: Boolean, fraction: JDouble): JavaDoubleRDD =
    sample(withReplacement, fraction, Utils.random.nextLong)
    
  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: JDouble, seed: Long): JavaDoubleRDD =
    fromRDD(srdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaDoubleRDD): JavaDoubleRDD = fromRDD(srdd.union(other.srdd))

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   */
  def intersection(other: JavaDoubleRDD): JavaDoubleRDD = fromRDD(srdd.intersection(other.srdd))

  // Double RDD functions

  /** Add up the elements in this RDD. */
  def sum(): JDouble = srdd.sum()

  /**
   * Returns the minimum element from this RDD as defined by
   * the default comparator natural order.
   * @return the minimum of the RDD
   */
  def min(): JDouble = min(com.google.common.collect.Ordering.natural())

  /**
   * Returns the maximum element from this RDD as defined by
   * the default comparator natural order.
   * @return the maximum of the RDD
   */
  def max(): JDouble = max(com.google.common.collect.Ordering.natural())

  /**
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and
   * count of the RDD's elements in one operation.
   */
  def stats(): StatCounter = srdd.stats()

  /** Compute the mean of this RDD's elements. */
  def mean(): JDouble = srdd.mean()

  /** Compute the variance of this RDD's elements. */
  def variance(): JDouble = srdd.variance()

  /** Compute the standard deviation of this RDD's elements. */
  def stdev(): JDouble = srdd.stdev()

  /**
   * Compute the sample standard deviation of this RDD's elements (which corrects for bias in
   * estimating the standard deviation by dividing by N-1 instead of N).
   */
  def sampleStdev(): JDouble = srdd.sampleStdev()

  /**
   * Compute the sample variance of this RDD's elements (which corrects for bias in
   * estimating the standard variance by dividing by N-1 instead of N).
   */
  def sampleVariance(): JDouble = srdd.sampleVariance()

  /** Return the approximate mean of the elements in this RDD. */
  def meanApprox(timeout: Long, confidence: JDouble): PartialResult[BoundedDouble] =
    srdd.meanApprox(timeout, confidence)

  /**
   * :: Experimental ::
   * Approximate operation to return the mean within a timeout.
   */
  @Experimental
  def meanApprox(timeout: Long): PartialResult[BoundedDouble] = srdd.meanApprox(timeout)

  /**
   * :: Experimental ::
   * Approximate operation to return the sum within a timeout.
   */
  @Experimental
  def sumApprox(timeout: Long, confidence: JDouble): PartialResult[BoundedDouble] =
    srdd.sumApprox(timeout, confidence)

  /**
   * :: Experimental ::
   * Approximate operation to return the sum within a timeout.
   */
  @Experimental
  def sumApprox(timeout: Long): PartialResult[BoundedDouble] = srdd.sumApprox(timeout)

  /**
   * Compute a histogram of the data using bucketCount number of buckets evenly
   *  spaced between the minimum and maximum of the RDD. For example if the min
   *  value is 0 and the max is 100 and there are two buckets the resulting
   *  buckets will be [0,50) [50,100]. bucketCount must be at least 1
   * If the RDD contains infinity, NaN throws an exception
   * If the elements in RDD do not vary (max == min) always returns a single bucket.
   */
  def histogram(bucketCount: Int): Pair[Array[scala.Double], Array[Long]] = {
    val result = srdd.histogram(bucketCount)
    (result._1, result._2)
  }

  /**
   * Compute a histogram using the provided buckets. The buckets are all open
   * to the left except for the last which is closed
   *  e.g. for the array
   *  [1,10,20,50] the buckets are [1,10) [10,20) [20,50]
   *  e.g 1&lt;=x&lt;10 , 10&lt;=x&lt;20, 20&lt;=x&lt;50
   *  And on the input of 1 and 50 we would have a histogram of 1,0,0
   *
   * Note: if your histogram is evenly spaced (e.g. [0, 10, 20, 30]) this can be switched
   * from an O(log n) insertion to O(1) per element. (where n = # buckets) if you set evenBuckets
   * to true.
   * buckets must be sorted and not contain any duplicates.
   * buckets array must be at least two elements
   * All NaN entries are treated the same. If you have a NaN bucket it must be
   * the maximum value of the last position and all NaN entries will be counted
   * in that bucket.
   */
  def histogram(buckets: Array[scala.Double]): Array[Long] = {
    srdd.histogram(buckets, false)
  }

  def histogram(buckets: Array[JDouble], evenBuckets: Boolean): Array[Long] = {
    srdd.histogram(buckets.map(_.toDouble), evenBuckets)
  }

  /** Assign a name to this RDD */
  def setName(name: String): JavaDoubleRDD = {
    srdd.setName(name)
    this
  }
}

object JavaDoubleRDD {
  def fromRDD(rdd: RDD[scala.Double]): JavaDoubleRDD = new JavaDoubleRDD(rdd)

  implicit def toRDD(rdd: JavaDoubleRDD): RDD[scala.Double] = rdd.srdd
}
