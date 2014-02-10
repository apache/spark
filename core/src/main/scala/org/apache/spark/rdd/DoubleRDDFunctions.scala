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

package org.apache.spark.rdd

import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.MeanEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.partial.SumEvaluator
import org.apache.spark.util.StatCounter
import org.apache.spark.{TaskContext, Logging}

import scala.collection.immutable.NumericRange

/**
 * Extra functions available on RDDs of Doubles through an implicit conversion.
 * Import `org.apache.spark.SparkContext._` at the top of your program to use these functions.
 */
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  /** Add up the elements in this RDD. */
  def sum(): Double = {
    self.reduce(_ + _)
  }

  /**
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and
   * count of the RDD's elements in one operation.
   */
  def stats(): StatCounter = {
    self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b))
  }

  /** Compute the mean of this RDD's elements. */
  def mean(): Double = stats().mean

  /** Compute the variance of this RDD's elements. */
  def variance(): Double = stats().variance

  /** Compute the standard deviation of this RDD's elements. */
  def stdev(): Double = stats().stdev

  /** 
   * Compute the sample standard deviation of this RDD's elements (which corrects for bias in
   * estimating the standard deviation by dividing by N-1 instead of N).
   */
  def sampleStdev(): Double = stats().sampleStdev

  /**
   * Compute the sample variance of this RDD's elements (which corrects for bias in
   * estimating the variance by dividing by N-1 instead of N).
   */
  def sampleVariance(): Double = stats().sampleVariance

  /** (Experimental) Approximate operation to return the mean within a timeout. */
  def meanApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.partitions.size, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /** (Experimental) Approximate operation to return the sum within a timeout. */
  def sumApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.partitions.size, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * Compute a histogram of the data using bucketCount number of buckets evenly
   *  spaced between the minimum and maximum of the RDD. For example if the min
   *  value is 0 and the max is 100 and there are two buckets the resulting
   *  buckets will be [0, 50) [50, 100]. bucketCount must be at least 1
   * If the RDD contains infinity, NaN throws an exception
   * If the elements in RDD do not vary (max == min) always returns a single bucket.
   */
  def histogram(bucketCount: Int): Pair[Array[Double], Array[Long]] = {
    // Compute the minimum and the maxium
    val (max: Double, min: Double) = self.mapPartitions { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
        (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }
    val increment = (max-min)/bucketCount.toDouble
    val range = if (increment != 0) {
      Range.Double.inclusive(min, max, increment)
    } else {
      List(min, min)
    }
    val buckets = range.toArray
    (buckets, histogram(buckets, true))
  }

  /**
   * Compute a histogram using the provided buckets. The buckets are all open
   * to the left except for the last which is closed
   *  e.g. for the array
   *  [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
   *  e.g 1<=x<10 , 10<=x<20, 20<=x<50
   *  And on the input of 1 and 50 we would have a histogram of 1, 0, 0 
   * 
   * Note: if your histogram is evenly spaced (e.g. [0, 10, 20, 30]) this can be switched
   * from an O(log n) inseration to O(1) per element. (where n = # buckets) if you set evenBuckets
   * to true.
   * buckets must be sorted and not contain any duplicates.
   * buckets array must be at least two elements 
   * All NaN entries are treated the same. If you have a NaN bucket it must be
   * the maximum value of the last position and all NaN entries will be counted
   * in that bucket.
   */
  def histogram(buckets: Array[Double], evenBuckets: Boolean = false): Array[Long] = {
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    // The histogramPartition function computes the partail histogram for a given
    // partition. The provided bucketFunction determines which bucket in the array
    // to increment or returns None if there is no bucket. This is done so we can
    // specialize for uniformly distributed buckets and save the O(log n) binary
    // search cost.
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
        Iterator[Array[Long]] = {
      val counters = new Array[Long](buckets.length - 1)
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => {counters(x) += 1}
          case _ => {}
        }
      }
      Iterator(counters)
    }
    // Merge the counters.
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] = {
      a1.indices.foreach(i => a1(i) += a2(i))
      a1
    }
    // Basic bucket function. This works using Java's built in Array
    // binary search. Takes log(size(buckets))
    def basicBucketFunction(e: Double): Option[Int] = {
      val location = java.util.Arrays.binarySearch(buckets, e)
      if (location < 0) {
        // If the location is less than 0 then the insertion point in the array
        // to keep it sorted is -location-1
        val insertionPoint = -location-1
        // If we have to insert before the first element or after the last one
        // its out of bounds.
        // We do this rather than buckets.lengthCompare(insertionPoint)
        // because Array[Double] fails to override it (for now).
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint-1)
        } else {
          None
        }
      } else if (location < buckets.length - 1) {
        // Exact match, just insert here
        Some(location)
      } else {
        // Exact match to the last element
        Some(location - 1)
      }
    }
    // Determine the bucket function in constant time. Requires that buckets are evenly spaced
    def fastBucketFunction(min: Double, increment: Double, count: Int)(e: Double): Option[Int] = {
      // If our input is not a number unless the increment is also NaN then we fail fast
      if (e.isNaN()) {
        return None
      }
      val bucketNumber = (e - min)/(increment)
      // We do this rather than buckets.lengthCompare(bucketNumber)
      // because Array[Double] fails to override it (for now).
      if (bucketNumber > count || bucketNumber < 0) {
        None
      } else {
        Some(bucketNumber.toInt.min(count - 1))
      }
    }
    // Decide which bucket function to pass to histogramPartition. We decide here
    // rather than having a general function so that the decission need only be made
    // once rather than once per shard
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets(0), buckets(1)-buckets(0), buckets.length-1) _
    } else {
      basicBucketFunction _
    }
    self.mapPartitions(histogramPartition(bucketFunction)).reduce(mergeCounters)
  }

}
