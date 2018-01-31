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

import org.apache.spark.TaskContext
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.MeanEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.partial.SumEvaluator
import org.apache.spark.util.StatCounter

/**
 * Extra functions available on RDDs of Doubles through an implicit conversion.
 */
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  /** Add up the elements in this RDD. */
  def sum(): Double = self.withScope {
    self.fold(0.0)(_ + _)
  }

  /**
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and
   * count of the RDD's elements in one operation.
   */
  def stats(): StatCounter = self.withScope {
    self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b))
  }

  /** Compute the mean of this RDD's elements. */
  def mean(): Double = self.withScope {
    stats().mean
  }

  /** Compute the population variance of this RDD's elements. */
  def variance(): Double = self.withScope {
    stats().variance
  }

  /** Compute the population standard deviation of this RDD's elements. */
  def stdev(): Double = self.withScope {
    stats().stdev
  }

  /**
   * Compute the sample standard deviation of this RDD's elements (which corrects for bias in
   * estimating the standard deviation by dividing by N-1 instead of N).
   */
  def sampleStdev(): Double = self.withScope {
    stats().sampleStdev
  }

  /**
   * Compute the sample variance of this RDD's elements (which corrects for bias in
   * estimating the variance by dividing by N-1 instead of N).
   */
  def sampleVariance(): Double = self.withScope {
    stats().sampleVariance
  }

  /**
   * Compute the population standard deviation of this RDD's elements.
   */
  @Since("2.1.0")
  def popStdev(): Double = self.withScope {
    stats().popStdev
  }

  /**
   * Compute the population variance of this RDD's elements.
   */
  @Since("2.1.0")
  def popVariance(): Double = self.withScope {
    stats().popVariance
  }

  /**
   * Approximate operation to return the mean within a timeout.
   */
  def meanApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * Approximate operation to return the sum within a timeout.
   */
  def sumApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.partitions.length, confidence)
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
  def histogram(bucketCount: Int): (Array[Double], Array[Long]) = self.withScope {
    // Scala's built-in range has issues. See #SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    }
    // Compute the minimum and the maximum
    val (max: Double, min: Double) = self.mapPartitions { items =>
      Iterator(
        items.foldRight((Double.NegativeInfinity, Double.PositiveInfinity)
        )((e: Double, x: (Double, Double)) => (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }
    val range = if (min != max) {
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, bucketCount)
    } else {
      List(min, min)
    }
    val buckets = range.toArray
    (buckets, histogram(buckets, true))
  }

  /**
   * Compute a histogram using the provided buckets. The buckets are all open
   * to the right except for the last which is closed.
   *  e.g. for the array
   *  [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
   *  e.g {@code <=x<10, 10<=x<20, 20<=x<=50}
   *  And on the input of 1 and 50 we would have a histogram of 1, 0, 1
   *
   * @note If your histogram is evenly spaced (e.g. [0, 10, 20, 30]) this can be switched
   * from an O(log n) insertion to O(1) per element. (where n = # buckets) if you set evenBuckets
   * to true.
   * buckets must be sorted and not contain any duplicates.
   * buckets array must be at least two elements
   * All NaN entries are treated the same. If you have a NaN bucket it must be
   * the maximum value of the last position and all NaN entries will be counted
   * in that bucket.
   */
  def histogram(
      buckets: Array[Double],
      evenBuckets: Boolean = false): Array[Long] = self.withScope {
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
          case Some(x: Int) => counters(x) += 1
          case _ => // No-Op
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
    def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
      // If our input is not a number unless the increment is also NaN then we fail fast
      if (e.isNaN || e < min || e > max) {
        None
      } else {
        // Compute ratio of e's distance along range to total range first, for better precision
        val bucketNumber = (((e - min) / (max - min)) * count).toInt
        // should be less than count, but will equal count if e == max, in which case
        // it's part of the last end-range-inclusive bucket, so return count-1
        Some(math.min(bucketNumber, count - 1))
      }
    }
    // Decide which bucket function to pass to histogramPartition. We decide here
    // rather than having a general function so that the decision need only be made
    // once rather than once per shard
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
    } else {
      basicBucketFunction _
    }
    if (self.partitions.length == 0) {
      new Array[Long](buckets.length - 1)
    } else {
      // reduce() requires a non-empty RDD. This works because the mapPartitions will make
      // non-empty partitions out of empty ones. But it doesn't handle the no-partitions case,
      // which is below
      self.mapPartitions(histogramPartition(bucketFunction)).reduce(mergeCounters)
    }
  }

}
