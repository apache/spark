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

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.SparkContext._

class DoubleRDDSuite extends FunSuite with SharedSparkContext {
  // Verify tests on the histogram functionality. We test with both evenly
  // and non-evenly spaced buckets as the bucket lookup function changes.
  test("WorksOnEmpty") {
    // Make sure that it works on an empty input
    val rdd: RDD[Double] = sc.parallelize(Seq())
    val buckets = Array(0.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksWithOutOfRangeWithOneBucket") {
    // Verify that if all of the elements are out of range the counts are zero
    val rdd = sc.parallelize(Seq(10.01, -0.01))
    val buckets = Array(0.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksInRangeWithOneBucket") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    val buckets = Array(0.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(4)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksInRangeWithOneBucketExactMatch") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    val buckets = Array(1.0, 4.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(4)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksWithOutOfRangeWithTwoBuckets") {
    // Verify that out of range works with two buckets
    val rdd = sc.parallelize(Seq(10.01, -0.01))
    val buckets = Array(0.0, 5.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(0, 0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksWithOutOfRangeWithTwoUnEvenBuckets") {
    // Verify that out of range works with two un even buckets
    val rdd = sc.parallelize(Seq(10.01, -0.01))
    val buckets = Array(0.0, 4.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(0, 0)
    assert(histogramResults === expectedHistogramResults)
  }

  test("WorksInRangeWithTwoBuckets") {
    // Make sure that it works with two equally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(1, 2, 3, 5, 6))
    val buckets = Array(0.0, 5.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(3, 2)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksInRangeWithTwoBucketsAndNaN") {
    // Make sure that it works with two equally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(1, 2, 3, 5, 6, Double.NaN))
    val buckets = Array(0.0, 5.0, 10.0)
    val histogramResults = rdd.histogram(buckets)
    val histogramResults2 = rdd.histogram(buckets, true)
    val expectedHistogramResults = Array(3, 2)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramResults2 === expectedHistogramResults)
  }

  test("WorksInRangeWithTwoUnevenBuckets") {
    // Make sure that it works with two unequally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(1, 2, 3, 5, 6))
    val buckets = Array(0.0, 5.0, 11.0)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(3, 2)
    assert(histogramResults === expectedHistogramResults)
  }

  test("WorksMixedRangeWithTwoUnevenBuckets") {
    // Make sure that it works with two unequally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(-0.01, 0.0, 1, 2, 3, 5, 6, 11.0, 11.01))
    val buckets = Array(0.0, 5.0, 11.0)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(4, 3)
    assert(histogramResults === expectedHistogramResults)
  }

  test("WorksMixedRangeWithFourUnevenBuckets") {
    // Make sure that it works with two unequally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0,
      200.0, 200.1))
    val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(4, 2, 1, 3)
    assert(histogramResults === expectedHistogramResults)
  }

  test("WorksMixedRangeWithUnevenBucketsAndNaN") {
    // Make sure that it works with two unequally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0,
      200.0, 200.1, Double.NaN))
    val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(4, 2, 1, 3)
    assert(histogramResults === expectedHistogramResults)
  }
  // Make sure this works with a NaN end bucket
  test("WorksMixedRangeWithUnevenBucketsAndNaNAndNaNRange") {
    // Make sure that it works with two unequally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0,
      200.0, 200.1, Double.NaN))
    val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0, Double.NaN)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(4, 2, 1, 2, 3)
    assert(histogramResults === expectedHistogramResults)
  }
  // Make sure this works with a NaN end bucket and an inifity
  test("WorksMixedRangeWithUnevenBucketsAndNaNAndNaNRangeAndInfity") {
    // Make sure that it works with two unequally spaced buckets and elements in each
    val rdd = sc.parallelize(Seq(-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0,
      200.0, 200.1, 1.0/0.0, -1.0/0.0, Double.NaN))
    val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0, Double.NaN)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(4, 2, 1, 2, 4)
    assert(histogramResults === expectedHistogramResults)
  }

  test("WorksWithOutOfRangeWithInfiniteBuckets") {
    // Verify that out of range works with two buckets
    val rdd = sc.parallelize(Seq(10.01, -0.01, Double.NaN))
    val buckets = Array(-1.0/0.0 , 0.0, 1.0/0.0)
    val histogramResults = rdd.histogram(buckets)
    val expectedHistogramResults = Array(1, 1)
    assert(histogramResults === expectedHistogramResults)
  }
  // Test the failure mode with an invalid bucket array
  test("ThrowsExceptionOnInvalidBucketArray") {
    val rdd = sc.parallelize(Seq(1.0))
    // Empty array
    intercept[IllegalArgumentException] {
      val buckets = Array.empty[Double]
      val result = rdd.histogram(buckets)
    }
    // Single element array
    intercept[IllegalArgumentException] {
      val buckets = Array(1.0)
      val result = rdd.histogram(buckets)
    }
  }

  // Test automatic histogram function
  test("WorksWithoutBucketsBasic") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    val (histogramBuckets, histogramResults) = rdd.histogram(1)
    val expectedHistogramResults = Array(4)
    val expectedHistogramBuckets = Array(1.0, 4.0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets === expectedHistogramBuckets)
  }
  // Test automatic histogram function with a single element
  test("WorksWithoutBucketsBasicSingleElement") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1))
    val (histogramBuckets, histogramResults) = rdd.histogram(1)
    val expectedHistogramResults = Array(1)
    val expectedHistogramBuckets = Array(1.0, 1.0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets === expectedHistogramBuckets)
  }
  // Test automatic histogram function with a single element
  test("WorksWithoutBucketsBasicNoRange") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1, 1, 1, 1))
    val (histogramBuckets, histogramResults) = rdd.histogram(1)
    val expectedHistogramResults = Array(4)
    val expectedHistogramBuckets = Array(1.0, 1.0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets === expectedHistogramBuckets)
  }

  test("WorksWithoutBucketsBasicTwo") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    val (histogramBuckets, histogramResults) = rdd.histogram(2)
    val expectedHistogramResults = Array(2, 2)
    val expectedHistogramBuckets = Array(1.0, 2.5, 4.0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets === expectedHistogramBuckets)
  }

  test("WorksWithoutBucketsWithMoreRequestedThanElements") {
    // Verify the basic case of one bucket and all elements in that bucket works
    val rdd = sc.parallelize(Seq(1, 2))
    val (histogramBuckets, histogramResults) = rdd.histogram(10)
    val expectedHistogramResults =
      Array(1, 0, 0, 0, 0, 0, 0, 0, 0, 1)
    val expectedHistogramBuckets =
      Array(1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets === expectedHistogramBuckets)
  }

  test("WorksWithoutBucketsForLargerDatasets") {
    // Verify the case of slighly larger datasets
    val rdd = sc.parallelize(6 to 99)
    val (histogramBuckets, histogramResults) = rdd.histogram(8)
    val expectedHistogramResults =
      Array(12, 12, 11, 12, 12, 11, 12, 12)
    val expectedHistogramBuckets =
      Array(6.0, 17.625, 29.25, 40.875, 52.5, 64.125, 75.75, 87.375, 99.0)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets === expectedHistogramBuckets)
  }

  test("WorksWithoutBucketsWithIrrationalBucketEdges") {
    // Verify the case of buckets with irrational edges. See #SPARK-2862.
    val rdd = sc.parallelize(6 to 99)
    val (histogramBuckets, histogramResults) = rdd.histogram(9)
    val expectedHistogramResults =
      Array(11, 10, 11, 10, 10, 11, 10, 10, 11)
    assert(histogramResults === expectedHistogramResults)
    assert(histogramBuckets(0) === 6.0)
    assert(histogramBuckets(9) === 99.0)
  }

  // Test the failure mode with an invalid RDD
  test("ThrowsExceptionOnInvalidRDDs") {
    // infinity
    intercept[UnsupportedOperationException] {
      val rdd = sc.parallelize(Seq(1, 1.0/0.0))
      val result = rdd.histogram(1)
    }
    // NaN
    intercept[UnsupportedOperationException] {
      val rdd = sc.parallelize(Seq(1, Double.NaN))
      val result = rdd.histogram(1)
    }
    // Empty
    intercept[UnsupportedOperationException] {
      val rdd: RDD[Double] = sc.parallelize(Seq())
      val result = rdd.histogram(1)
    }
  }

}
