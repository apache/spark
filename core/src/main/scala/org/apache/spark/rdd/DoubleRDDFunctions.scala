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
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and count
   * of the RDD's elements in one operation.
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
}
