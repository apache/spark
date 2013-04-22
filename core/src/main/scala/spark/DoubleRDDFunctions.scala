package spark

import spark.partial.BoundedDouble
import spark.partial.MeanEvaluator
import spark.partial.PartialResult
import spark.partial.SumEvaluator
import spark.util.StatCounter

/**
 * Extra functions available on RDDs of Doubles through an implicit conversion.
 * Import `spark.SparkContext._` at the top of your program to use these functions.
 */
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  /** Add up the elements in this RDD. */
  def sum(): Double = {
    self.reduce(_ + _)
  }

  /**
   * Return a [[spark.util.StatCounter]] object that captures the mean, variance and count
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
  def sampleStdev(): Double = stats().stdev

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
