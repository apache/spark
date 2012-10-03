package spark

import spark.partial.BoundedDouble
import spark.partial.MeanEvaluator
import spark.partial.PartialResult
import spark.partial.SumEvaluator

import spark.util.StatCounter

/**
 * Extra functions available on RDDs of Doubles through an implicit conversion.
 */
private[spark] class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  def sum(): Double = {
    self.reduce(_ + _)
  }

  def stats(): StatCounter = {
    self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b))
  }

  def mean(): Double = stats().mean

  def variance(): Double = stats().variance

  def stdev(): Double = stats().stdev

  def meanApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.splits.size, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  def sumApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.splits.size, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }
}
