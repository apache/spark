package spark.partial

import cern.jet.stat.Probability

import spark.util.StatCounter

/**
 * An ApproximateEvaluator for means.
 */
private[spark] class MeanEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[StatCounter, BoundedDouble] {

  var outputsMerged = 0
  var counter = new StatCounter

  override def merge(outputId: Int, taskResult: StatCounter) {
    outputsMerged += 1
    counter.merge(taskResult)
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(counter.mean, 1.0, counter.mean, counter.mean)
    } else if (outputsMerged == 0) {
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val mean = counter.mean
      val stdev = math.sqrt(counter.sampleVariance / counter.count)
      val confFactor = {
        if (counter.count > 100) {
          Probability.normalInverse(1 - (1 - confidence) / 2)
        } else {
          Probability.studentTInverse(1 - confidence, (counter.count - 1).toInt)
        }
      }
      val low = mean - confFactor * stdev
      val high = mean + confFactor * stdev
      new BoundedDouble(mean, confidence, low, high)
    }
  }
}
