package spark.partial

import cern.jet.stat.Probability

import spark.util.StatCounter

/**
 * An ApproximateEvaluator for sums. It estimates the mean and the cont and multiplies them
 * together, then uses the formula for the variance of two independent random variables to get
 * a variance for the result and compute a confidence interval.
 */
private[spark] class SumEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[StatCounter, BoundedDouble] {

  var outputsMerged = 0
  var counter = new StatCounter

  override def merge(outputId: Int, taskResult: StatCounter) {
    outputsMerged += 1
    counter.merge(taskResult)
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(counter.sum, 1.0, counter.sum, counter.sum)
    } else if (outputsMerged == 0) {
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val meanEstimate = counter.mean
      val meanVar = counter.sampleVariance / counter.count
      val countEstimate = (counter.count + 1 - p) / p
      val countVar = (counter.count + 1) * (1 - p) / (p * p)
      val sumEstimate = meanEstimate * countEstimate
      val sumVar = (meanEstimate * meanEstimate * countVar) +
                   (countEstimate * countEstimate * meanVar) +
                   (meanVar * countVar)
      val sumStdev = math.sqrt(sumVar)
      val confFactor = {
        if (counter.count > 100) {
          Probability.normalInverse(1 - (1 - confidence) / 2)
        } else {
          Probability.studentTInverse(1 - confidence, (counter.count - 1).toInt)
        }
      }
      val low = sumEstimate - confFactor * sumStdev
      val high = sumEstimate + confFactor * sumStdev
      new BoundedDouble(sumEstimate, confidence, low, high)
    }
  }
}
