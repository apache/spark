package org.apache.spark.mllib.regression

import org.apache.spark.Logging

trait StreamingDecay {
  def getDiscount(numNewDataPoints: Long): Double
}

trait StreamingDecaySetter[T <: StreamingDecaySetter[T]] extends Logging {
  self: T =>
  var decayFactor: Double = 1
  var timeUnit: String = StreamingDecay.BATCHES

  /** Set the decay factor directly (for forgetful algorithms). */
  def setDecayFactor(a: Double): T = {
    this.decayFactor = a
    this
  }

  /** Set the half life and time unit ("batches" or "points") for forgetful algorithms. */
  def setHalfLife(halfLife: Double, timeUnit: String): T = {
    if (timeUnit != StreamingDecay.BATCHES && timeUnit != StreamingDecay.POINTS) {
      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
    }
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
    this.timeUnit = timeUnit
    this
  }
  
  def getDiscount(numNewDataPoints: Long): Double = timeUnit match {
    case StreamingDecay.BATCHES => decayFactor
    case StreamingDecay.POINTS => math.pow(decayFactor, numNewDataPoints)
  }
}

object StreamingDecay {
  final val BATCHES = "batches"
  final val POINTS = "points"
}