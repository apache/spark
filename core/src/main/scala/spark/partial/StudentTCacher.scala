package spark.partial

import cern.jet.stat.Probability

/**
 * A utility class for caching Student's T distribution values for a given confidence level
 * and various sample sizes. This is used by the MeanEvaluator to efficiently calculate
 * confidence intervals for many keys.
 */
private[spark] class StudentTCacher(confidence: Double) {
  val NORMAL_APPROX_SAMPLE_SIZE = 100  // For samples bigger than this, use Gaussian approximation
  val normalApprox = Probability.normalInverse(1 - (1 - confidence) / 2)
  val cache = Array.fill[Double](NORMAL_APPROX_SAMPLE_SIZE)(-1.0)

  def get(sampleSize: Long): Double = {
    if (sampleSize >= NORMAL_APPROX_SAMPLE_SIZE) {
      normalApprox
    } else {
      val size = sampleSize.toInt
      if (cache(size) < 0) {
        cache(size) = Probability.studentTInverse(1 - confidence, size - 1)
      }
      cache(size)
    }
  }
}
