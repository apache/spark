package spark.util

/**
 * A class for tracking the statistics of a set of numbers (count, mean and variance) in a
 * numerically robust way. Includes support for merging two StatCounters. Based on Welford and
 * Chan's algorithms described at http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance.
 */
class StatCounter(values: TraversableOnce[Double]) {
  private var n: Long = 0     // Running count of our values
  private var mu: Double = 0  // Running mean of our values
  private var m2: Double = 0  // Running variance numerator (sum of (x - mean)^2)

  merge(values)

  def this() = this(Nil)

  def merge(value: Double): StatCounter = {
    val delta = value - mu
    n += 1
    mu += delta / n
    m2 += delta * (value - mu)
    this
  }

  def merge(values: TraversableOnce[Double]): StatCounter = {
    values.foreach(v => merge(v))
    this
  }

  def merge(other: StatCounter): StatCounter = {
    if (other == this) {
      merge(other.copy())  // Avoid overwriting fields in a weird order
    } else {
      val delta = other.mu - mu
      if (other.n * 10 < n) {
        mu = mu + (delta * other.n) / (n + other.n)
      } else if (n * 10 < other.n) {
        mu = other.mu - (delta * n) / (n + other.n)
      } else {
        mu = (mu * n + other.mu * other.n) / (n + other.n)
      }
      m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n)
      n += other.n
      this
    }
  }

  def copy(): StatCounter = {
    val other = new StatCounter
    other.n = n
    other.mu = mu
    other.m2 = m2
    other
  }

  def count: Long = n

  def mean: Double = mu

  def sum: Double = n * mu

  def variance: Double = {
    if (n == 0)
      Double.NaN
    else
      m2 / n
  }

  def sampleVariance: Double = {
    if (n <= 1)
      Double.NaN
    else
      m2 / (n - 1)
  }

  def stdev: Double = math.sqrt(variance)

  def sampleStdev: Double = math.sqrt(sampleVariance)

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f)".format(count, mean, stdev)
  }
}

object StatCounter {
  def apply(values: TraversableOnce[Double]) = new StatCounter(values)

  def apply(values: Double*) = new StatCounter(values)
}
