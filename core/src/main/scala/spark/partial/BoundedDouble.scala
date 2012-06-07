package spark.partial

/**
 * A Double with error bars on it.
 */
class BoundedDouble(val mean: Double, val confidence: Double, val low: Double, val high: Double) {
  override def toString(): String = "[%.3f, %.3f]".format(low, high)
}
