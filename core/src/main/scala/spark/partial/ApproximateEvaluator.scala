package spark.partial

/**
 * An object that computes a function incrementally by merging in results of type U from multiple
 * tasks. Allows partial evaluation at any point by calling currentResult().
 */
private[spark] trait ApproximateEvaluator[U, R] {
  def merge(outputId: Int, taskResult: U): Unit
  def currentResult(): R
}
