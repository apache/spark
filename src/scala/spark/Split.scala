package spark

/**
 * A partition of an RDD.
 */
@serializable trait Split {
  /**
   * Get a unique ID for this split which can be used, for example, to
   * set up caches based on it. The ID should stay the same if we serialize
   * and then deserialize the split.
   */
  def getId(): String
}
