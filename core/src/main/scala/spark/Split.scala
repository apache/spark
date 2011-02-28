package spark

/**
 * A partition of an RDD.
 */
@serializable trait Split {
  /**
   * Get the split's index within its parent RDD
   */
  val index: Int
  
  // A better default implementation of HashCode
  override def hashCode(): Int = index
}
