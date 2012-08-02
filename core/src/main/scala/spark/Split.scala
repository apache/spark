package spark

/**
 * A partition of an RDD.
 */
trait Split extends Serializable {
  /**
   * Get the split's index within its parent RDD
   */
  def index: Int
  
  // A better default implementation of HashCode
  override def hashCode(): Int = index
}
