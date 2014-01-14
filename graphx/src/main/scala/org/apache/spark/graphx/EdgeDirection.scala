package org.apache.spark.graphx

/**
 * The direction of a directed edge relative to a vertex.
 */
class EdgeDirection private (private val name: String) extends Serializable {
  /**
   * Reverse the direction of an edge.  An in becomes out,
   * out becomes in and both and either remain the same.
   */
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In   => EdgeDirection.Out
    case EdgeDirection.Out  => EdgeDirection.In
    case EdgeDirection.Either => EdgeDirection.Either
    case EdgeDirection.Both => EdgeDirection.Both
  }

  override def toString: String = "EdgeDirection." + name

  override def equals(o: Any) = o match {
    case other: EdgeDirection => other.name == name
    case _ => false
  }

  override def hashCode = name.hashCode
}


/**
 * A set of [[EdgeDirection]]s.
 */
object EdgeDirection {
  /** Edges arriving at a vertex. */
  final val In = new EdgeDirection("In")

  /** Edges originating from a vertex. */
  final val Out = new EdgeDirection("Out")

  /** Edges originating from *or* arriving at a vertex of interest. */
  final val Either = new EdgeDirection("Either")

  /** Edges originating from *and* arriving at a vertex of interest. */
  final val Both = new EdgeDirection("Both")
}
