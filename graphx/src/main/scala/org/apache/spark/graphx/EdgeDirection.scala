package org.apache.spark.graphx

/**
 * The direction of a directed edge relative to a vertex.
 */
class EdgeDirection private (private val name: String) extends Serializable {
  /**
   * Reverse the direction of an edge.  An in becomes out,
   * out becomes in and both remains both.
   */
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In   => EdgeDirection.Out
    case EdgeDirection.Out  => EdgeDirection.In
    case EdgeDirection.Both => EdgeDirection.Both
  }

  override def toString: String = "EdgeDirection." + name

  override def equals(o: Any) = o match {
    case other: EdgeDirection => other.name == name
    case _ => false
  }

  override def hashCode = name.hashCode
}


object EdgeDirection {
  /** Edges arriving at a vertex. */
  final val In = new EdgeDirection("In")

  /** Edges originating from a vertex. */
  final val Out = new EdgeDirection("Out")

  /** All edges adjacent to a vertex. */
  final val Both = new EdgeDirection("Both")
}
