package org.apache.spark.graph


/**
 * The direction of directed edge relative to a vertex used to select
 * the set of adjacent neighbors when running a neighborhood query.
 */
sealed abstract class EdgeDirection {
  /**
   * Reverse the direction of an edge.  An in becomes out, 
   * out becomes in and both remains both.
   */
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In   => EdgeDirection.Out
    case EdgeDirection.Out  => EdgeDirection.In
    case EdgeDirection.Both => EdgeDirection.Both
  }
}


object EdgeDirection {
  /**
   * Edges arriving at a vertex.
   */
  case object In extends EdgeDirection

  /**
   * Edges originating from a vertex
   */
  case object Out extends EdgeDirection

  /**
   * All edges adjacent to a vertex
   */
  case object Both extends EdgeDirection
}
