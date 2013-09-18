package org.apache.spark.graph


/**
 * The direction of directed edge relative to a vertex used to select
 * the set of adjacent neighbors when running a neighborhood query.
 */
sealed abstract class EdgeDirection {
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In   => EdgeDirection.In
    case EdgeDirection.Out  => EdgeDirection.Out
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
