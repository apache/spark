package spark.graph


sealed abstract class EdgeDirection {
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In   => EdgeDirection.In
    case EdgeDirection.Out  => EdgeDirection.Out
    case EdgeDirection.Both => EdgeDirection.Both
  }
}


object EdgeDirection {
  case object In extends EdgeDirection
  case object Out extends EdgeDirection
  case object Both extends EdgeDirection
}
