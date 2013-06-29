package spark.graph


class EdgeTriplet[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD,
                  @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] {
  var src: Vertex[VD] = _
  var dst: Vertex[VD] = _
  var data: ED = _

  def otherVertex(vid: Vid): Vertex[VD] = if (src.id == vid) dst else src

  def vertex(vid: Vid): Vertex[VD] = if (src.id == vid) src else dst

  def relativeDirection(vid: Vid): EdgeDirection = {
    if (vid == src.id) EdgeDirection.Out else EdgeDirection.In
  }
}
