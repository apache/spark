package spark.graph


/**
 * A single directed edge consisting of a source id, target id,
 * and the data associated with the Edgee.
 *
 * @tparam ED type of the edge attribute
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
  var src: Vid = 0,
  var dst: Vid = 0,
  var data: ED = nullValue[ED])
