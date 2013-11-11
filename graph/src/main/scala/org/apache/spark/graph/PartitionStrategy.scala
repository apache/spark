package org.apache.spark.graph

//import org.apache.spark.graph._


sealed trait PartitionStrategy extends Serializable { def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid}

//case object EdgePartition2D extends PartitionStrategy {
object EdgePartition2D extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val ceilSqrtNumParts: Pid = math.ceil(math.sqrt(numParts)).toInt
    val mixingPrime: Vid = 1125899906842597L
    val col: Pid = ((math.abs(src) * mixingPrime) % ceilSqrtNumParts).toInt
    val row: Pid = ((math.abs(dst) * mixingPrime) % ceilSqrtNumParts).toInt
    (col * ceilSqrtNumParts + row) % numParts
  }
}



object EdgePartition1D extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val mixingPrime: Vid = 1125899906842597L
    (math.abs(src) * mixingPrime).toInt % numParts
  }
}


object RandomVertexCut extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    math.abs((src, dst).hashCode()) % numParts
  }
}


object CanonicalRandomVertexCut extends PartitionStrategy {
  override def getPartition(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val lower = math.min(src, dst)
    val higher = math.max(src, dst)
    math.abs((lower, higher).hashCode()) % numParts
  }
}
