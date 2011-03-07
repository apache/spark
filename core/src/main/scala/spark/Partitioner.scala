package spark

@serializable
abstract class Partitioner {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any) = {
    val mod = key.hashCode % partitions
    if (mod < 0) mod + partitions else mod // Guard against negative hash codes
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ => false
  }
}