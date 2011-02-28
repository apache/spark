package spark

@serializable
abstract class Partitioner[K] {
  def numPartitions: Int
  def getPartition(key: K): Int
}

class HashPartitioner[K](partitions: Int) extends Partitioner[K] {
  def numPartitions = partitions

  def getPartition(key: K) = {
    val mod = key.hashCode % partitions
    if (mod < 0) mod + partitions else mod // Guard against negative hash codes
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner[_] =>
      h.numPartitions == numPartitions
    case _ => false
  }
}