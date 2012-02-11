package spark

abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any) = {
    val mod = key.hashCode % partitions
    if (mod < 0) {
      mod + partitions
    } else {
      mod // Guard against negative hash codes
    }
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}

class RangePartitioner[K <% Ordered[K],V](partitions: Int, rdd: RDD[(K,V)], ascending: Boolean = true) 
  extends Partitioner {

  def numPartitions = partitions

  val rddSize = rdd.count()
  val maxSampleSize = partitions*10.0
  val frac = 1.0.min(maxSampleSize / rddSize)
  val rddSample = rdd.sample(true, frac, 1).collect.toList
    .sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1)
    .map(_._1)
  val bucketSize:Float = rddSample.size / partitions
  val rangeBounds = rddSample.zipWithIndex.filter(_._2 % bucketSize == 0)
    .map(_._1).slice(1, partitions)

  def getPartition(key: Any): Int = { 
    key match {
      case k:K => {
        val p = 
          rangeBounds.zipWithIndex.foldLeft(0) {
            case (part, (bound, index)) =>  
              if (k > bound) index + 1 else part
          }   
        if (ascending) p else numPartitions-1-p
      }   
      case _ => 0
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[K,V] =>
      r.numPartitions == numPartitions
    case _ => false
  }
}

