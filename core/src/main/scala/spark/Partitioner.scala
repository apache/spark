package spark

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If any of the RDDs already has a partitioner, choose that one.
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   *
   * Unless spark.default.parallelism is set, He number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    for (r <- bySize if r.partitioner != None) {
      return r.partitioner.get
    }
    if (System.getProperty("spark.default.parallelism") != null) {
      return new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      return new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

/**
 * A [[spark.Partitioner]] that implements hash-based partitioning using Java's `Object.hashCode`.
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    if (key == null) {
      return 0
    } else {
      val mod = key.hashCode % partitions
      if (mod < 0) {
        mod + partitions
      } else {
        mod // Guard against negative hash codes
      }
    }
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}

/**
 * A [[spark.Partitioner]] that partitions sortable records by range into roughly equal ranges.
 * Determines the ranges by sampling the RDD passed in.
 */
class RangePartitioner[K <% Ordered[K]: ClassManifest, V](
    partitions: Int,
    @transient rdd: RDD[(K,V)],
    private val ascending: Boolean = true) 
  extends Partitioner {

  // An array of upper bounds for the first (partitions - 1) partitions
  private val rangeBounds: Array[K] = {
    if (partitions == 1) {
      Array()
    } else {
      val rddSize = rdd.count()
      val maxSampleSize = partitions * 20.0
      val frac = math.min(maxSampleSize / math.max(rddSize, 1), 1.0)
      val rddSample = rdd.sample(false, frac, 1).map(_._1).collect().sortWith(_ < _)
      if (rddSample.length == 0) {
        Array()
      } else {
        val bounds = new Array[K](partitions - 1)
        for (i <- 0 until partitions - 1) {
          val index = (rddSample.length - 1) * (i + 1) / partitions
          bounds(i) = rddSample(index)
        }
        bounds
      }
    }
  }

  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    // TODO: Use a binary search here if number of partitions is large
    val k = key.asInstanceOf[K]
    var partition = 0
    while (partition < rangeBounds.length && k > rangeBounds(partition)) {
      partition += 1
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_,_] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }
}
