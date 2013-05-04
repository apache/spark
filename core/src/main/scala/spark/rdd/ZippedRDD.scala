package spark.rdd

import spark.{Utils, OneToOneDependency, RDD, SparkContext, Partition, TaskContext}
import java.io.{ObjectOutputStream, IOException}


private[spark] class ZippedPartition[T: ClassManifest, U: ClassManifest](
    idx: Int,
    @transient rdd1: RDD[T],
    @transient rdd2: RDD[U]
  ) extends Partition {

  var partition1 = rdd1.partitions(idx)
  var partition2 = rdd2.partitions(idx)
  override val index: Int = idx

  def partitions = (partition1, partition2)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent partition at the time of task serialization
    partition1 = rdd1.partitions(idx)
    partition2 = rdd2.partitions(idx)
    oos.defaultWriteObject()
  }
}

class ZippedRDD[T: ClassManifest, U: ClassManifest](
    sc: SparkContext,
    var rdd1: RDD[T],
    var rdd2: RDD[U])
  extends RDD[(T, U)](sc, List(new OneToOneDependency(rdd1), new OneToOneDependency(rdd2))) {

  override def getPartitions: Array[Partition] = {
    if (rdd1.partitions.size != rdd2.partitions.size) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    val array = new Array[Partition](rdd1.partitions.size)
    for (i <- 0 until rdd1.partitions.size) {
      array(i) = new ZippedPartition(i, rdd1, rdd2)
    }
    array
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(T, U)] = {
    val (partition1, partition2) = s.asInstanceOf[ZippedPartition[T, U]].partitions
    rdd1.iterator(partition1, context).zip(rdd2.iterator(partition2, context))
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    // Note that as number of slaves in cluster increase, the computed preferredLocations can become small : so we might need
    // to look at alternate strategies to alleviate this. (If there are no (or very small number of preferred locations), we
    // will end up transferred the blocks to 'any' node in the cluster - paying with n/w and cache cost.
    // Maybe pick one or the other ? (so that atleast one block is local ?).
    // Choose node which is hosting 'larger' of the blocks ?
    // Look at rack locality to ensure chosen host is atleast rack local to both hosting node ?, etc (would be good to defer this if possible)
    val (partition1, partition2) = s.asInstanceOf[ZippedPartition[T, U]].partitions
    val pref1 = rdd1.preferredLocations(partition1)
    val pref2 = rdd2.preferredLocations(partition2)

    // exact match - instance local and host local.
    val exactMatchLocations = pref1.intersect(pref2)

    // remove locations which are already handled via exactMatchLocations, and intersect where both partitions are node local.
    val otherNodeLocalPref1 = pref1.filter(loc => ! exactMatchLocations.contains(loc)).map(loc => Utils.parseHostPort(loc)._1)
    val otherNodeLocalPref2 = pref2.filter(loc => ! exactMatchLocations.contains(loc)).map(loc => Utils.parseHostPort(loc)._1)
    val otherNodeLocalLocations = otherNodeLocalPref1.intersect(otherNodeLocalPref2)


    // Can have mix of instance local (hostPort) and node local (host) locations as preference !
    exactMatchLocations ++ otherNodeLocalLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
