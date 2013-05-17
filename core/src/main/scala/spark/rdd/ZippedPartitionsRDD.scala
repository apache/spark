package spark.rdd

import spark.{Utils, OneToOneDependency, RDD, SparkContext, Partition, TaskContext}
import java.io.{ObjectOutputStream, IOException}

private[spark] class ZippedPartitionsPartition(
    idx: Int,
    @transient rdds: Seq[RDD[_]])
  extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))
  def partitions = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    oos.defaultWriteObject()
  }
}

abstract class ZippedPartitionsBaseRDD[V: ClassManifest](
    sc: SparkContext,
    var rdds: Seq[RDD[_]])
  extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

  override def getPartitions: Array[Partition] = {
    val sizes = rdds.map(x => x.partitions.size)
    if (!sizes.forall(x => x == sizes(0))) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    val array = new Array[Partition](sizes(0))
    for (i <- 0 until sizes(0)) {
      array(i) = new ZippedPartitionsPartition(i, rdds)
    }
    array
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    // Note that as number of rdd's increase and/or number of slaves in cluster increase, the computed preferredLocations below
    // become diminishingly small : so we might need to look at alternate strategies to alleviate this.
    // If there are no (or very small number of preferred locations), we will end up transferred the blocks to 'any' node in the
    // cluster - paying with n/w and cache cost.
    // Maybe pick a node which figures max amount of time ?
    // Choose node which is hosting 'larger' of some subset of blocks ?
    // Look at rack locality to ensure chosen host is atleast rack local to both hosting node ?, etc (would be good to defer this if possible)
    val splits = s.asInstanceOf[ZippedPartitionsPartition].partitions
    val rddSplitZip = rdds.zip(splits)

    // exact match.
    val exactMatchPreferredLocations = rddSplitZip.map(x => x._1.preferredLocations(x._2))
    val exactMatchLocations = exactMatchPreferredLocations.reduce((x, y) => x.intersect(y))

    // Remove exact match and then do host local match.
    val otherNodePreferredLocations = rddSplitZip.map(x => {
      x._1.preferredLocations(x._2).map(hostPort => {
        val host = Utils.parseHostPort(hostPort)._1

        if (exactMatchLocations.contains(host)) null else host
      }).filter(_ != null)
    })
    val otherNodeLocalLocations = otherNodePreferredLocations.reduce((x, y) => x.intersect(y))

    otherNodeLocalLocations ++ exactMatchLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

class ZippedPartitionsRDD2[A: ClassManifest, B: ClassManifest, V: ClassManifest](
    sc: SparkContext,
    f: (Iterator[A], Iterator[B]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B])
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2)) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}

class ZippedPartitionsRDD3
  [A: ClassManifest, B: ClassManifest, C: ClassManifest, V: ClassManifest](
    sc: SparkContext,
    f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C])
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3)) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
  }
}

class ZippedPartitionsRDD4
  [A: ClassManifest, B: ClassManifest, C: ClassManifest, D:ClassManifest, V: ClassManifest](
    sc: SparkContext,
    f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    var rdd4: RDD[D])
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3, rdd4)) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context),
      rdd4.iterator(partitions(3), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    rdd4 = null
  }
}
