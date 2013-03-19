package spark.rdd

import spark.{Dependency, OneToOneDependency, NarrowDependency, RDD, Partition, TaskContext}
import java.io.{ObjectOutputStream, IOException}

private[spark] case class CoalescedRDDPartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int]
  ) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}

/**
 * Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
 * this RDD computes one or more of the parent ones. Will produce exactly `maxPartitions` if the
 * parent had more than this many partitions, or fewer if the parent had fewer.
 *
 * This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
 * or to avoid having a large number of small tasks when processing a directory with many files.
 */
class CoalescedRDD[T: ClassManifest](
    @transient var prev: RDD[T],
    maxPartitions: Int)
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  override def getPartitions: Array[Partition] = {
    val prevSplits = prev.partitions
    if (prevSplits.length < maxPartitions) {
      prevSplits.map(_.index).map{idx => new CoalescedRDDPartition(idx, prev, Array(idx)) }
    } else {
      (0 until maxPartitions).map { i =>
        val rangeStart = ((i.toLong * prevSplits.length) / maxPartitions).toInt
        val rangeEnd = (((i.toLong + 1) * prevSplits.length) / maxPartitions).toInt
        new CoalescedRDDPartition(i, prev, (rangeStart until rangeEnd).toArray)
      }.toArray
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentSplit =>
      firstParent[T].iterator(parentSplit, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
