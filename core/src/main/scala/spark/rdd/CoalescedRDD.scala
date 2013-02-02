package spark.rdd

import spark.{Dependency, OneToOneDependency, NarrowDependency, RDD, Split, TaskContext}
import java.io.{ObjectOutputStream, IOException}

private[spark] case class CoalescedRDDSplit(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int]
  ) extends Split {
  var parents: Seq[Split] = parentsIndices.map(rdd.splits(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    parents = parentsIndices.map(rdd.splits(_))
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

  override def getSplits: Array[Split] = {
    val prevSplits = prev.splits
    if (prevSplits.length < maxPartitions) {
      prevSplits.map(_.index).map{idx => new CoalescedRDDSplit(idx, prev, Array(idx)) }
    } else {
      (0 until maxPartitions).map { i =>
        val rangeStart = (i * prevSplits.length) / maxPartitions
        val rangeEnd = ((i + 1) * prevSplits.length) / maxPartitions
        new CoalescedRDDSplit(i, prev, (rangeStart until rangeEnd).toArray)
      }.toArray
    }
  }

  override def compute(split: Split, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[CoalescedRDDSplit].parents.iterator.flatMap { parentSplit =>
      firstParent[T].iterator(parentSplit, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        splits(id).asInstanceOf[CoalescedRDDSplit].parentsIndices
    }
  )

  override def clearDependencies() {
    prev = null
  }
}
