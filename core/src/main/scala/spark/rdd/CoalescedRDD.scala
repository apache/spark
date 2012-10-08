package spark.rdd

import spark.NarrowDependency
import spark.RDD
import spark.Split

private class CoalescedRDDSplit(val index: Int, val parents: Array[Split]) extends Split

/**
 * Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
 * this RDD computes one or more of the parent ones. Will produce exactly `maxPartitions` if the
 * parent had more than this many partitions, or fewer if the parent had fewer.
 *
 * This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
 * or to avoid having a large number of small tasks when processing a directory with many files.
 */
class CoalescedRDD[T: ClassManifest](prev: RDD[T], maxPartitions: Int)
  extends RDD[T](prev.context) {

  @transient val splits_ : Array[Split] = {
    val prevSplits = prev.splits
    if (prevSplits.length < maxPartitions) {
      prevSplits.zipWithIndex.map{ case (s, idx) => new CoalescedRDDSplit(idx, Array(s)) }
    } else {
      (0 until maxPartitions).map { i =>
        val rangeStart = (i * prevSplits.length) / maxPartitions
        val rangeEnd = ((i + 1) * prevSplits.length) / maxPartitions
        new CoalescedRDDSplit(i, prevSplits.slice(rangeStart, rangeEnd))
      }.toArray
    }
  }

  override def splits = splits_

  override def compute(split: Split): Iterator[T] = {
    split.asInstanceOf[CoalescedRDDSplit].parents.iterator.flatMap {
      parentSplit => prev.iterator(parentSplit)
    }
  }

  val dependencies = List(
    new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        splits(id).asInstanceOf[CoalescedRDDSplit].parents.map(_.index)
    }
  )
}
