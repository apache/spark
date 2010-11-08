package spark

import mesos.SlaveOffer


/**
 * An RDD that takes the splits of a parent RDD and gives them unique indexes.
 * This is useful for a variety of shuffle implementations.
 */
class NumberedSplitRDD[T: ClassManifest](prev: RDD[T])
extends RDD[(Int, Iterator[T])](prev.sparkContext) {
  @transient val splits_ = {
    prev.splits.zipWithIndex.map {
      case (s, i) => new NumberedSplitRDDSplit(s, i): Split
    }.toArray
  }

  override def splits = splits_

  override def preferredLocations(split: Split) = {
    val nsplit = split.asInstanceOf[NumberedSplitRDDSplit]
    prev.preferredLocations(nsplit.prev)
  }

  override def iterator(split: Split) = {
    val nsplit = split.asInstanceOf[NumberedSplitRDDSplit]
    Iterator((nsplit.index, prev.iterator(nsplit.prev)))
  }

  override def taskStarted(split: Split, slot: SlaveOffer) = {
    val nsplit = split.asInstanceOf[NumberedSplitRDDSplit]
    prev.taskStarted(nsplit.prev, slot)
  }
}


/**
 * A split in a NumberedSplitRDD.
 */
class NumberedSplitRDDSplit(val prev: Split, val index: Int) extends Split {
  override def getId() = "NumberedSplitRDDSplit(%d)".format(index)
}
