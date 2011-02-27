package spark

import java.io.FileOutputStream
import java.io.ObjectOutputStream
import scala.collection.mutable.HashMap


class ShuffleMapTask(val stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_,_], val partition: Int, locs: Seq[String])
extends Task[String] {
  val split = rdd.splits(partition)

  override def run: String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner[Any]]
    val buckets = Array.tabulate(numOutputSplits)(_ => new HashMap[Any, Any])
    for (elem <- rdd.iterator(split)) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      var bucketId = partitioner.getPartition(k)
      val bucket = buckets(bucketId)
      bucket(k) = bucket.get(k) match {
        case Some(c) => aggregator.mergeValue(c, v)
        case None => aggregator.createCombiner(v)
      }
    }
    for (i <- 0 until numOutputSplits) {
      val file = LocalFileShuffle.getOutputFile(dep.shuffleId, partition, i)
      val out = new ObjectOutputStream(new FileOutputStream(file))
      buckets(i).foreach(pair => out.writeObject(pair))
      out.close()
    }
    return LocalFileShuffle.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}