package spark

import java.io.FileOutputStream
import java.io.ObjectOutputStream
import scala.collection.mutable.HashMap


class ShuffleMapTask(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_,_], val partition: Int, locs: Seq[String])
extends DAGTask[String](stageId) with Logging {
  val split = rdd.splits(partition)

  override def run (attemptId: Int): String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
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
    val ser = SparkEnv.get.serializer.newInstance()
    for (i <- 0 until numOutputSplits) {
      val file = LocalFileShuffle.getOutputFile(dep.shuffleId, partition, i)
      val out = ser.outputStream(new FileOutputStream(file))
      buckets(i).foreach(pair => out.writeObject(pair))
      // TODO: have some kind of EOF marker
      out.close()
    }
    return LocalFileShuffle.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
