package spark.rdd

import java.util.{HashMap => JHashMap}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import spark.RDD
import spark.Partitioner
import spark.Dependency
import spark.TaskContext
import spark.Partition
import spark.SparkEnv
import spark.ShuffleDependency
import spark.OneToOneDependency


/**
 * An optimized version of cogroup for set difference/subtraction.
 *
 * It is possible to implement this operation with just `cogroup`, but
 * that is less efficient because all of the entries from `rdd2`, for
 * both matching and non-matching values in `rdd1`, are kept in the
 * JHashMap until the end.
 *
 * With this implementation, only the entries from `rdd1` are kept in-memory,
 * and the entries from `rdd2` are essentially streamed, as we only need to
 * touch each once to decide if the value needs to be removed.
 *
 * This is particularly helpful when `rdd1` is much smaller than `rdd2`, as
 * you can use `rdd1`'s partitioner/partition size and not worry about running
 * out of memory because of the size of `rdd2`.
 */
private[spark] class SubtractedRDD[K: ClassManifest, V: ClassManifest, W: ClassManifest](
    @transient var rdd1: RDD[(K, V)],
    @transient var rdd2: RDD[(K, W)],
    part: Partitioner,
    val serializerClass: String = null)
  extends RDD[(K, V)](rdd1.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(rdd1, rdd2).map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        new ShuffleDependency(rdd.asInstanceOf[RDD[(K, Any)]], part, serializerClass)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will depend on rdd1 and rdd2
      array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val serializer = SparkEnv.get.serializerManager.get(serializerClass)
    val map = new JHashMap[K, ArrayBuffer[V]]
    def getSeq(k: K): ArrayBuffer[V] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = new ArrayBuffer[V]()
        map.put(k, seq)
        seq
      }
    }
    def integrate(dep: CoGroupSplitDep, op: ((K, V)) => Unit) = dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        for (t <- rdd.iterator(itsSplit, context))
          op(t.asInstanceOf[(K, V)])
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        val iter = SparkEnv.get.shuffleFetcher.fetch(shuffleId, partition.index,
          context.taskMetrics, serializer)
        for (t <- iter)
          op(t.asInstanceOf[(K, V)])
      }
    }
    // the first dep is rdd1; add all values to the map
    integrate(partition.deps(0), t => getSeq(t._1) += t._2)
    // the second dep is rdd2; remove all of its keys
    integrate(partition.deps(1), t => map.remove(t._1))
    map.iterator.map { t =>  t._2.iterator.map { (t._1, _) } }.flatten
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

}
