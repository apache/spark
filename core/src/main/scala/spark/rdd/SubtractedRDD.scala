package spark.rdd

import java.util.{HashSet => JHashSet}
import scala.collection.JavaConversions._
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
private[spark] class SubtractedRDD[T: ClassManifest](
    @transient var rdd1: RDD[T],
    @transient var rdd2: RDD[T],
    part: Partitioner) extends RDD[T](rdd1.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(rdd1, rdd2).map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        val mapSideCombinedRDD = rdd.mapPartitions(i => {
          val set = new JHashSet[T]()
          while (i.hasNext) {
            set.add(i.next)
          }
          set.iterator
        }, true)
        // ShuffleDependency requires a tuple (k, v), which it will partition by k.
        // We need this to partition to map to the same place as the k for
        // OneToOneDependency, which means:
        // - for already-tupled RDD[(A, B)], into getPartition(a)
        // - for non-tupled RDD[C], into getPartition(c)
        val part2 = new Partitioner() {
          def numPartitions = part.numPartitions
          def getPartition(key: Any) = key match {
            case (k, v) => part.getPartition(k)
            case k => part.getPartition(k)
          }
        }
        new ShuffleDependency(mapSideCombinedRDD.map((_, null)), part2)
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
      }.toList)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(p: Partition, context: TaskContext): Iterator[T] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val set = new JHashSet[T]
    def integrate(dep: CoGroupSplitDep, op: T => Unit) = dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) =>
        for (k <- rdd.iterator(itsSplit, context))
          op(k.asInstanceOf[T])
      case ShuffleCoGroupSplitDep(shuffleId) =>
        for ((k, _) <- SparkEnv.get.shuffleFetcher.fetch(shuffleId, partition.index, context.taskMetrics))
          op(k.asInstanceOf[T])
    }
    // the first dep is rdd1; add all keys to the set
    integrate(partition.deps(0), set.add)
    // the second dep is rdd2; remove all of its keys from the set
    integrate(partition.deps(1), set.remove)
    set.iterator
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

}