package spark.rdd

import java.io.{ObjectOutputStream, IOException}
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

import spark.{Aggregator, Logging, Partitioner, RDD, SparkEnv, Split, TaskContext}
import spark.{Dependency, OneToOneDependency, ShuffleDependency}


private[spark] sealed trait CoGroupSplitDep extends Serializable

private[spark] case class NarrowCoGroupSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Split
  ) extends CoGroupSplitDep {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.splits(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep

private[spark]
class CoGroupSplit(idx: Int, val deps: Seq[CoGroupSplitDep]) extends Split with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

private[spark] class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    { (b1, b2) => b1 ++ b2 })
  with Serializable

class CoGroupedRDD[K](@transient var rdds: Seq[RDD[(_, _)]], part: Partitioner)
  extends RDD[(K, Seq[Seq[_]])](rdds.head.context, Nil) with Logging {

  private val aggr = new CoGroupAggregator

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        val mapSideCombinedRDD = rdd.mapPartitions(aggr.combineValuesByKey(_), true)
        new ShuffleDependency[Any, ArrayBuffer[Any]](mapSideCombinedRDD, part)
      }
    }
  }

  override def getSplits: Array[Split] = {
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupSplit will have a dependency per contributing RDD
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.splits(i))
        }
      }.toList)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(s: Split, context: TaskContext): Iterator[(K, Seq[Seq[_]])] = {
    val split = s.asInstanceOf[CoGroupSplit]
    val numRdds = split.deps.size
    // e.g. for `(k, a) cogroup (k, b)`, K -> Seq(ArrayBuffer as, ArrayBuffer bs)
    val map = new JHashMap[K, Seq[ArrayBuffer[Any]]]
    def getSeq(k: K): Seq[ArrayBuffer[Any]] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = Array.fill(numRdds)(new ArrayBuffer[Any])
        map.put(k, seq)
        seq
      }
    }
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit, context)) {
          getSeq(k.asInstanceOf[K])(depNum) += v
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        val fetcher = SparkEnv.get.shuffleFetcher
        val fetchItr = fetcher.fetch[K, Seq[Any]](shuffleId, split.index)
        for ((k, vs) <- fetchItr) {
          getSeq(k)(depNum) ++= vs
        }
        context.task.shuffleReadMillis = Some(fetchItr.getNetMillis)
        context.task.remoteFetchTime = Some(fetchItr.remoteFetchTime)
        context.task.remoteFetchWaitTime = Some(fetchItr.remoteFetchWaitTime)
        context.task.remoteReadBytes = Some(fetchItr.remoteBytesRead)
        context.task.totalBlocksFetched = Some(fetchItr.totalBlocks)
        context.task.localBlocksFetched = Some(fetchItr.numLocalBlocks)
        context.task.remoteBlocksFetched = Some(fetchItr.numRemoteBlocks)
      }
    }
    JavaConversions.mapAsScalaMap(map).iterator
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
