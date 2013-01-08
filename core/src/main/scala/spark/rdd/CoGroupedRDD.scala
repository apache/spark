package spark.rdd

import java.io.{ObjectOutputStream, IOException}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

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

  val aggr = new CoGroupAggregator

  @transient
  var deps_ = {
    val deps = new ArrayBuffer[Dependency[_]]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        deps += new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        val mapSideCombinedRDD = rdd.mapPartitions(aggr.combineValuesByKey(_), true)
        deps += new ShuffleDependency[Any, ArrayBuffer[Any]](mapSideCombinedRDD, part)
      }
    }
    deps.toList
  }

  override def getDependencies = deps_

  @transient
  var splits_ : Array[Split] = {
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId): CoGroupSplitDep
          case _ =>
            new NarrowCoGroupSplitDep(r, i, r.splits(i)): CoGroupSplitDep
        }
      }.toList)
    }
    array
  }

  override def getSplits = splits_
  
  override val partitioner = Some(part)

  override def compute(s: Split, context: TaskContext): Iterator[(K, Seq[Seq[_]])] = {
    val split = s.asInstanceOf[CoGroupSplit]
    val numRdds = split.deps.size
    val map = new HashMap[K, Seq[ArrayBuffer[Any]]]
    def getSeq(k: K): Seq[ArrayBuffer[Any]] = {
      map.getOrElseUpdate(k, Array.fill(numRdds)(new ArrayBuffer[Any]))
    }
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, itsSplitIndex, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit, context)) {
          getSeq(k.asInstanceOf[K])(depNum) += v
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        def mergePair(pair: (K, Seq[Any])) {
          val mySeq = getSeq(pair._1)
          for (v <- pair._2)
            mySeq(depNum) += v
        }
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[K, Seq[Any]](shuffleId, split.index).foreach(mergePair)
      }
    }
    map.iterator
  }

  override def clearDependencies() {
    deps_ = null
    splits_ = null
    rdds = null
  }
}
