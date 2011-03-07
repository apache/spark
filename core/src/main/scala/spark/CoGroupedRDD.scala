package spark

import java.net.URL
import java.io.EOFException
import java.io.ObjectInputStream
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

@serializable
sealed trait CoGroupSplitDep
case class NarrowCoGroupSplitDep(rdd: RDD[_], split: Split) extends CoGroupSplitDep
case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep

@serializable
class CoGroupSplit(idx: Int, val deps: Seq[CoGroupSplitDep])
extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

@serializable
class CoGroupAggregator[K] extends Aggregator[K, Any, ArrayBuffer[Any]] (
  { x => ArrayBuffer(x) },
  { (b, x) => b += x },
  { (b1, b2) => b1 ++ b2 }
)

class CoGroupedRDD[K](rdds: Seq[RDD[(K, _)]], part: Partitioner[K])
extends RDD[(K, Seq[Seq[_]])](rdds.first.context) {
  val aggr = new CoGroupAggregator[K]
  
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        deps += new OneToOneDependency(rdd)
      } else {
        deps += new ShuffleDependency[K, Any, ArrayBuffer[Any]](
            context.newShuffleId, rdd, aggr, part)
      }
    }
    deps.toList
  }
  
  @transient val splits_ : Array[Split] = {
    val firstRdd = rdds.first
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId): CoGroupSplitDep
          case _ =>
            new NarrowCoGroupSplitDep(r, r.splits(i)): CoGroupSplitDep
        }
      }.toList)
    }
    array
  }

  override def splits = splits_
  
  override val partitioner: Option[Partitioner[_]] = Some(part.asInstanceOf[Partitioner[_]])
  
  override def preferredLocations(s: Split) = Nil
  
  override def compute(s: Split): Iterator[(K, Seq[Seq[_]])] = {
    val split = s.asInstanceOf[CoGroupSplit]
    val map = new HashMap[K, Seq[ArrayBuffer[Any]]]
    def getSeq(k: K): Seq[ArrayBuffer[Any]] = {
      map.getOrElseUpdate(k, Array.fill(rdds.size)(new ArrayBuffer[Any]))
    }
    for ((dep, index) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, itsSplit) => {
        // Read them from the parent
        for ((k: K, v) <- rdd.iterator(itsSplit)) {
          getSeq(k)(index) += v
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        val splitsByUri = new HashMap[String, ArrayBuffer[Int]]
        val serverUris = MapOutputTracker.getServerUris(shuffleId)
        for ((serverUri, index) <- serverUris.zipWithIndex) {
          splitsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
        }
        for ((serverUri, inputIds) <- Utils.shuffle(splitsByUri)) {
          for (i <- inputIds) {
            val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, index)
            val inputStream = new ObjectInputStream(new URL(url).openStream())
            try {
              while (true) {
                val (k, vs) = inputStream.readObject().asInstanceOf[(K, Seq[Any])]
                val mySeq = getSeq(k)
                for (v <- vs)
                  mySeq(index) += v
              }
            } catch {
              case e: EOFException => {}
            }
            inputStream.close()
          }
        }
      }
    }
    map.iterator
  }
}