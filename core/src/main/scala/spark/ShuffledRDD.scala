package spark

import scala.collection.mutable.HashMap


class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C](
  parent: RDD[(K, V)],
  aggregator: Aggregator[K, V, C],
  part : Partitioner)
extends RDD[(K, C)](parent.context) {
  //override val partitioner = Some(part)
  override val partitioner = Some(part)
  
  @transient val splits_ =
    Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new HashMap[K, C]
    def mergePair(k: K, c: C) {
      combiners(k) = combiners.get(k) match {
        case Some(oldC) => aggregator.mergeCombiners(oldC, c)
        case None => c
      }
    }
    new SimpleShuffleFetcher().fetch[K, C](dep.shuffleId, split.index, mergePair)
    combiners.iterator
  }
}
