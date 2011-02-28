package spark

import java.net.URL
import java.io.EOFException
import java.io.ObjectInputStream
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C](
  parent: RDD[(K, V)],
  aggregator: Aggregator[K, V, C],
  part : Partitioner[K])
extends RDD[(K, C)](parent.context) {
  override val partitioner = Some(part)
  
  @transient val splits_ =
    Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val shuffleId = dep.shuffleId
    val splitId = split.index
    val splitsByUri = new HashMap[String, ArrayBuffer[Int]]
    val serverUris = MapOutputTracker.getServerUris(shuffleId)
    for ((serverUri, index) <- serverUris.zipWithIndex) {
      splitsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
    }
    val combiners = new HashMap[K, C]
    for ((serverUri, inputIds) <- Utils.shuffle(splitsByUri)) {
      for (i <- inputIds) {
        val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, splitId)
        val inputStream = new ObjectInputStream(new URL(url).openStream())
        try {
          while (true) {
            val (k, c) = inputStream.readObject().asInstanceOf[(K, C)]
            combiners(k) = combiners.get(k) match {
              case Some(oldC) => aggregator.mergeCombiners(oldC, c)
              case None => c
            }
          }
        } catch {
          case e: EOFException => {}
        }
        inputStream.close()
      }
    }
    combiners.iterator
  }
}