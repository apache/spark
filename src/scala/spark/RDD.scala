package spark

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.HashSet
import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import mesos._

import com.google.common.collect.MapMaker

@serializable
abstract class RDD[T: ClassManifest, Split](
    @transient sc: SparkContext) {
  def splits: Array[Split]
  def iterator(split: Split): Iterator[T]
  def preferredLocations(split: Split): Seq[String]

  def taskStarted(split: Split, slot: SlaveOffer) {}

  def sparkContext = sc

  def map[U: ClassManifest](f: T => U) = new MappedRDD(this, sc.clean(f))
  def filter(f: T => Boolean) = new FilteredRDD(this, sc.clean(f))
  def aggregateSplit() = new SplitRDD(this)
  def cache() = new CachedRDD(this)
  def sample(withReplacement: Boolean, frac: Double, seed: Int) = new SampledRDD(this, withReplacement, frac, seed)

  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    val tasks = splits.map(s => new ForeachTask(this, s, cleanF)).toArray
    sc.runTaskObjects(tasks)
  }

  def collect(): Array[T] = {
    val tasks = splits.map(s => new CollectTask(this, s))
    val results = sc.runTaskObjects(tasks)
    Array.concat(results: _*)
  }

  def toArray(): Array[T] = collect()

  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val tasks = splits.map(s => new ReduceTask(this, s, f))
    val results = new ArrayBuffer[T]
    for (option <- sc.runTaskObjects(tasks); elem <- option)
      results += elem
    if (results.size == 0)
      throw new UnsupportedOperationException("empty collection")
    else
      return results.reduceLeft(f)
  }

  def take(num: Int): Array[T] = {
    if (num == 0)
      return new Array[T](0)
    val buf = new ArrayBuffer[T]
    for (split <- splits; elem <- iterator(split)) {
      buf += elem
      if (buf.length == num)
        return buf.toArray
    }
    return buf.toArray
  }

  def first: T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  def count(): Long = 
    try { map(x => 1L).reduce(_+_) }
    catch { case e: UnsupportedOperationException => 0L }

  def union[OtherSplit](other: RDD[T, OtherSplit]) =
    new UnionRDD(sc, this, other)

  def ++[OtherSplit](other: RDD[T, OtherSplit]) = this.union(other)

}

@serializable
abstract class RDDTask[U: ClassManifest, T: ClassManifest, Split](
  val rdd: RDD[T, Split], val split: Split)
extends Task[U] {
  override def preferredLocations() = rdd.preferredLocations(split)
  override def markStarted(slot: SlaveOffer) { rdd.taskStarted(split, slot) }
}

class ForeachTask[T: ClassManifest, Split](
  rdd: RDD[T, Split], split: Split, func: T => Unit)
extends RDDTask[Unit, T, Split](rdd, split) {
  override def run() {
    println("Processing " + split)
    rdd.iterator(split).foreach(func)
  }
}

class CollectTask[T, Split](
  rdd: RDD[T, Split], split: Split)(implicit m: ClassManifest[T])
extends RDDTask[Array[T], T, Split](rdd, split) {
  override def run(): Array[T] = {
    println("Processing " + split)
    rdd.iterator(split).toArray(m)
  }
}

class ReduceTask[T: ClassManifest, Split](
  rdd: RDD[T, Split], split: Split, f: (T, T) => T)
extends RDDTask[Option[T], T, Split](rdd, split) {
  override def run(): Option[T] = {
    println("Processing " + split)
    val iter = rdd.iterator(split)
    if (iter.hasNext)
      Some(iter.reduceLeft(f))
    else
      None
  }
}

class MappedRDD[U: ClassManifest, T: ClassManifest, Split](
  prev: RDD[T, Split], f: T => U) 
extends RDD[U, Split](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) = prev.iterator(split).map(f)
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class FilteredRDD[T: ClassManifest, Split](
  prev: RDD[T, Split], f: T => Boolean) 
extends RDD[T, Split](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) = prev.iterator(split).filter(f)
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class SplitRDD[T: ClassManifest, Split](
  prev: RDD[T, Split]) 
extends RDD[Array[T], Split](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) = Iterator.fromArray(Array(prev.iterator(split).toArray))
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}


@serializable class SeededSplit[Split](val prev: Split, val seed: Int) {}

class SampledRDD[T: ClassManifest, Split](
  prev: RDD[T, Split], withReplacement: Boolean, frac: Double, seed: Int) 
extends RDD[T, SeededSplit[Split]](prev.sparkContext) {
  @transient val splits_ = { val rg = new Random(seed); prev.splits.map(x => new SeededSplit(x, rg.nextInt)) }
  override def splits = splits_
  override def preferredLocations(split: SeededSplit[Split]) = prev.preferredLocations(split.prev)
  override def iterator(split: SeededSplit[Split]) = { 
    val rg = new Random(split.seed);
    // Sampling with replacement (TODO: use reservoir sampling to make this more efficient?)
    if (withReplacement) {
      val oldData = prev.iterator(split.prev).toArray
      val sampleSize = (oldData.size * frac).ceil.toInt
      val sampledData = for (i <- 1 to sampleSize) yield oldData(rg.nextInt(oldData.size)) // all of oldData's indices are candidates, even if sampleSize < oldData.size
      sampledData.iterator
    }
    // Sampling without replacement
    else {
      prev.iterator(split.prev).filter(x => (rg.nextDouble <= frac))
    }
  }
  override def taskStarted(split: SeededSplit[Split], slot: SlaveOffer) = prev.taskStarted(split.prev, slot)
}


class CachedRDD[T, Split](
  prev: RDD[T, Split])(implicit m: ClassManifest[T])
extends RDD[T, Split](prev.sparkContext) {
  val id = CachedRDD.newId()
  @transient val cacheLocs = Map[Split, List[String]]()

  override def splits = prev.splits

  override def preferredLocations(split: Split) = {
    if (cacheLocs.contains(split))
      cacheLocs(split)
    else
      prev.preferredLocations(split)
  }
  
  override def iterator(split: Split): Iterator[T] = {
    val key = id + "::" + split.toString
    val cache = CachedRDD.cache
    val loading = CachedRDD.loading
    val cachedVal = cache.get(key)
    if (cachedVal != null) {
      // Split is in cache, so just return its values
      return Iterator.fromArray(cachedVal.asInstanceOf[Array[T]])
    } else {
      // Mark the split as loading (unless someone else marks it first)
      loading.synchronized {
        if (loading.contains(key)) {
          while (loading.contains(key)) {
            try {loading.wait()} catch {case _ =>}
          }
          return Iterator.fromArray(cache.get(key).asInstanceOf[Array[T]])
        } else {
          loading.add(key)
        }
      }
      // If we got here, we have to load the split
      println("Loading and caching " + split)
      val array = prev.iterator(split).toArray(m)
      cache.put(key, array)
      loading.synchronized {
        loading.remove(key)
        loading.notifyAll()
      }
      return Iterator.fromArray(array)
    }
  }

  override def taskStarted(split: Split, slot: SlaveOffer) {
    val oldList = cacheLocs.getOrElse(split, Nil)
    val host = slot.getHost
    if (!oldList.contains(host))
      cacheLocs(split) = host :: oldList
  }
}

private object CachedRDD {
  val nextId = new AtomicLong(0) // Generates IDs for cached RDDs (on master)
  def newId() = nextId.getAndIncrement()

  // Stores map results for various splits locally (on workers)
  val cache = new MapMaker().softValues().makeMap[String, AnyRef]()

  // Remembers which splits are currently being loaded (on workers)
  val loading = new HashSet[String]
}

@serializable
abstract class UnionSplit[T: ClassManifest] {
  def iterator(): Iterator[T]
  def preferredLocations(): Seq[String]
}

@serializable
class UnionSplitImpl[T: ClassManifest, Split](
  rdd: RDD[T, Split], split: Split)
extends UnionSplit[T] {
  override def iterator() = rdd.iterator(split)
  override def preferredLocations() = rdd.preferredLocations(split)
}

@serializable
class UnionRDD[T: ClassManifest, Split1, Split2](
  sc: SparkContext, rdd1: RDD[T, Split1], rdd2: RDD[T, Split2])
extends RDD[T, UnionSplit[T]](sc) {

  @transient val splits_ : Array[UnionSplit[T]] = {
    val a1 = rdd1.splits.map(s => new UnionSplitImpl(rdd1, s))
    val a2 = rdd2.splits.map(s => new UnionSplitImpl(rdd2, s))
    (a1 ++ a2).toArray
  }

  override def splits = splits_

  override def iterator(s: UnionSplit[T]): Iterator[T] = s.iterator()

  override def preferredLocations(s: UnionSplit[T]): Seq[String] = 
    s.preferredLocations()
}
