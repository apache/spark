package spark

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.HashSet
import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import mesos._

import com.google.common.collect.MapMaker

@serializable
abstract class RDD[T: ClassManifest](
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

  def sample(withReplacement: Boolean, frac: Double, seed: Int) =
    new SampledRDD(this, withReplacement, frac, seed)

  def flatMap[U: ClassManifest](f: T => Traversable[U]) =
    new FlatMappedRDD(this, sc.clean(f))

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

  def union(other: RDD[T]) = new UnionRDD(sc, Array(this, other))
  def cartesian[U: ClassManifest](other: RDD[U]) = new CartesianRDD(sc, this, other)

  def ++(other: RDD[T]) = this.union(other)
}

@serializable
abstract class RDDTask[U: ClassManifest, T: ClassManifest](
  val rdd: RDD[T], val split: Split)
extends Task[U] {
  override def preferredLocations() = rdd.preferredLocations(split)
  override def markStarted(slot: SlaveOffer) { rdd.taskStarted(split, slot) }
}

class ForeachTask[T: ClassManifest](
  rdd: RDD[T], split: Split, func: T => Unit)
extends RDDTask[Unit, T](rdd, split) with Logging {
  override def run() {
    logInfo("Processing " + split)
    rdd.iterator(split).foreach(func)
  }
}

class CollectTask[T](
  rdd: RDD[T], split: Split)(implicit m: ClassManifest[T])
extends RDDTask[Array[T], T](rdd, split) with Logging {
  override def run(): Array[T] = {
    logInfo("Processing " + split)
    rdd.iterator(split).toArray(m)
  }
}

class ReduceTask[T: ClassManifest](
  rdd: RDD[T], split: Split, f: (T, T) => T)
extends RDDTask[Option[T], T](rdd, split) with Logging {
  override def run(): Option[T] = {
    logInfo("Processing " + split)
    val iter = rdd.iterator(split)
    if (iter.hasNext)
      Some(iter.reduceLeft(f))
    else
      None
  }
}

class MappedRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T], f: T => U)
extends RDD[U](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) = prev.iterator(split).map(f)
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class FilteredRDD[T: ClassManifest](
  prev: RDD[T], f: T => Boolean)
extends RDD[T](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) = prev.iterator(split).filter(f)
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T], f: T => Traversable[U])
extends RDD[U](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) =
    prev.iterator(split).toStream.flatMap(f).iterator
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class SplitRDD[T: ClassManifest](prev: RDD[T])
extends RDD[Array[T]](prev.sparkContext) {
  override def splits = prev.splits
  override def preferredLocations(split: Split) = prev.preferredLocations(split)
  override def iterator(split: Split) = Iterator.fromArray(Array(prev.iterator(split).toArray))
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}


@serializable class SeededSplit(val prev: Split, val seed: Int) extends Split {
  override def getId() =
    "SeededSplit(" + prev.getId() + ", seed " + seed + ")"
}

class SampledRDD[T: ClassManifest](
  prev: RDD[T], withReplacement: Boolean, frac: Double, seed: Int)
extends RDD[T](prev.sparkContext) {

  @transient val splits_ = { val rg = new Random(seed); prev.splits.map(x => new SeededSplit(x, rg.nextInt)) }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override def preferredLocations(split: Split) = prev.preferredLocations(split.asInstanceOf[SeededSplit].prev)

  override def iterator(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SeededSplit]
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

  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split.asInstanceOf[SeededSplit].prev, slot)
}


class CachedRDD[T](
  prev: RDD[T])(implicit m: ClassManifest[T])
extends RDD[T](prev.sparkContext) with Logging {
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
    val key = id + "::" + split.getId()
    logInfo("CachedRDD split key is " + key)
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
      logInfo("Loading and caching " + split)
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
class UnionSplit[T: ClassManifest](rdd: RDD[T], split: Split)
extends Split {
  def iterator() = rdd.iterator(split)
  def preferredLocations() = rdd.preferredLocations(split)
  override def getId() = "UnionSplit(" + split.getId() + ")"
}

@serializable
class UnionRDD[T: ClassManifest](sc: SparkContext, rdds: Seq[RDD[T]])
extends RDD[T](sc) {
  @transient val splits_ : Array[Split] = {
    val splits: Seq[Split] =
      for (rdd <- rdds; split <- rdd.splits)
        yield new UnionSplit(rdd, split)
    splits.toArray
  }

  override def splits = splits_

  override def iterator(s: Split): Iterator[T] =
    s.asInstanceOf[UnionSplit[T]].iterator()

  override def preferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[UnionSplit[T]].preferredLocations()
}

@serializable class CartesianSplit(val s1: Split, val s2: Split) extends Split {
  override def getId() =
    "CartesianSplit(" + s1.getId() + ", " + s2.getId() + ")"
}

@serializable
class CartesianRDD[T: ClassManifest, U:ClassManifest](
  sc: SparkContext, rdd1: RDD[T], rdd2: RDD[U])
extends RDD[Pair[T, U]](sc) {
  @transient val splits_ = {
    // create the cross product split
    rdd2.splits.map(y => rdd1.splits.map(x => new CartesianSplit(x, y))).flatten
  }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override def preferredLocations(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)
  }

  override def iterator(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    for (x <- rdd1.iterator(currSplit.s1); y <- rdd2.iterator(currSplit.s2)) yield (x, y)
  }

  override def taskStarted(split: Split, slot: SlaveOffer) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    rdd1.taskStarted(currSplit.s1, slot)
    rdd2.taskStarted(currSplit.s2, slot)
  }
}

@serializable class PairRDDExtras[K, V](rdd: RDD[(K, V)]) {
  def reduceByKey(func: (V, V) => V): Map[K, V] = {
    def mergeMaps(m1: HashMap[K, V], m2: HashMap[K, V]): HashMap[K, V] = {
      for ((k, v) <- m2) {
        m1.get(k) match {
          case None => m1(k) = v
          case Some(w) => m1(k) = func(w, v)
        }
      }
      return m1
    }
    rdd.map(pair => HashMap(pair)).reduce(mergeMaps)
  }
}
