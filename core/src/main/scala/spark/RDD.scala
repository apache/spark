package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.HashSet
import java.util.Random
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.HadoopWriter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import SparkContext._

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents
 * an immutable, partitioned collection of elements that can be operated on in parallel.
 *
 * Each RDD is characterized by five main properties:
 * - A list of splits (partitions)
 * - A function for computing each split
 * - A list of dependencies on other RDDs
 * - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * - Optionally, a list of preferred locations to compute each split on (e.g. block locations for HDFS)
 *
 * All the scheduling and execution in Spark is done based on these methods, allowing each
 * RDD to implement its own way of computing itself.
 *
 * This class also contains transformation methods available on all RDDs (e.g. map and filter).
 * In addition, PairRDDFunctions contains extra methods available on RDDs of key-value pairs,
 * and SequenceFileRDDFunctions contains extra methods for saving RDDs to Hadoop SequenceFiles.
 */
@serializable
abstract class RDD[T: ClassManifest](@transient sc: SparkContext) {
  // Methods that must be implemented by subclasses
  def splits: Array[Split]
  def compute(split: Split): Iterator[T]
  val dependencies: List[Dependency[_]]
  
  // Optionally overridden by subclasses to specify how they are partitioned
  val partitioner: Option[Partitioner] = None

  // Optionally overridden by subclasses to specify placement preferences
  def preferredLocations(split: Split): Seq[String] = Nil
  
  def context = sc
  
  // Get a unique ID for this RDD
  val id = sc.newRddId()
  
  // Variables relating to caching
  private var shouldCache = false
  
  // Change this RDD's caching
  def cache(): RDD[T] = {
    shouldCache = true
    this
  }
  
  // Read this RDD; will read from cache if applicable, or otherwise compute
  final def iterator(split: Split): Iterator[T] = {
    if (shouldCache) {
      SparkEnv.get.cacheTracker.getOrCompute[T](this, split)
    } else {
      compute(split)
    }
  }
  
  // Transformations (return a new RDD)
  
  def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))
  
  def flatMap[U: ClassManifest](f: T => Traversable[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))
  
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)

  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

  def ++(other: RDD[T]): RDD[T] = this.union(other)

  def glom(): RDD[Array[T]] = new GlommedRDD(this)

  def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] =
    new CartesianRDD(sc, this, other)

  def groupBy[K: ClassManifest](f: T => K, numSplits: Int): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(numSplits)
  }

  def groupBy[K: ClassManifest](f: T => K): RDD[(K, Seq[T])] =
    groupBy[K](f, sc.defaultParallelism)

  def pipe(command: String): RDD[String] =
    new PipedRDD(this, command)

  def pipe(command: Seq[String]): RDD[String] =
    new PipedRDD(this, command)

  def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))

  // Actions (launch a job to return a value to the user program)
  
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext)
        Some(iter.reduceLeft(f))
      else
        None
    }
    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    for (opt <- options; elem <- opt)
      results += elem
    if (results.size == 0)
      throw new UnsupportedOperationException("empty collection")
    else
      return results.reduceLeft(f)
  }
  
  def count(): Long = {
    sc.runJob(this, (iter: Iterator[T]) => {
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next
      }
      result
    }).sum
  }

  def toArray(): Array[T] = collect()
  
  override def toString(): String = {
    "%s(%d)".format(getClass.getSimpleName, id)
  }

  // Take the first num elements of the RDD. This currently scans the partitions
  // *one by one*, so it will be slow if a lot of partitions are required. In that
  // case, use collect() to get the whole RDD instead.
  def take(num: Int): Array[T] = {
    if (num == 0)
      return new Array[T](0)
    val buf = new ArrayBuffer[T]
    var p = 0
    while (buf.size < num && p < splits.size) {
      val left = num - buf.size
      val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, Array(p), true)
      buf ++= res(0)
      if (buf.size == num)
        return buf.toArray
      p += 1
    }
    return buf.toArray
  }

  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString))).saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  def saveAsObjectFile(path: String) {
    this.glom.map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x)))).saveAsSequenceFile(path)
  }
}

class MappedRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T], f: T => U)
extends RDD[U](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).map(f)
}

class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T], f: T => Traversable[U])
extends RDD[U](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).toStream.flatMap(f).iterator
}

class FilteredRDD[T: ClassManifest](
  prev: RDD[T], f: T => Boolean)
extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).filter(f)
}

class GlommedRDD[T: ClassManifest](prev: RDD[T])
extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = Array(prev.iterator(split).toArray).iterator
}

class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T], f: Iterator[T] => Iterator[U])
extends RDD[U](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = f(prev.iterator(split))
}
