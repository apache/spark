package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.Random
import java.util.Date
import java.util.{HashMap => JHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions.mapAsScalaMap

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

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}

import spark.partial.BoundedDouble
import spark.partial.CountEvaluator
import spark.partial.GroupedCountEvaluator
import spark.partial.PartialResult
import spark.storage.StorageLevel

import SparkContext._

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
 * partitioned collection of elements that can be operated on in parallel.
 *
 * Each RDD is characterized by five main properties:
 * - A list of splits (partitions)
 * - A function for computing each split
 * - A list of dependencies on other RDDs
 * - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *   HDFS)
 *
 * All the scheduling and execution in Spark is done based on these methods, allowing each RDD to 
 * implement its own way of computing itself.
 *
 * This class also contains transformation methods available on all RDDs (e.g. map and filter). In 
 * addition, PairRDDFunctions contains extra methods available on RDDs of key-value pairs, and 
 * SequenceFileRDDFunctions contains extra methods for saving RDDs to Hadoop SequenceFiles.
 */
abstract class RDD[T: ClassManifest](@transient sc: SparkContext) extends Serializable {

  // Methods that must be implemented by subclasses
  def splits: Array[Split]
  def compute(split: Split): Iterator[T]
  @transient val dependencies: List[Dependency[_]]
  
  // Record user function generating this RDD
  val origin = getOriginDescription
  
  // Optionally overridden by subclasses to specify how they are partitioned
  val partitioner: Option[Partitioner] = None

  // Optionally overridden by subclasses to specify placement preferences
  def preferredLocations(split: Split): Seq[String] = Nil
  
  def context = sc

  def elementClassManifest: ClassManifest[T] = classManifest[T]
  
  // Get a unique ID for this RDD
  val id = sc.newRddId()
  
  // Variables relating to persistence
  private var storageLevel: StorageLevel = StorageLevel.NONE
  
  // Change this RDD's storage level
  def persist(newLevel: StorageLevel): RDD[T] = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    storageLevel = newLevel
    this
  }

  // Turn on the default caching level for this RDD
  def persist(): RDD[T] = persist(StorageLevel.MEMORY_ONLY)
  
  // Turn on the default caching level for this RDD
  def cache(): RDD[T] = persist()

  def getStorageLevel = storageLevel
  
  def checkpoint(level: StorageLevel = StorageLevel.MEMORY_AND_DISK_2): RDD[T] = {
    if (!level.useDisk && level.replication < 2) {
      throw new Exception("Cannot checkpoint without using disk or replication (level requested was " + level + ")")
    } 
    
    // This is a hack. Ideally this should re-use the code used by the CacheTracker
    // to generate the key.
    def getSplitKey(split: Split) = "rdd:%d:%d".format(this.id, split.index)
    
    persist(level)
    sc.runJob(this, (iter: Iterator[T]) => {} )
    
    val p = this.partitioner
    
    new BlockRDD[T](sc, splits.map(getSplitKey).toArray) {
      override val partitioner = p 
    }
  }
  
  // Read this RDD; will read from cache if applicable, or otherwise compute
  final def iterator(split: Split): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheTracker.getOrCompute[T](this, split, storageLevel)
    } else {
      compute(split)
    }
  }
  
  // Describe which spark and user functions generated this RDD. Only works if called from
  // constructor.
  def getOriginDescription : String = {
    val trace = Thread.currentThread().getStackTrace().filter( el =>
        (!el.getMethodName().contains("getStackTrace")))

    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<not_found>"
    var firstUserMethod = "<not_found>"
    var firstUserFile = "<not_found>"
    var firstUserLine = -1
    var finished = false
    
    for (el <- trace) {
      if (!finished) {
        if (el.getClassName().contains("spark") && !el.getClassName().startsWith("spark.examples")) {
          lastSparkMethod = el.getMethodName()
        }
        else {
          firstUserMethod = el.getMethodName()
          firstUserLine = el.getLineNumber()
          firstUserFile = el.getFileName()
          finished = true
        }
      }
    }
    "%s at: %s (%s:%s)".format(lastSparkMethod, firstUserMethod, firstUserFile, firstUserLine)
  }
  
  // Transformations (return a new RDD)
  
  def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))
  
  def flatMap[U: ClassManifest](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))
  
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  def distinct(numSplits: Int = splits.size): RDD[T] =
    map(x => (x, "")).reduceByKey((x, y) => x, numSplits).map(_._1)

  def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)

  def takeSample(withReplacement: Boolean, num: Int, seed: Int): Array[T] = {
    var fraction = 0.0
    var total = 0
    var multiplier = 3.0
    var initialCount = count()
    var maxSelected = 0
    
    if (initialCount > Integer.MAX_VALUE - 1) {
      maxSelected = Integer.MAX_VALUE - 1
    } else {
      maxSelected = initialCount.toInt
    }
    
    if (num > initialCount) {
      total = maxSelected
      fraction = math.min(multiplier * (maxSelected + 1) / initialCount, 1.0)
    } else if (num < 0) {
      throw(new IllegalArgumentException("Negative number of elements requested"))
    } else {
      fraction = math.min(multiplier * (num + 1) / initialCount, 1.0)
      total = num
    }
  
    val rand = new Random(seed)
    var samples = this.sample(withReplacement, fraction, rand.nextInt).collect()
  
    while (samples.length < total) {
      samples = this.sample(withReplacement, fraction, rand.nextInt).collect()
    }
  
    Utils.randomizeInPlace(samples, rand).take(total)
  }

  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

  def ++(other: RDD[T]): RDD[T] = this.union(other)

  def glom(): RDD[Array[T]] = new GlommedRDD(this)

  def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(sc, this, other)

  def groupBy[K: ClassManifest](f: T => K, numSplits: Int): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(numSplits)
  }

  def groupBy[K: ClassManifest](f: T => K): RDD[(K, Seq[T])] = groupBy[K](f, sc.defaultParallelism)

  def pipe(command: String): RDD[String] = new PipedRDD(this, command)

  def pipe(command: Seq[String]): RDD[String] = new PipedRDD(this, command)

  def pipe(command: Seq[String], env: Map[String, String]): RDD[String] =
    new PipedRDD(this, command, env)

  def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))

  def mapPartitionsWithSplit[U: ClassManifest](f: (Int, Iterator[T]) => Iterator[U]): RDD[U] =
    new MapPartitionsWithSplitRDD(this, sc.clean(f))

  // Actions (launch a job to return a value to the user program)
  
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  def toArray(): Array[T] = collect()

  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      }else {
        None
      }
    }
    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    for (opt <- options; elem <- opt) {
      results += elem
    }
    if (results.size == 0) {
      throw new UnsupportedOperationException("empty collection")
    } else {
      return results.reduceLeft(cleanF)
    }
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function op(t1, t2) is allowed to 
   * modify t1 and return it as its result value to avoid object allocation; however, it should not
   * modify t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = {
    val cleanOp = sc.clean(op)
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp))
    return results.fold(zeroValue)(cleanOp)
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   */
  def aggregate[U: ClassManifest](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val results = sc.runJob(this,
        (iter: Iterator[T]) => iter.aggregate(zeroValue)(cleanSeqOp, cleanCombOp))
    return results.fold(zeroValue)(cleanCombOp)
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

  /**
   * Approximate version of count() that returns a potentially incomplete result after a timeout.
   */
  def countApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    val countElements: (TaskContext, Iterator[T]) => Long = { (ctx, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next
      }
      result
    }
    val evaluator = new CountEvaluator(splits.size, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   * Count elements equal to each value, returning a map of (value, count) pairs. The final combine
   * step happens locally on the master, equivalent to running a single reduce task.
   *
   * TODO: This should perhaps be distributed by default.
   */
  def countByValue(): Map[T, Long] = {
    def countPartition(iter: Iterator[T]): Iterator[OLMap[T]] = {
      val map = new OLMap[T]
      while (iter.hasNext) {
        val v = iter.next()
        map.put(v, map.getLong(v) + 1L)
      }
      Iterator(map)
    }
    def mergeMaps(m1: OLMap[T], m2: OLMap[T]): OLMap[T] = {
      val iter = m2.object2LongEntrySet.fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        m1.put(entry.getKey, m1.getLong(entry.getKey) + entry.getLongValue)
      }
      return m1
    }
    val myResult = mapPartitions(countPartition).reduce(mergeMaps)
    myResult.asInstanceOf[java.util.Map[T, Long]]   // Will be wrapped as a Scala mutable Map
  }

  /**
   * Approximate version of countByValue().
   */
  def countByValueApprox(
      timeout: Long,
      confidence: Double = 0.95
      ): PartialResult[Map[T, BoundedDouble]] = {
    val countPartition: (TaskContext, Iterator[T]) => OLMap[T] = { (ctx, iter) =>
      val map = new OLMap[T]
      while (iter.hasNext) {
        val v = iter.next()
        map.put(v, map.getLong(v) + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](splits.size, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }
  
  /**
   * Take the first num elements of the RDD. This currently scans the partitions *one by one*, so
   * it will be slow if a lot of partitions are required. In that case, use collect() to get the
   * whole RDD instead.
   */
  def take(num: Int): Array[T] = {
    if (num == 0) {
      return new Array[T](0)
    }
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
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  def saveAsObjectFile(path: String) {
    this.glom
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /** A private method for tests, to look at the contents of each partition */
  private[spark] def collectPartitions(): Array[Array[T]] = {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }
}

class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).map(f)
}

class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => TraversableOnce[U])
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).flatMap(f)
}

class FilteredRDD[T: ClassManifest](prev: RDD[T], f: T => Boolean) extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).filter(f)
}

class GlommedRDD[T: ClassManifest](prev: RDD[T]) extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = Array(prev.iterator(split).toArray).iterator
}

class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U])
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = f(prev.iterator(split))
}

/**
 * A variant of the MapPartitionsRDD that passes the split index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
class MapPartitionsWithSplitRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: (Int, Iterator[T]) => Iterator[U])
  extends RDD[U](prev.context) {

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = f(split.index, prev.iterator(split))
}
