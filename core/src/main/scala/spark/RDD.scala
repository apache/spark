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
import spark.rdd.BlockRDD
import spark.rdd.CartesianRDD
import spark.rdd.FilteredRDD
import spark.rdd.FlatMappedRDD
import spark.rdd.GlommedRDD
import spark.rdd.MappedRDD
import spark.rdd.MapPartitionsRDD
import spark.rdd.MapPartitionsWithSplitRDD
import spark.rdd.PipedRDD
import spark.rdd.SampledRDD
import spark.rdd.UnionRDD
import spark.storage.StorageLevel

import SparkContext._

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[spark.PairRDDFunctions]] contains operations available only on RDDs of key-value pairs, such
 * as `groupByKey` and `join`; [[spark.DoubleRDDFunctions]] contains operations available only on
 * RDDs of Doubles; and [[spark.SequenceFileRDDFunctions]] contains operations available on RDDs
 * that can be saved as SequenceFiles. These operations are automatically available on any RDD of
 * the right type (e.g. RDD[(Int, Int)] through implicit conversions when you
 * `import spark.SparkContext._`.
 *
 * Internally, each RDD is characterized by five main properties:
 *
 *  - A list of splits (partitions)
 *  - A function for computing each split
 *  - A list of dependencies on other RDDs
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * [[http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf Spark paper]] for more details
 * on RDD internals.
 */
abstract class RDD[T: ClassManifest](@transient sc: SparkContext) extends Serializable {

  // Methods that must be implemented by subclasses:

  /** Set of partitions in this RDD. */
  def splits: Array[Split]

  /** Function for computing a given partition. */
  def compute(split: Split): Iterator[T]

  /** How this RDD depends on any parent RDDs. */
  @transient val dependencies: List[Dependency[_]]

  // Methods available on all RDDs:
  
  /** Record user function generating this RDD. */
  private[spark] val origin = Utils.getSparkCallSite
  
  /** Optionally overridden by subclasses to specify how they are partitioned. */
  val partitioner: Option[Partitioner] = None

  /** Optionally overridden by subclasses to specify placement preferences. */
  def preferredLocations(split: Split): Seq[String] = Nil
  
  /** The [[spark.SparkContext]] that this RDD was created on. */
  def context = sc

  private[spark] def elementClassManifest: ClassManifest[T] = classManifest[T]
  
  /** A unique ID for this RDD (within its SparkContext). */
  val id = sc.newRddId()
  
  // Variables relating to persistence
  private var storageLevel: StorageLevel = StorageLevel.NONE
  
  /** 
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): RDD[T] = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    storageLevel = newLevel
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def persist(): RDD[T] = persist(StorageLevel.MEMORY_ONLY)
  
  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): RDD[T] = persist()

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel = storageLevel
  
  private[spark] def checkpoint(level: StorageLevel = StorageLevel.MEMORY_AND_DISK_2): RDD[T] = {
    if (!level.useDisk && level.replication < 2) {
      throw new Exception("Cannot checkpoint without using disk or replication (level requested was " + level + ")")
    } 
    
    // This is a hack. Ideally this should re-use the code used by the CacheTracker
    // to generate the key.
    def getSplitKey(split: Split) = "rdd_%d_%d".format(this.id, split.index)
    
    persist(level)
    sc.runJob(this, (iter: Iterator[T]) => {} )
    
    val p = this.partitioner
    
    new BlockRDD[T](sc, splits.map(getSplitKey).toArray) {
      override val partitioner = p 
    }
  }
  
  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
   */
  final def iterator(split: Split): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheTracker.getOrCompute[T](this, split, storageLevel)
    } else {
      compute(split)
    }
  }
  
  // Transformations (return a new RDD)
  
  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMap[U: ClassManifest](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numSplits: Int = splits.size): RDD[T] =
    map(x => (x, null)).reduceByKey((x, y) => x, numSplits).map(_._1)

  /**
   * Return a sampled subset of this RDD.
   */
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

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def ++(other: RDD[T]): RDD[T] = this.union(other)

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   */
  def glom(): RDD[Array[T]] = new GlommedRDD(this)

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   */
  def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(sc, this, other)

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key.
   */
  def groupBy[K: ClassManifest](f: T => K, numSplits: Int): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(numSplits)
  }

  /**
   * Return an RDD of grouped items.
   */
  def groupBy[K: ClassManifest](f: T => K): RDD[(K, Seq[T])] = groupBy[K](f, sc.defaultParallelism)

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String): RDD[String] = new PipedRDD(this, command)

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: Seq[String]): RDD[String] = new PipedRDD(this, command)

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: Seq[String], env: Map[String, String]): RDD[String] =
    new PipedRDD(this, command, env)

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))

   /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   */
  def mapPartitionsWithSplit[U: ClassManifest](f: (Int, Iterator[T]) => Iterator[U]): RDD[U] =
    new MapPartitionsWithSplitRDD(this, sc.clean(f))

  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  def toArray(): Array[T] = collect()

  /**
   * Reduces the elements of this RDD using the specified associative binary operator.
   */
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

  /**
   * Return the number of elements in the RDD.
   */
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
   * (Experimental) Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
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
   * Return the count of each unique value in this RDD as a map of (value, count) pairs. The final
   * combine step happens locally on the master, equivalent to running a single reduce task.
   */
  def countByValue(): Map[T, Long] = {
    // TODO: This should perhaps be distributed by default.
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
   * (Experimental) Approximate version of countByValue().
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

  /**
   * Return the first element in this RDD.
   */
  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
   */
  def saveAsObjectFile(path: String) {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /** A private method for tests, to look at the contents of each partition */
  private[spark] def collectPartitions(): Array[Array[T]] = {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }
}
