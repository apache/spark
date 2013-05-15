package spark

import java.net.URL
import java.util.{Date, Random}
import java.util.{HashMap => JHashMap}

import scala.collection.Map
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}

import spark.Partitioner._
import spark.partial.BoundedDouble
import spark.partial.CountEvaluator
import spark.partial.GroupedCountEvaluator
import spark.partial.PartialResult
import spark.rdd.CoalescedRDD
import spark.rdd.CartesianRDD
import spark.rdd.FilteredRDD
import spark.rdd.FlatMappedRDD
import spark.rdd.GlommedRDD
import spark.rdd.MappedRDD
import spark.rdd.MapPartitionsRDD
import spark.rdd.MapPartitionsWithIndexRDD
import spark.rdd.PipedRDD
import spark.rdd.SampledRDD
import spark.rdd.ShuffledRDD
import spark.rdd.SubtractedRDD
import spark.rdd.UnionRDD
import spark.rdd.ZippedRDD
import spark.rdd.ZippedPartitionsRDD2
import spark.rdd.ZippedPartitionsRDD3
import spark.rdd.ZippedPartitionsRDD4
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
 *  - A list of partitions
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
abstract class RDD[T: ClassManifest](
    @transient private var sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))

  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  /** Implemented by subclasses to compute a given partition. */
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition]

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /** Optionally overridden by subclasses to specify placement preferences. */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD */
  var name: String = null

  /** Assign a name to this RDD */
  def setName(_name: String) = {
    name = _name
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
   */
  def persist(newLevel: StorageLevel): RDD[T] = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    storageLevel = newLevel
    // Register the RDD with the SparkContext
    sc.persistentRdds(id) = this
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def persist(): RDD[T] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): RDD[T] = persist()

  /** Mark the RDD as non-persistent, and remove all blocks for it from memory and disk. */
  def unpersist(): RDD[T] = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.env.blockManager.master.removeRdd(id)
    sc.persistentRdds.remove(id)
    storageLevel = StorageLevel.NONE
    this
  }

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel = storageLevel

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  private var dependencies_ : Seq[Dependency[_]] = null
  @transient private var partitions_ : Array[Partition] = null

  /** An Option holding our checkpoint RDD, if we are checkpointed */
  private def checkpointRDD: Option[RDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
   * Get the list of dependencies of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
      }
      partitions_
    }
  }

  /**
   * Get the preferred location of a split, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] = {
    if (isCheckpointed) {
      firstParent[T].iterator(split, context)
    } else {
      compute(split, context)
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
  def distinct(numPartitions: Int): RDD[T] =
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)

  def distinct(): RDD[T] = distinct(partitions.size)

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false): RDD[T] = {
    if (shuffle) {
      // include a shuffle step so that our upstream tasks are still distributed
      new CoalescedRDD(new ShuffledRDD(map(x => (x, null)), new HashPartitioner(numPartitions)), numPartitions).keys
    } else {
      new CoalescedRDD(this, numPartitions)
    }
  }

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
   * Return an RDD of grouped items.
   */
  def groupBy[K: ClassManifest](f: T => K): RDD[(K, Seq[T])] =
    groupBy[K](f, defaultPartitioner(this))

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key.
   */
  def groupBy[K: ClassManifest](f: T => K, numPartitions: Int): RDD[(K, Seq[T])] =
    groupBy(f, new HashPartitioner(numPartitions))

  /**
   * Return an RDD of grouped items.
   */
  def groupBy[K: ClassManifest](f: T => K, p: Partitioner): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

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
  def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f), preservesPartitioning)

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   */
  def mapPartitionsWithIndex[U: ClassManifest](
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] =
    new MapPartitionsWithIndexRDD(this, sc.clean(f), preservesPartitioning)

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   */
  @deprecated("use mapPartitionsWithIndex", "0.7.0")
  def mapPartitionsWithSplit[U: ClassManifest](
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] =
    new MapPartitionsWithIndexRDD(this, sc.clean(f), preservesPartitioning)

  /**
   * Maps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  def mapWith[A: ClassManifest, U: ClassManifest](constructA: Int => A, preservesPartitioning: Boolean = false)
    (f:(T, A) => U): RDD[U] = {
      def iterF(index: Int, iter: Iterator[T]): Iterator[U] = {
        val a = constructA(index)
        iter.map(t => f(t, a))
      }
    new MapPartitionsWithIndexRDD(this, sc.clean(iterF _), preservesPartitioning)
  }

  /**
   * FlatMaps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  def flatMapWith[A: ClassManifest, U: ClassManifest](constructA: Int => A, preservesPartitioning: Boolean = false)
    (f:(T, A) => Seq[U]): RDD[U] = {
      def iterF(index: Int, iter: Iterator[T]): Iterator[U] = {
        val a = constructA(index)
        iter.flatMap(t => f(t, a))
      }
    new MapPartitionsWithIndexRDD(this, sc.clean(iterF _), preservesPartitioning)
  }

  /**
   * Applies f to each element of this RDD, where f takes an additional parameter of type A.
   * This additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  def foreachWith[A: ClassManifest](constructA: Int => A)
    (f:(T, A) => Unit) {
      def iterF(index: Int, iter: Iterator[T]): Iterator[T] = {
        val a = constructA(index)
        iter.map(t => {f(t, a); t})
      }
    (new MapPartitionsWithIndexRDD(this, sc.clean(iterF _), true)).foreach(_ => {})
  }

  /**
   * Filters this RDD with p, where p takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  def filterWith[A: ClassManifest](constructA: Int => A)
    (p:(T, A) => Boolean): RDD[T] = {
      def iterF(index: Int, iter: Iterator[T]): Iterator[T] = {
        val a = constructA(index)
        iter.filter(t => p(t, a))
      }
    new MapPartitionsWithIndexRDD(this, sc.clean(iterF _), true)
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = new ZippedRDD(sc, this, other)

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[B: ClassManifest, V: ClassManifest](
      f: (Iterator[T], Iterator[B]) => Iterator[V],
      rdd2: RDD[B]): RDD[V] =
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2)

  def zipPartitions[B: ClassManifest, C: ClassManifest, V: ClassManifest](
      f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V],
      rdd2: RDD[B],
      rdd3: RDD[C]): RDD[V] =
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3)

  def zipPartitions[B: ClassManifest, C: ClassManifest, D: ClassManifest, V: ClassManifest](
      f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V],
      rdd2: RDD[B],
      rdd3: RDD[C],
      rdd4: RDD[D]): RDD[V] =
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4)


  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartition(f: Iterator[T] => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
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
   * Return an RDD that contains all matching values by applying `f`.
   */
  def collect[U: ClassManifest](f: PartialFunction[T, U]): RDD[U] = {
    filter(f.isDefinedAt).map(f)
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtract(other: RDD[T]): RDD[T] =
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.size)))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] =
    subtract(other, new HashPartitioner(numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], p: Partitioner): RDD[T] = {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      val p2 = new Partitioner() {
        override def numPartitions = p.numPartitions
        override def getPartition(k: Any) = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and associative binary operator.
   */
  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    val mergeResult = (index: Int, taskResult: Option[T]) => {
      if (taskResult != None) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function op(t1, t2) is allowed to
   * modify t1 and return it as its result value to avoid object allocation; however, it should not
   * modify t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp)
    val mergeResult = (index: Int, taskResult: T) => jobResult = op(jobResult, taskResult)
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
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
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = {
    sc.runJob(this, (iter: Iterator[T]) => {
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
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
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.size, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   * Return the count of each unique value in this RDD as a map of (value, count) pairs. The final
   * combine step happens locally on the master, equivalent to running a single reduce task.
   */
  def countByValue(): Map[T, Long] = {
    if (elementClassManifest.erasure.isArray) {
      throw new SparkException("countByValue() does not support arrays")
    }
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
    if (elementClassManifest.erasure.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OLMap[T] = { (ctx, iter) =>
      val map = new OLMap[T]
      while (iter.hasNext) {
        val v = iter.next()
        map.put(v, map.getLong(v) + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.size, confidence)
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
    while (buf.size < num && p < partitions.size) {
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

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = {
    map(x => (f(x), x))
  }

  /** A private method for tests, to look at the contents of each partition */
  private[spark] def collectPartitions(): Array[Array[T]] = {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with SparkContext.setCheckpointDir() and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def checkpoint() {
    if (context.checkpointDir.isEmpty) {
      throw new Exception("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new RDDCheckpointData(this))
      checkpointData.get.markForCheckpoint()
    }
  }

  /**
   * Return whether this RDD has been checkpointed or not
   */
  def isCheckpointed: Boolean = {
    checkpointData.map(_.isCheckpointed).getOrElse(false)
  }

  /**
   * Gets the name of the file to which this RDD was checkpointed
   */
  def getCheckpointFile: Option[String] = {
    checkpointData.flatMap(_.getCheckpointFile)
  }

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /** Record user function generating this RDD. */
  private[spark] val origin = Utils.getSparkCallSite

  private[spark] def elementClassManifest: ClassManifest[T] = classManifest[T]

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  /** Returns the first parent RDD */
  protected[spark] def firstParent[U: ClassManifest] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** The [[spark.SparkContext]] that this RDD was created on. */
  def context = sc

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  private var doCheckpointCalled = false

  /**
   * Performs the checkpointing of this RDD by saving this. It is called by the DAGScheduler
   * after a job using this RDD has completed (therefore the RDD has been materialized and
   * potentially stored in memory). doCheckpoint() is called recursively on the parent RDDs.
   */
  private[spark] def doCheckpoint() {
    if (!doCheckpointCalled) {
      doCheckpointCalled = true
      if (checkpointData.isDefined) {
        checkpointData.get.doCheckpoint()
      } else {
        dependencies.foreach(_.rdd.doCheckpoint())
      }
    }
  }

  /**
   * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
   * created from the checkpoint file, and forget its old dependencies and partitions.
   */
  private[spark] def markCheckpointed(checkpointRDD: RDD[_]) {
    clearDependencies()
    partitions_ = null
    deps = null    // Forget the constructor argument for dependencies too
  }

  /**
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs is removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[spark.rdd.UnionRDD]] for an example.
   */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = {
    def debugString(rdd: RDD[_], prefix: String = ""): Seq[String] = {
      Seq(prefix + rdd + " (" + rdd.partitions.size + " partitions)") ++
        rdd.dependencies.flatMap(d => debugString(d.rdd, prefix + "  "))
    }
    debugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""),
    getClass.getSimpleName,
    id,
    origin)

}
