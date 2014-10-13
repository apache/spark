/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.util.{Properties, Random}

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils, CallSite}
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{BernoulliSampler, PoissonSampler, SamplingUtils}

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; and
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * These operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)]
 * through implicit conversions when you `import org.apache.spark.SparkContext._`.
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
abstract class RDD[T: ClassTag](
    @transient private var sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
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

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================

  /** The SparkContext that created this RDD. */
  def sparkContext: SparkContext = sc

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD */
  @transient var name: String = null

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
   */
  def persist(newLevel: StorageLevel): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    sc.persistRDD(this)
    // Register the RDD with the ContextCleaner for automatic GC-based cleanup
    sc.cleaner.foreach(_.registerRDDForCleanup(this))
    storageLevel = newLevel
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): this.type = persist()

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking)
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
   * Get the preferred locations of a partition, taking into account whether the
   * RDD is checkpointed.
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
   * Return the ancestors of the given RDD that are related to it only through a sequence of
   * narrow dependencies. This traverses the given RDD's dependency tree using DFS, but maintains
   * no ordering on the RDDs returned.
   */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]

    def visit(rdd: RDD[_]) {
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
      val narrowParents = narrowDependencies.map(_.rdd)
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)
        visit(parent)
      }
    }

    visit(this)

    // In case there is a cycle, do not include the root itself
    ancestors.filterNot(_ == this).toSeq
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointed) firstParent[T].iterator(split, context) else compute(split, context)
  }

  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] =
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): RDD[T] = distinct(partitions.size)

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = {
    coalesce(numPartitions, shuffle = true)
  }

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * Note: With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
      : RDD[T] = {
    if (shuffle) {
      /** Distributes elements evenly across output partitions, starting from a random partition. */
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = (new Random(index)).nextInt(numPartitions)
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)]

      // include a shuffle step so that our upstream tasks are still distributed
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition),
        new HashPartitioner(numPartitions)),
        numPartitions).values
    } else {
      new CoalescedRDD(this, numPartitions)
    }
  }

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T] = {
    require(fraction >= 0.0, "Negative fraction value: " + fraction)
    if (withReplacement) {
      new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
    } else {
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
    }
  }

  /**
   * Randomly splits this RDD with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed random seed
   *
   * @return split RDDs in an array
   */
  def randomSplit(weights: Array[Double], seed: Long = Utils.random.nextLong): Array[RDD[T]] = {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](x(0), x(1)), true, seed)
    }.toArray
  }

  /**
   * Return a fixed-size sampled subset of this RDD in an array
   *
   * @param withReplacement whether sampling is done with replacement
   * @param num size of the returned sample
   * @param seed seed for the random number generator
   * @return sample of specified size in an array
   */
  def takeSample(withReplacement: Boolean,
      num: Int,
      seed: Long = Utils.random.nextLong): Array[T] = {
    val numStDev =  10.0

    if (num < 0) {
      throw new IllegalArgumentException("Negative number of elements requested")
    } else if (num == 0) {
      return new Array[T](0)
    }

    val initialCount = this.count()
    if (initialCount == 0) {
      return new Array[T](0)
    }

    val maxSampleSize = Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt
    if (num > maxSampleSize) {
      throw new IllegalArgumentException("Cannot support a sample size > Int.MaxValue - " +
        s"$numStDev * math.sqrt(Int.MaxValue)")
    }

    val rand = new Random(seed)
    if (!withReplacement && num >= initialCount) {
      return Utils.randomizeInPlace(this.collect(), rand)
    }

    val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
      withReplacement)

    var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

    // If the first sample didn't turn out large enough, keep trying to take samples;
    // this shouldn't happen often because we use a big multiplier for the initial size
    var numIters = 0
    while (samples.length < num) {
      logWarning(s"Needed to re-sample due to insufficient sample size. Repeat #$numIters")
      samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
      numIters += 1
    }

    Utils.randomizeInPlace(samples, rand).take(num)
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
   * Return this RDD sorted by the given key function.
   */
  def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.size)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] =
    this.keyBy[K](f)
        .sortByKey(ascending, numPartitions)
        .values

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   */
  def intersection(other: RDD[T]): RDD[T] = {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   *
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null)
      : RDD[T] = {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *
   * Note that this method performs a shuffle internally.
   *
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), new HashPartitioner(numPartitions))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   */
  def glom(): RDD[Array[T]] = new GlommedRDD(this)

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(sc, this, other)

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] =
    groupBy[K](f, defaultPartitioner(this))

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] =
    groupBy(f, new HashPartitioner(numPartitions))

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
      : RDD[(K, Iterable[T])] = {
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
  def pipe(command: String, env: Map[String, String]): RDD[String] =
    new PipedRDD(this, command, env)

  /**
   * Return an RDD created by piping elements to a forked external process.
   * The print behavior can be customized by providing two functions.
   *
   * @param command command to run in forked process.
   * @param env environment variables to set.
   * @param printPipeContext Before piping elements, this function is called as an oppotunity
   *                         to pipe context data. Print line function (like out.println) will be
   *                         passed as printPipeContext's parameter.
   * @param printRDDElement Use this function to customize how to pipe elements. This function
   *                        will be called with each RDD element as the 1st parameter, and the
   *                        print line function (like out.println()) as the 2nd parameter.
   *                        An example of pipe the RDD data of groupBy() in a streaming way,
   *                        instead of constructing a huge String to concat all the elements:
   *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
   *                          for (e <- record._2){f(e)}
   * @param separateWorkingDir Use separate working directories for each task.
   * @return the result RDD
   */
  def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false): RDD[String] = {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(index, iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * :: DeveloperApi ::
   * Return a new RDD by applying a function to each partition of this RDD. This is a variant of
   * mapPartitions that also passes the TaskContext into the closure.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  @DeveloperApi
  @deprecated("use TaskContext.get", "1.2.0")
  def mapPartitionsWithContext[U: ClassTag](
      f: (TaskContext, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(context, iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   */
  @deprecated("use mapPartitionsWithIndex", "0.7.0")
  def mapPartitionsWithSplit[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    mapPartitionsWithIndex(f, preservesPartitioning)
  }

  /**
   * Maps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex", "1.0.0")
  def mapWith[A, U: ClassTag]
      (constructA: Int => A, preservesPartitioning: Boolean = false)
      (f: (T, A) => U): RDD[U] = {
    mapPartitionsWithIndex((index, iter) => {
      val a = constructA(index)
      iter.map(t => f(t, a))
    }, preservesPartitioning)
  }

  /**
   * FlatMaps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex and flatMap", "1.0.0")
  def flatMapWith[A, U: ClassTag]
      (constructA: Int => A, preservesPartitioning: Boolean = false)
      (f: (T, A) => Seq[U]): RDD[U] = {
    mapPartitionsWithIndex((index, iter) => {
      val a = constructA(index)
      iter.flatMap(t => f(t, a))
    }, preservesPartitioning)
  }

  /**
   * Applies f to each element of this RDD, where f takes an additional parameter of type A.
   * This additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex and foreach", "1.0.0")
  def foreachWith[A](constructA: Int => A)(f: (T, A) => Unit) {
    mapPartitionsWithIndex { (index, iter) =>
      val a = constructA(index)
      iter.map(t => {f(t, a); t})
    }.foreach(_ => {})
  }

  /**
   * Filters this RDD with p, where p takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   */
  @deprecated("use mapPartitionsWithIndex and filter", "1.0.0")
  def filterWith[A](constructA: Int => A)(p: (T, A) => Boolean): RDD[T] = {
    mapPartitionsWithIndex((index, iter) => {
      val a = constructA(index)
      iter.filter(t => p(t, a))
    }, preservesPartitioning = true)
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[(T, U)] {
        def hasNext = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next = (thisIter.next, otherIter.next)
      }
    }
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)

  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, false)

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, false)

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, false)


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
   * Return an iterator that contains all of the elements in this RDD.
   *
   * The iterator will consume as much memory as the largest partition in this RDD.
   */
  def toLocalIterator: Iterator[T] = {
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p), allowLocal = false).head
    }
    (0 until partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  @deprecated("use collect", "1.0.0")
  def toArray(): Array[T] = collect()

  /**
   * Return an RDD that contains all matching values by applying `f`.
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = {
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
  def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = {
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
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
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
      if (taskResult.isDefined) {
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
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
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
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
   * :: Experimental ::
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   */
  @Experimental
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
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValue() does not support arrays")
    }
    // TODO: This should perhaps be distributed by default.
    val countPartition = (iter: Iterator[T]) => {
      val map = new OpenHashMap[T,Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      Iterator(map)
    }: Iterator[OpenHashMap[T,Long]]
    val mergeMaps = (m1: OpenHashMap[T,Long], m2: OpenHashMap[T,Long]) => {
      m2.foreach { case (key, value) =>
        m1.changeValue(key, value, _ + value)
      }
      m1
    }: OpenHashMap[T,Long]
    val myResult = mapPartitions(countPartition).reduce(mergeMaps)
    // Convert to a Scala mutable map
    val mutableResult = scala.collection.mutable.Map[T,Long]()
    myResult.foreach { case (k, v) => mutableResult.put(k, v) }
    mutableResult
  }

  /**
   * :: Experimental ::
   * Approximate version of countByValue().
   */
  @Experimental
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
      (implicit ord: Ordering[T] = null)
      : PartialResult[Map[T, BoundedDouble]] =
  {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T,Long] = { (ctx, iter) =>
      val map = new OpenHashMap[T,Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.size, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
   * :: Experimental ::
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp > p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  @Experimental
  def countApproxDistinct(p: Int, sp: Int): Long = {
    require(p >= 4, s"p ($p) must be at least 4")
    require(sp <= 32, s"sp ($sp) cannot be greater than 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = {
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(p, 0)
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   *
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The index assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithIndex(): RDD[(T, Long)] = new ZippedWithIndexRDD(this)

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithUniqueId(): RDD[(T, Long)] = {
    val n = this.partitions.size.toLong
    this.mapPartitionsWithIndex { case (k, iter) =>
      iter.zipWithIndex.map { case (item, i) =>
        (item, i * n + k)
      }
    }
  }

  /**
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
   */
  def take(num: Int): Array[T] = {
    if (num == 0) {
      return new Array[T](0)
    }

    val buf = new ArrayBuffer[T]
    val totalParts = this.partitions.length
    var partsScanned = 0
    while (buf.size < num && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry. Otherwise,
        // interpolate the number of partitions we need to try, but overestimate it by 50%.
        // We also cap the estimation in the end.
        if (buf.size == 0) {
          numPartsToTry = partsScanned * 4
        } else {
          // the left side of max is >=1 whenever partsScanned >= 2
          numPartsToTry = Math.max((1.5 * num * partsScanned / buf.size).toInt - partsScanned, 1)
          numPartsToTry = Math.min(numPartsToTry, partsScanned * 4) 
        }
      }

      val left = num - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p, allowLocal = true)

      res.foreach(buf ++= _.take(num - buf.size))
      partsScanned += numPartsToTry
    }

    buf.toArray
  }

  /**
   * Return the first element in this RDD.
   */
  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  /**
   * Returns the top K (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T]. This does the opposite of [[takeOrdered]]. For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * @param num the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = takeOrdered(num)(ord.reverse)

  /**
   * Returns the first K (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * @param num the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = {
    if (num == 0) {
      Array.empty
    } else {
      mapPartitions { items =>
        // Priority keeps the largest elements, so let's reverse the ordering.
        val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
        queue ++= util.collection.Utils.takeOrdered(items, num)(ord)
        Iterator.single(queue)
      }.reduce { (queue1, queue2) =>
        queue1 ++= queue2
        queue1
      }.toArray.sorted(ord)
    }
  }

  /**
   * Returns the max of this RDD as defined by the implicit Ordering[T].
   * @return the maximum element of the RDD
   * */
  def max()(implicit ord: Ordering[T]): T = this.reduce(ord.max)

  /**
   * Returns the min of this RDD as defined by the implicit Ordering[T].
   * @return the minimum element of the RDD
   * */
  def min()(implicit ord: Ordering[T]): T = this.reduce(ord.min)

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
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
  def isCheckpointed: Boolean = checkpointData.exists(_.isCheckpointed)

  /**
   * Gets the name of the file to which this RDD was checkpointed
   */
  def getCheckpointFile: Option[String] = checkpointData.flatMap(_.getCheckpointFile)

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /** User code that created this RDD (e.g. `textFile`, `parallelize`). */
  @transient private[spark] val creationSite = sc.getCallSite()

  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  /** Returns the first parent RDD */
  protected[spark] def firstParent[U: ClassTag] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] */
  protected[spark] def parent[U: ClassTag](j: Int) = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context = sc

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java <-> Scala API compatibility.
   */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java <-> Scala API compatibility.
   */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  @transient private var doCheckpointCalled = false

  /**
   * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
   * has completed (therefore the RDD has been materialized and potentially stored in memory).
   * doCheckpoint() is called recursively on the parent RDDs.
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
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    def debugSelf (rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = storageLevel.description
      val storageInfo = rdd.context.getRDDStorageInfo.filter(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; TachyonSize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          bytesToString(info.tachyonSize), bytesToString(info.diskSize)))

      s"$rdd [$persistence]" +: storageInfo
    }

    // Apply a different rule to the last child
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_,_,_]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_,_,_]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_,_,_]], true)

          (frontDepStrings ++ lastDepStrings)
      }
    }
    // The first RDD in the dependency stack has no parents, so no need for a +-
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.size + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix $desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.size + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
        + (if (isLastChild) "  " else "| ")
        + (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset)))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$thisPrefix+-$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix$desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(rdd: RDD[_],
                    prefix: String = "",
                    isShuffle: Boolean = true,
                    isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      } else {
        debugSelf(rdd).map(prefix + _) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}
