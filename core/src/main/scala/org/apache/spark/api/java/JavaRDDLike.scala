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

package org.apache.spark.api.java

import java.{lang => jl}
import java.lang.{Iterable => JIterable}
import java.util.{Comparator, Iterator => JIterator, List => JList, Map => JMap}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark._
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.JavaUtils.mapAsSerializableJavaMap
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2, _}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * As a workaround for https://issues.scala-lang.org/browse/SI-8905, implementations
 * of JavaRDDLike should extend this dummy abstract class instead of directly inheriting
 * from the trait. See SPARK-3266 for additional details.
 */
private[spark] abstract class AbstractJavaRDDLike[T, This <: JavaRDDLike[T, This]]
  extends JavaRDDLike[T, This]

/**
 * Defines operations common to several Java RDD implementations.
 *
 * @note This trait is not intended to be implemented by user code.
 */
trait JavaRDDLike[T, This <: JavaRDDLike[T, This]] extends Serializable {
  def wrapRDD(rdd: RDD[T]): This

  implicit val classTag: ClassTag[T]

  def rdd: RDD[T]

  /** Set of partitions in this RDD. */
  def partitions: JList[Partition] = rdd.partitions.toImmutableArraySeq.asJava

  /** Return the number of partitions in this RDD. */
  @Since("1.6.0")
  def getNumPartitions: Int = rdd.getNumPartitions

  /** The partitioner of this RDD. */
  def partitioner: Optional[Partitioner] = JavaUtils.optionToOptional(rdd.partitioner)

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context: SparkContext = rdd.context

  /** A unique ID for this RDD (within its SparkContext). */
  def id: Int = rdd.id

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel: StorageLevel = rdd.getStorageLevel

  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementers of custom
   * subclasses of RDD.
   */
  def iterator(split: Partition, taskContext: TaskContext): JIterator[T] =
    rdd.iterator(split, taskContext).asJava

  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def map[R](f: JFunction[T, R]): JavaRDD[R] =
    new JavaRDD(rdd.map(f)(fakeClassTag))(fakeClassTag)

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   */
  def mapPartitionsWithIndex[R](
      f: JFunction2[jl.Integer, JIterator[T], JIterator[R]],
      preservesPartitioning: Boolean = false): JavaRDD[R] =
    new JavaRDD(rdd.mapPartitionsWithIndex((a, b) => f.call(a, b.asJava).asScala,
        preservesPartitioning)(fakeClassTag))(fakeClassTag)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def mapToDouble[R](f: DoubleFunction[T]): JavaDoubleRDD = {
    new JavaDoubleRDD(rdd.map(f.call(_).doubleValue()))
  }

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def mapToPair[K2, V2](f: PairFunction[T, K2, V2]): JavaPairRDD[K2, V2] = {
    def cm: ClassTag[(K2, V2)] = implicitly[ClassTag[(K2, V2)]]
    new JavaPairRDD(rdd.map[(K2, V2)](f)(cm))(fakeClassTag[K2], fakeClassTag[V2])
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMap[U](f: FlatMapFunction[T, U]): JavaRDD[U] = {
    def fn: (T) => Iterator[U] = (x: T) => f.call(x).asScala
    JavaRDD.fromRDD(rdd.flatMap(fn)(fakeClassTag[U]))(fakeClassTag[U])
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMapToDouble(f: DoubleFlatMapFunction[T]): JavaDoubleRDD = {
    def fn: (T) => Iterator[jl.Double] = (x: T) => f.call(x).asScala
    new JavaDoubleRDD(rdd.flatMap(fn).map(_.doubleValue()))
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMapToPair[K2, V2](f: PairFlatMapFunction[T, K2, V2]): JavaPairRDD[K2, V2] = {
    def fn: (T) => Iterator[(K2, V2)] = (x: T) => f.call(x).asScala
    def cm: ClassTag[(K2, V2)] = implicitly[ClassTag[(K2, V2)]]
    JavaPairRDD.fromRDD(rdd.flatMap(fn)(cm))(fakeClassTag[K2], fakeClassTag[V2])
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitions[U](f: FlatMapFunction[JIterator[T], U]): JavaRDD[U] = {
    def fn: (Iterator[T]) => Iterator[U] = {
      (x: Iterator[T]) => f.call(x.asJava).asScala
    }
    JavaRDD.fromRDD(rdd.mapPartitions(fn)(fakeClassTag[U]))(fakeClassTag[U])
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitions[U](f: FlatMapFunction[JIterator[T], U],
      preservesPartitioning: Boolean): JavaRDD[U] = {
    def fn: (Iterator[T]) => Iterator[U] = {
      (x: Iterator[T]) => f.call(x.asJava).asScala
    }
    JavaRDD.fromRDD(
      rdd.mapPartitions(fn, preservesPartitioning)(fakeClassTag[U]))(fakeClassTag[U])
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitionsToDouble(f: DoubleFlatMapFunction[JIterator[T]]): JavaDoubleRDD = {
    def fn: (Iterator[T]) => Iterator[jl.Double] = {
      (x: Iterator[T]) => f.call(x.asJava).asScala
    }
    new JavaDoubleRDD(rdd.mapPartitions(fn).map(_.doubleValue()))
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitionsToPair[K2, V2](f: PairFlatMapFunction[JIterator[T], K2, V2]):
  JavaPairRDD[K2, V2] = {
    def fn: (Iterator[T]) => Iterator[(K2, V2)] = {
      (x: Iterator[T]) => f.call(x.asJava).asScala
    }
    JavaPairRDD.fromRDD(rdd.mapPartitions(fn))(fakeClassTag[K2], fakeClassTag[V2])
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitionsToDouble(f: DoubleFlatMapFunction[JIterator[T]],
      preservesPartitioning: Boolean): JavaDoubleRDD = {
    def fn: (Iterator[T]) => Iterator[jl.Double] = {
      (x: Iterator[T]) => f.call(x.asJava).asScala
    }
    new JavaDoubleRDD(rdd.mapPartitions(fn, preservesPartitioning)
      .map(_.doubleValue()))
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   */
  def mapPartitionsToPair[K2, V2](f: PairFlatMapFunction[JIterator[T], K2, V2],
      preservesPartitioning: Boolean): JavaPairRDD[K2, V2] = {
    def fn: (Iterator[T]) => Iterator[(K2, V2)] = {
      (x: Iterator[T]) => f.call(x.asJava).asScala
    }
    JavaPairRDD.fromRDD(
      rdd.mapPartitions(fn, preservesPartitioning))(fakeClassTag[K2], fakeClassTag[V2])
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartition(f: VoidFunction[JIterator[T]]): Unit = {
    rdd.foreachPartition(x => f.call(x.asJava))
  }

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   */
  def glom(): JavaRDD[JList[T]] =
    new JavaRDD(rdd.glom().map(_.toImmutableArraySeq.asJava))

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   */
  def cartesian[U](other: JavaRDDLike[U, _]): JavaPairRDD[T, U] =
    JavaPairRDD.fromRDD(rdd.cartesian(other.rdd)(other.classTag))(classTag, other.classTag)

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key.
   */
  def groupBy[U](f: JFunction[T, U]): JavaPairRDD[U, JIterable[T]] = {
    // The type parameter is U instead of K in order to work around a compiler bug; see SPARK-4459
    implicit val ctagK: ClassTag[U] = fakeClassTag
    implicit val ctagV: ClassTag[JList[T]] = fakeClassTag
    JavaPairRDD.fromRDD(groupByResultToJava(rdd.groupBy(f)(fakeClassTag)))
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key.
   */
  def groupBy[U](f: JFunction[T, U], numPartitions: Int): JavaPairRDD[U, JIterable[T]] = {
    // The type parameter is U instead of K in order to work around a compiler bug; see SPARK-4459
    implicit val ctagK: ClassTag[U] = fakeClassTag
    implicit val ctagV: ClassTag[JList[T]] = fakeClassTag
    JavaPairRDD.fromRDD(groupByResultToJava(rdd.groupBy(f, numPartitions)(fakeClassTag[U])))
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String): JavaRDD[String] = {
    rdd.pipe(command)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: JList[String]): JavaRDD[String] = {
    rdd.pipe(command.asScala.toSeq)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: JList[String], env: JMap[String, String]): JavaRDD[String] = {
    rdd.pipe(command.asScala.toSeq, env.asScala)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: JList[String],
           env: JMap[String, String],
           separateWorkingDir: Boolean,
           bufferSize: Int): JavaRDD[String] = {
    rdd.pipe(command.asScala.toSeq, env.asScala, null, null, separateWorkingDir, bufferSize)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: JList[String],
           env: JMap[String, String],
           separateWorkingDir: Boolean,
           bufferSize: Int,
           encoding: String): JavaRDD[String] = {
    rdd.pipe(command.asScala.toSeq, env.asScala, null, null, separateWorkingDir, bufferSize,
      encoding)
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip[U](other: JavaRDDLike[U, _]): JavaPairRDD[T, U] = {
    JavaPairRDD.fromRDD(rdd.zip(other.rdd)(other.classTag))(classTag, other.classTag)
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[U, V](
      other: JavaRDDLike[U, _],
      f: FlatMapFunction2[JIterator[T], JIterator[U], V]): JavaRDD[V] = {
    def fn: (Iterator[T], Iterator[U]) => Iterator[V] = {
      (x: Iterator[T], y: Iterator[U]) => f.call(x.asJava, y.asJava).asScala
    }
    JavaRDD.fromRDD(
      rdd.zipPartitions(other.rdd)(fn)(other.classTag, fakeClassTag[V]))(fakeClassTag[V])
  }

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
   */
  def zipWithUniqueId(): JavaPairRDD[T, jl.Long] = {
    JavaPairRDD.fromRDD(rdd.zipWithUniqueId()).asInstanceOf[JavaPairRDD[T, jl.Long]]
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
   */
  def zipWithIndex(): JavaPairRDD[T, jl.Long] = {
    JavaPairRDD.fromRDD(rdd.zipWithIndex()).asInstanceOf[JavaPairRDD[T, jl.Long]]
  }

  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: VoidFunction[T]): Unit = {
    rdd.foreach(x => f.call(x))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def collect(): JList[T] =
    rdd.collect().toImmutableArraySeq.asJava

  /**
   * Return an iterator that contains all of the elements in this RDD.
   *
   * The iterator will consume as much memory as the largest partition in this RDD.
   */
  def toLocalIterator(): JIterator[T] =
     rdd.toLocalIterator.asJava

  /**
   * Return an array that contains all of the elements in a specific partition of this RDD.
   */
  def collectPartitions(partitionIds: Array[Int]): Array[JList[T]] = {
    // This is useful for implementing `take` from other language frontends
    // like Python where the data is serialized.
    val res = context.runJob(rdd, (it: Iterator[T]) => it.toArray, partitionIds.toImmutableArraySeq)
    res.map(_.toImmutableArraySeq.asJava)
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and associative binary
   * operator.
   */
  def reduce(f: JFunction2[T, T, T]): T = rdd.reduce(f)

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree
   * @see [[org.apache.spark.api.java.JavaRDDLike#reduce]]
   */
  def treeReduce(f: JFunction2[T, T, T], depth: Int): T = rdd.treeReduce(f, depth)

  /**
   * `org.apache.spark.api.java.JavaRDDLike.treeReduce` with suggested depth 2.
   */
  def treeReduce(f: JFunction2[T, T, T]): T = treeReduce(f, 2)

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function
   * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * This behaves somewhat differently from fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then fold those results into the final result, rather than
   * apply the fold to each element sequentially in some defined ordering. For functions
   * that are not commutative, the result may differ from that of a fold applied to a
   * non-distributed collection.
   */
  def fold(zeroValue: T)(f: JFunction2[T, T, T]): T =
    rdd.fold(zeroValue)(f)

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.IterableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   */
  def aggregate[U](zeroValue: U)(seqOp: JFunction2[U, T, U],
    combOp: JFunction2[U, U, U]): U =
    rdd.aggregate(zeroValue)(seqOp, combOp)(fakeClassTag[U])

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree
   * @see [[org.apache.spark.api.java.JavaRDDLike#aggregate]]
   */
  def treeAggregate[U](
      zeroValue: U,
      seqOp: JFunction2[U, T, U],
      combOp: JFunction2[U, U, U],
      depth: Int): U = {
    rdd.treeAggregate(zeroValue)(seqOp, combOp, depth)(fakeClassTag[U])
  }

  /**
   * `org.apache.spark.api.java.JavaRDDLike.treeAggregate` with suggested depth 2.
   */
  def treeAggregate[U](
      zeroValue: U,
      seqOp: JFunction2[U, T, U],
      combOp: JFunction2[U, U, U]): U = {
    treeAggregate(zeroValue, seqOp, combOp, 2)
  }

  /**
   * `org.apache.spark.api.java.JavaRDDLike.treeAggregate` with a parameter to do the
   * final aggregation on the executor.
   */
  def treeAggregate[U](
      zeroValue: U,
      seqOp: JFunction2[U, T, U],
      combOp: JFunction2[U, U, U],
      depth: Int,
      finalAggregateOnExecutor: Boolean): U = {
    rdd.treeAggregate(zeroValue, seqOp, combOp, depth, finalAggregateOnExecutor)(fakeClassTag[U])
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = rdd.count()

  /**
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   *
   * The confidence is the probability that the error bounds of the result will
   * contain the true value. That is, if countApprox were called repeatedly
   * with confidence 0.9, we would expect 90% of the results to contain the
   * true count. The confidence must be in the range [0,1] or an exception will
   * be thrown.
   *
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param confidence the desired statistical confidence in the result
   * @return a potentially incomplete result, with error bounds
   */
  def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    rdd.countApprox(timeout, confidence)

  /**
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   *
   * @param timeout maximum time to wait for the job, in milliseconds
   */
  def countApprox(timeout: Long): PartialResult[BoundedDouble] =
    rdd.countApprox(timeout)

  /**
   * Return the count of each unique value in this RDD as a map of (value, count) pairs. The final
   * combine step happens locally on the master, equivalent to running a single reduce task.
   */
  def countByValue(): JMap[T, jl.Long] =
    mapAsSerializableJavaMap(rdd.countByValue()).asInstanceOf[JMap[T, jl.Long]]

  /**
   * Approximate version of countByValue().
   *
   * The confidence is the probability that the error bounds of the result will
   * contain the true value. That is, if countApprox were called repeatedly
   * with confidence 0.9, we would expect 90% of the results to contain the
   * true count. The confidence must be in the range [0,1] or an exception will
   * be thrown.
   *
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param confidence the desired statistical confidence in the result
   * @return a potentially incomplete result, with error bounds
   */
  def countByValueApprox(
    timeout: Long,
    confidence: Double
    ): PartialResult[JMap[T, BoundedDouble]] =
    rdd.countByValueApprox(timeout, confidence).map(mapAsSerializableJavaMap)

  /**
   * Approximate version of countByValue().
   *
   * @param timeout maximum time to wait for the job, in milliseconds
   * @return a potentially incomplete result, with error bounds
   */
  def countByValueApprox(timeout: Long): PartialResult[JMap[T, BoundedDouble]] =
    rdd.countByValueApprox(timeout).map(mapAsSerializableJavaMap)

  /**
   * Take the first num elements of the RDD. This currently scans the partitions *one by one*, so
   * it will be slow if a lot of partitions are required. In that case, use collect() to get the
   * whole RDD instead.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def take(num: Int): JList[T] =
    rdd.take(num).toImmutableArraySeq.asJava

  def takeSample(withReplacement: Boolean, num: Int): JList[T] =
    takeSample(withReplacement, num, Utils.random.nextLong)

  def takeSample(withReplacement: Boolean, num: Int, seed: Long): JList[T] =
    rdd.takeSample(withReplacement, num, seed).toImmutableArraySeq.asJava

  /**
   * Return the first element in this RDD.
   */
  def first(): T = rdd.first()

  /**
   * @return true if and only if the RDD contains no elements at all. Note that an RDD
   *         may be empty even when it has at least 1 partition.
   */
  def isEmpty(): Boolean = rdd.isEmpty()

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  def saveAsTextFile(path: String): Unit = {
    rdd.saveAsTextFile(path)
  }


  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    rdd.saveAsTextFile(path, codec)
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
   */
  def saveAsObjectFile(path: String): Unit = {
    rdd.saveAsObjectFile(path)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   */
  def keyBy[U](f: JFunction[T, U]): JavaPairRDD[U, T] = {
    // The type parameter is U instead of K in order to work around a compiler bug; see SPARK-4459
    implicit val ctag: ClassTag[U] = fakeClassTag
    JavaPairRDD.fromRDD(rdd.keyBy(f))
  }

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with SparkContext.setCheckpointDir() and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def checkpoint(): Unit = {
    rdd.checkpoint()
  }

  /**
   * Return whether this RDD has been checkpointed or not
   */
  def isCheckpointed: Boolean = rdd.isCheckpointed

  /**
   * Gets the name of the file to which this RDD was checkpointed
   */
  def getCheckpointFile(): Optional[String] = {
    JavaUtils.optionToOptional(rdd.getCheckpointFile)
  }

  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString(): String = {
    rdd.toDebugString
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by
   * the specified Comparator[T] and maintains the order.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   * @param num k, the number of top elements to return
   * @param comp the comparator that defines the order
   * @return an array of top elements
   */
  def top(num: Int, comp: Comparator[T]): JList[T] = {
    rdd.top(num)(Ordering.comparatorToOrdering(comp)).toImmutableArraySeq.asJava
  }

  /**
   * Returns the top k (largest) elements from this RDD using the
   * natural ordering for T and maintains the order.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   * @param num k, the number of top elements to return
   * @return an array of top elements
   */
  def top(num: Int): JList[T] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[T]]
    top(num, comp)
  }

  /**
   * Returns the first k (smallest) elements from this RDD as defined by
   * the specified Comparator[T] and maintains the order.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   * @param num k, the number of elements to return
   * @param comp the comparator that defines the order
   * @return an array of top elements
   */
  def takeOrdered(num: Int, comp: Comparator[T]): JList[T] = {
    rdd.takeOrdered(num)(Ordering.comparatorToOrdering(comp)).toImmutableArraySeq.asJava
  }

  /**
   * Returns the maximum element from this RDD as defined by the specified
   * Comparator[T].
   *
   * @param comp the comparator that defines ordering
   * @return the maximum of the RDD
   */
  def max(comp: Comparator[T]): T = {
    rdd.max()(Ordering.comparatorToOrdering(comp))
  }

  /**
   * Returns the minimum element from this RDD as defined by the specified
   * Comparator[T].
   *
   * @param comp the comparator that defines ordering
   * @return the minimum of the RDD
   */
  def min(comp: Comparator[T]): T = {
    rdd.min()(Ordering.comparatorToOrdering(comp))
  }

  /**
   * Returns the first k (smallest) elements from this RDD using the
   * natural ordering for T while maintain the order.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   * @param num k, the number of top elements to return
   * @return an array of top elements
   */
  def takeOrdered(num: Int): JList[T] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[T]]
    takeOrdered(num, comp)
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double): Long = rdd.countApproxDistinct(relativeSD)

  def name(): String = rdd.name

  /**
   * The asynchronous version of `count`, which returns a
   * future for counting the number of elements in this RDD.
   */
  def countAsync(): JavaFutureAction[jl.Long] = {
    new JavaFutureActionWrapper[Long, jl.Long](rdd.countAsync(), jl.Long.valueOf)
  }

  /**
   * The asynchronous version of `collect`, which returns a future for
   * retrieving an array containing all of the elements in this RDD.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def collectAsync(): JavaFutureAction[JList[T]] = {
    new JavaFutureActionWrapper(rdd.collectAsync(), (x: Seq[T]) => x.asJava)
  }

  /**
   * The asynchronous version of the `take` action, which returns a
   * future for retrieving the first `num` elements of this RDD.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def takeAsync(num: Int): JavaFutureAction[JList[T]] = {
    new JavaFutureActionWrapper(rdd.takeAsync(num), (x: Seq[T]) => x.asJava)
  }

  /**
   * The asynchronous version of the `foreach` action, which
   * applies a function f to all the elements of this RDD.
   */
  def foreachAsync(f: VoidFunction[T]): JavaFutureAction[Void] = {
    new JavaFutureActionWrapper[Unit, Void](rdd.foreachAsync(x => f.call(x)),
      { x => null.asInstanceOf[Void] })
  }

  /**
   * The asynchronous version of the `foreachPartition` action, which
   * applies a function f to each partition of this RDD.
   */
  def foreachPartitionAsync(f: VoidFunction[JIterator[T]]): JavaFutureAction[Void] = {
    new JavaFutureActionWrapper[Unit, Void](rdd.foreachPartitionAsync(x => f.call(x.asJava)),
      { x => null.asInstanceOf[Void] })
  }
}
