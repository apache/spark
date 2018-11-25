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
import java.util.{Comparator, Iterator => JIterator, List => JList}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.Partitioner._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.JavaUtils.mapAsSerializableJavaMap
import org.apache.spark.api.java.function.{FlatMapFunction, Function => JFunction,
  Function2 => JFunction2, PairFunction}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.{OrderedRDDFunctions, RDD}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class JavaPairRDD[K, V](val rdd: RDD[(K, V)])
                       (implicit val kClassTag: ClassTag[K], implicit val vClassTag: ClassTag[V])
  extends AbstractJavaRDDLike[(K, V), JavaPairRDD[K, V]] {

  override def wrapRDD(rdd: RDD[(K, V)]): JavaPairRDD[K, V] = JavaPairRDD.fromRDD(rdd)

  override val classTag: ClassTag[(K, V)] = rdd.elementClassTag

  import JavaPairRDD._

  // Common RDD functions

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
  def cache(): JavaPairRDD[K, V] = new JavaPairRDD[K, V](rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.persist(newLevel))

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   * This method blocks until all blocks are deleted.
   */
  def unpersist(): JavaPairRDD[K, V] = wrapRDD(rdd.unpersist())

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   */
  def unpersist(blocking: Boolean): JavaPairRDD[K, V] = wrapRDD(rdd.unpersist(blocking))

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaPairRDD[K, V] = new JavaPairRDD[K, V](rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int): JavaPairRDD[K, V] =
      new JavaPairRDD[K, V](rdd.distinct(numPartitions))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[(K, V), java.lang.Boolean]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.filter(x => f.call(x).booleanValue()))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int): JavaPairRDD[K, V] = fromRDD(rdd.coalesce(numPartitions))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean): JavaPairRDD[K, V] =
    fromRDD(rdd.coalesce(numPartitions, shuffle))

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int): JavaPairRDD[K, V] = fromRDD(rdd.repartition(numPartitions))

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double): JavaPairRDD[K, V] =
    sample(withReplacement, fraction, Utils.random.nextLong)

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.sample(withReplacement, fraction, seed))

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling).
   *
   * Create a sample of this RDD using variable sampling rates for different keys as specified by
   * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
   * RDD, to produce a sample of size that's approximately equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values.
   */
  def sampleByKey(withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double],
      seed: Long): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.sampleByKey(
      withReplacement,
      fractions.asScala.mapValues(_.toDouble).toMap, // map to Scala Double; toMap to serialize
      seed))

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling).
   *
   * Create a sample of this RDD using variable sampling rates for different keys as specified by
   * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
   * RDD, to produce a sample of size that's approximately equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values.
   *
   * Use Utils.random.nextLong as the default seed for the random number generator.
   */
  def sampleByKey(withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double]): JavaPairRDD[K, V] =
    sampleByKey(withReplacement, fractions, Utils.random.nextLong)

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling) containing exactly
   * math.ceil(numItems * samplingRate) for each stratum (group of pairs with the same key).
   *
   * This method differs from `sampleByKey` in that we make additional passes over the RDD to
   * create a sample size that's exactly equal to the sum of math.ceil(numItems * samplingRate)
   * over all key values with a 99.99% confidence. When sampling without replacement, we need one
   * additional pass over the RDD to guarantee sample size; when sampling with replacement, we need
   * two additional passes.
   */
  def sampleByKeyExact(withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double],
      seed: Long): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.sampleByKeyExact(
      withReplacement,
      fractions.asScala.mapValues(_.toDouble).toMap, // map to Scala Double; toMap to serialize
      seed))

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling) containing exactly
   * math.ceil(numItems * samplingRate) for each stratum (group of pairs with the same key).
   *
   * This method differs from `sampleByKey` in that we make additional passes over the RDD to
   * create a sample size that's exactly equal to the sum of math.ceil(numItems * samplingRate)
   * over all key values with a 99.99% confidence. When sampling without replacement, we need one
   * additional pass over the RDD to guarantee sample size; when sampling with replacement, we need
   * two additional passes.
   *
   * Use Utils.random.nextLong as the default seed for the random number generator.
   */
  def sampleByKeyExact(
      withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double]): JavaPairRDD[K, V] =
    sampleByKeyExact(withReplacement, fractions, Utils.random.nextLong)

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.union(other.rdd))

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * @note This method performs a shuffle internally.
   */
  def intersection(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.intersection(other.rdd))


  // first() has to be overridden here so that the generated method has the signature
  // 'public scala.Tuple2 first()'; if the trait's definition is used,
  // then the method has the signature 'public java.lang.Object first()',
  // causing NoSuchMethodErrors at runtime.
  override def first(): (K, V) = rdd.first()

  // Pair RDD functions

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns a JavaPairRDD[(K, V)] into a result of type JavaPairRDD[(K, C)], for a
   * "combined type" C.
   *
   * Users provide three functions:
   *
   *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *  - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, the serializer that is use
   * for the shuffle, and whether to perform map-side aggregation (if a mapper can produce multiple
   * items with the same key).
   *
   * @note V and C can be different -- for example, one might group an RDD of type (Int, Int) into
   * an RDD of type (Int, List[Int]).
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
      mergeValue: JFunction2[C, V, C],
      mergeCombiners: JFunction2[C, C, C],
      partitioner: Partitioner,
      mapSideCombine: Boolean,
      serializer: Serializer): JavaPairRDD[K, C] = {
      implicit val ctag: ClassTag[C] = fakeClassTag
    fromRDD(rdd.combineByKeyWithClassTag(
      createCombiner,
      mergeValue,
      mergeCombiners,
      partitioner,
      mapSideCombine,
      serializer
    ))
  }

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns a JavaPairRDD[(K, V)] into a result of type JavaPairRDD[(K, C)], for a
   * "combined type" C.
   *
   * Users provide three functions:
   *
   *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *  - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD. This method automatically
   * uses map-side aggregation in shuffling the RDD.
   *
   * @note V and C can be different -- for example, one might group an RDD of type (Int, Int) into
   * an RDD of type (Int, List[Int]).
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
      mergeValue: JFunction2[C, V, C],
      mergeCombiners: JFunction2[C, C, C],
      partitioner: Partitioner): JavaPairRDD[K, C] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, true, null)
  }

  /**
   * Simplified version of combineByKey that hash-partitions the output RDD and uses map-side
   * aggregation.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
      mergeValue: JFunction2[C, V, C],
      mergeCombiners: JFunction2[C, C, C],
      numPartitions: Int): JavaPairRDD[K, C] =
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numPartitions))

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce.
   */
  def reduceByKey(partitioner: Partitioner, func: JFunction2[V, V, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.reduceByKey(partitioner, func))

  /**
   * Merge the values for each key using an associative and commutative reduce function, but return
   * the result immediately to the master as a Map. This will also perform the merging locally on
   * each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce.
   */
  def reduceByKeyLocally(func: JFunction2[V, V, V]): java.util.Map[K, V] =
    mapAsSerializableJavaMap(rdd.reduceByKeyLocally(func))

  /** Count the number of elements for each key, and return the result to the master as a Map. */
  def countByKey(): java.util.Map[K, jl.Long] =
    mapAsSerializableJavaMap(rdd.countByKey()).asInstanceOf[java.util.Map[K, jl.Long]]

  /**
   * Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  def countByKeyApprox(timeout: Long): PartialResult[java.util.Map[K, BoundedDouble]] =
    rdd.countByKeyApprox(timeout).map(mapAsSerializableJavaMap)

  /**
   * Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
  : PartialResult[java.util.Map[K, BoundedDouble]] =
    rdd.countByKeyApprox(timeout, confidence).map(mapAsSerializableJavaMap)

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U](zeroValue: U, partitioner: Partitioner, seqFunc: JFunction2[U, V, U],
      combFunc: JFunction2[U, U, U]): JavaPairRDD[K, U] = {
    implicit val ctag: ClassTag[U] = fakeClassTag
    fromRDD(rdd.aggregateByKey(zeroValue, partitioner)(seqFunc, combFunc))
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U](zeroValue: U, numPartitions: Int, seqFunc: JFunction2[U, V, U],
      combFunc: JFunction2[U, U, U]): JavaPairRDD[K, U] = {
    implicit val ctag: ClassTag[U] = fakeClassTag
    fromRDD(rdd.aggregateByKey(zeroValue, numPartitions)(seqFunc, combFunc))
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's.
   * The former operation is used for merging values within a partition, and the latter is used for
   * merging values between partitions. To avoid memory allocation, both of these functions are
   * allowed to modify and return their first argument instead of creating a new U.
   */
  def aggregateByKey[U](zeroValue: U, seqFunc: JFunction2[U, V, U], combFunc: JFunction2[U, U, U]):
      JavaPairRDD[K, U] = {
    implicit val ctag: ClassTag[U] = fakeClassTag
    fromRDD(rdd.aggregateByKey(zeroValue)(seqFunc, combFunc))
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g ., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, partitioner: Partitioner, func: JFunction2[V, V, V])
  : JavaPairRDD[K, V] = fromRDD(rdd.foldByKey(zeroValue, partitioner)(func))

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g ., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, numPartitions: Int, func: JFunction2[V, V, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.foldByKey(zeroValue, numPartitions)(func))

  /**
   * Merge the values for each key using an associative function and a neutral "zero value"
   * which may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, func: JFunction2[V, V, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.foldByKey(zeroValue)(func))

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  def reduceByKey(func: JFunction2[V, V, V], numPartitions: Int): JavaPairRDD[K, V] =
    fromRDD(rdd.reduceByKey(func, numPartitions))

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   *
   * @note If you are grouping in order to perform an aggregation (such as a sum or average) over
   * each key, using `JavaPairRDD.reduceByKey` or `JavaPairRDD.combineByKey`
   * will provide much better performance.
   */
  def groupByKey(partitioner: Partitioner): JavaPairRDD[K, JIterable[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey(partitioner)))

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions.
   *
   * @note If you are grouping in order to perform an aggregation (such as a sum or average) over
   * each key, using `JavaPairRDD.reduceByKey` or `JavaPairRDD.combineByKey`
   * will provide much better performance.
   */
  def groupByKey(numPartitions: Int): JavaPairRDD[K, JIterable[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey(numPartitions)))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   */
  def subtract(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.subtract(other))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaPairRDD[K, V], numPartitions: Int): JavaPairRDD[K, V] =
    fromRDD(rdd.subtract(other, numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaPairRDD[K, V], p: Partitioner): JavaPairRDD[K, V] =
    fromRDD(rdd.subtract(other, p))

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   */
  def subtractByKey[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, V] = {
    implicit val ctag: ClassTag[W] = fakeClassTag
    fromRDD(rdd.subtractByKey(other))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   */
  def subtractByKey[W](other: JavaPairRDD[K, W], numPartitions: Int): JavaPairRDD[K, V] = {
    implicit val ctag: ClassTag[W] = fakeClassTag
    fromRDD(rdd.subtractByKey(other, numPartitions))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   */
  def subtractByKey[W](other: JavaPairRDD[K, W], p: Partitioner): JavaPairRDD[K, V] = {
    implicit val ctag: ClassTag[W] = fakeClassTag
    fromRDD(rdd.subtractByKey(other, p))
  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   */
  def partitionBy(partitioner: Partitioner): JavaPairRDD[K, V] =
    fromRDD(rdd.partitionBy(partitioner))

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  def join[W](other: JavaPairRDD[K, W], partitioner: Partitioner): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other, partitioner))

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def leftOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (V, Optional[W])] = {
    val joinResult = rdd.leftOuterJoin(other, partitioner)
    fromRDD(joinResult.mapValues{case (v, w) => (v, JavaUtils.optionToOptional(w))})
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (Optional[V], W)] = {
    val joinResult = rdd.rightOuterJoin(other, partitioner)
    fromRDD(joinResult.mapValues{case (v, w) => (JavaUtils.optionToOptional(v), w)})
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   */
  def fullOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (Optional[V], Optional[W])] = {
    val joinResult = rdd.fullOuterJoin(other, partitioner)
    fromRDD(joinResult.mapValues{ case (v, w) =>
      (JavaUtils.optionToOptional(v), JavaUtils.optionToOptional(w))
    })
  }

  /**
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the existing
   * partitioner/parallelism level and using map-side aggregation.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C]): JavaPairRDD[K, C] = {
    implicit val ctag: ClassTag[C] = fakeClassTag
    fromRDD(combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(rdd)))
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  def reduceByKey(func: JFunction2[V, V, V]): JavaPairRDD[K, V] = {
    fromRDD(reduceByKey(defaultPartitioner(rdd), func))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level.
   *
   * @note If you are grouping in order to perform an aggregation (such as a sum or average) over
   * each key, using `JavaPairRDD.reduceByKey` or `JavaPairRDD.combineByKey`
   * will provide much better performance.
   */
  def groupByKey(): JavaPairRDD[K, JIterable[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey()))

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other))

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: JavaPairRDD[K, W], numPartitions: Int): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other, numPartitions))

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def leftOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, Optional[W])] = {
    val joinResult = rdd.leftOuterJoin(other)
    fromRDD(joinResult.mapValues{case (v, w) => (v, JavaUtils.optionToOptional(w))})
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def leftOuterJoin[W](other: JavaPairRDD[K, W], numPartitions: Int)
  : JavaPairRDD[K, (V, Optional[W])] = {
    val joinResult = rdd.leftOuterJoin(other, numPartitions)
    fromRDD(joinResult.mapValues{case (v, w) => (v, JavaUtils.optionToOptional(w))})
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def rightOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (Optional[V], W)] = {
    val joinResult = rdd.rightOuterJoin(other)
    fromRDD(joinResult.mapValues{case (v, w) => (JavaUtils.optionToOptional(v), w)})
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](other: JavaPairRDD[K, W], numPartitions: Int)
  : JavaPairRDD[K, (Optional[V], W)] = {
    val joinResult = rdd.rightOuterJoin(other, numPartitions)
    fromRDD(joinResult.mapValues{case (v, w) => (JavaUtils.optionToOptional(v), w)})
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD using the existing partitioner/
   * parallelism level.
   */
  def fullOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (Optional[V], Optional[W])] = {
    val joinResult = rdd.fullOuterJoin(other)
    fromRDD(joinResult.mapValues{ case (v, w) =>
      (JavaUtils.optionToOptional(v), JavaUtils.optionToOptional(w))
    })
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD into the given number of partitions.
   */
  def fullOuterJoin[W](other: JavaPairRDD[K, W], numPartitions: Int)
  : JavaPairRDD[K, (Optional[V], Optional[W])] = {
    val joinResult = rdd.fullOuterJoin(other, numPartitions)
    fromRDD(joinResult.mapValues{ case (v, w) =>
      (JavaUtils.optionToOptional(v), JavaUtils.optionToOptional(w))
    })
  }

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   *
   * @note this method should only be used if the resulting data is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def collectAsMap(): java.util.Map[K, V] = mapAsSerializableJavaMap(rdd.collectAsMap())


  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U](f: JFunction[V, U]): JavaPairRDD[K, U] = {
    implicit val ctag: ClassTag[U] = fakeClassTag
    fromRDD(rdd.mapValues(f))
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  def flatMapValues[U](f: FlatMapFunction[V, U]): JavaPairRDD[K, U] = {
    def fn: (V) => Iterator[U] = (x: V) => f.call(x).asScala
    implicit val ctag: ClassTag[U] = fakeClassTag
    fromRDD(rdd.flatMapValues(fn))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (JIterable[V], JIterable[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other, partitioner)))

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2],
      partitioner: Partitioner): JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2, partitioner)))

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: JavaPairRDD[K, W1],
      other2: JavaPairRDD[K, W2],
      other3: JavaPairRDD[K, W3],
      partitioner: Partitioner)
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2], JIterable[W3])] =
    fromRDD(cogroupResult3ToJava(rdd.cogroup(other1, other2, other3, partitioner)))

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JIterable[V], JIterable[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other)))

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2])
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2)))

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: JavaPairRDD[K, W1],
      other2: JavaPairRDD[K, W2],
      other3: JavaPairRDD[K, W3])
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2], JIterable[W3])] =
    fromRDD(cogroupResult3ToJava(rdd.cogroup(other1, other2, other3)))

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: JavaPairRDD[K, W], numPartitions: Int)
  : JavaPairRDD[K, (JIterable[V], JIterable[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other, numPartitions)))

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2], numPartitions: Int)
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2, numPartitions)))

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: JavaPairRDD[K, W1],
      other2: JavaPairRDD[K, W2],
      other3: JavaPairRDD[K, W3],
      numPartitions: Int)
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2], JIterable[W3])] =
    fromRDD(cogroupResult3ToJava(rdd.cogroup(other1, other2, other3, numPartitions)))

  /** Alias for cogroup. */
  def groupWith[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JIterable[V], JIterable[W])] =
    fromRDD(cogroupResultToJava(rdd.groupWith(other)))

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2])
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.groupWith(other1, other2)))

  /** Alias for cogroup. */
  def groupWith[W1, W2, W3](other1: JavaPairRDD[K, W1],
      other2: JavaPairRDD[K, W2],
      other3: JavaPairRDD[K, W3])
  : JavaPairRDD[K, (JIterable[V], JIterable[W1], JIterable[W2], JIterable[W3])] =
    fromRDD(cogroupResult3ToJava(rdd.groupWith(other1, other2, other3)))

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   */
  def lookup(key: K): JList[V] = rdd.lookup(key).asJava

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsHadoopFile[F <: OutputFormat[_, _]](
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F],
      conf: JobConf) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsHadoopFile[F <: OutputFormat[_, _]](
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F]) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass)
  }

  /** Output the RDD to any Hadoop-supported file system, compressing with the supplied codec. */
  def saveAsHadoopFile[F <: OutputFormat[_, _]](
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F],
      codec: Class[_ <: CompressionCodec]) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, codec)
  }

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F],
      conf: Configuration) {
    rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using
   * a Configuration object for that storage system.
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration) {
    rdd.saveAsNewAPIHadoopDataset(conf)
  }

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F]) {
    rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   */
  def saveAsHadoopDataset(conf: JobConf) {
    rdd.saveAsHadoopDataset(conf)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    repartitionAndSortWithinPartitions(partitioner, comp)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner, comp: Comparator[K])
    : JavaPairRDD[K, V] = {
    implicit val ordering = comp // Allow implicit conversion of Comparator to Ordering.
    fromRDD(
      new OrderedRDDFunctions[K, V, (K, V)](rdd).repartitionAndSortWithinPartitions(partitioner))
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements in
   * ascending order. Calling `collect` or `save` on the resulting RDD will return or output an
   * ordered list of records (in the `save` case, they will be written to multiple `part-X` files
   * in the filesystem, in order of the keys).
   */
  def sortByKey(): JavaPairRDD[K, V] = sortByKey(true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(ascending: Boolean): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKey(comp, ascending)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(ascending: Boolean, numPartitions: Int): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKey(comp, ascending, numPartitions)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(comp: Comparator[K]): JavaPairRDD[K, V] = sortByKey(comp, true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(comp: Comparator[K], ascending: Boolean): JavaPairRDD[K, V] = {
    implicit val ordering = comp // Allow implicit conversion of Comparator to Ordering.
    fromRDD(new OrderedRDDFunctions[K, V, (K, V)](rdd).sortByKey(ascending))
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(comp: Comparator[K], ascending: Boolean, numPartitions: Int): JavaPairRDD[K, V] = {
    implicit val ordering = comp // Allow implicit conversion of Comparator to Ordering.
    fromRDD(new OrderedRDDFunctions[K, V, (K, V)](rdd).sortByKey(ascending, numPartitions))
  }

  /**
   * Return an RDD with the keys of each tuple.
   */
  def keys(): JavaRDD[K] = JavaRDD.fromRDD[K](rdd.map(_._1))

  /**
   * Return an RDD with the values of each tuple.
   */
  def values(): JavaRDD[V] = JavaRDD.fromRDD[V](rdd.map(_._2))

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param partitioner partitioner of the resulting RDD.
   */
  def countApproxDistinctByKey(relativeSD: Double, partitioner: Partitioner)
  : JavaPairRDD[K, jl.Long] = {
    fromRDD(rdd.countApproxDistinctByKey(relativeSD, partitioner)).
      asInstanceOf[JavaPairRDD[K, jl.Long]]
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param numPartitions number of partitions of the resulting RDD.
   */
  def countApproxDistinctByKey(relativeSD: Double, numPartitions: Int): JavaPairRDD[K, jl.Long] = {
    fromRDD(rdd.countApproxDistinctByKey(relativeSD, numPartitions)).
      asInstanceOf[JavaPairRDD[K, jl.Long]]
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinctByKey(relativeSD: Double): JavaPairRDD[K, jl.Long] = {
    fromRDD(rdd.countApproxDistinctByKey(relativeSD)).asInstanceOf[JavaPairRDD[K, jl.Long]]
  }

  /** Assign a name to this RDD */
  def setName(name: String): JavaPairRDD[K, V] = {
    rdd.setName(name)
    this
  }
}

object JavaPairRDD {
  private[spark]
  def groupByResultToJava[K: ClassTag, T](rdd: RDD[(K, Iterable[T])]): RDD[(K, JIterable[T])] = {
    rddToPairRDDFunctions(rdd).mapValues(_.asJava)
  }

  private[spark]
  def cogroupResultToJava[K: ClassTag, V, W](
      rdd: RDD[(K, (Iterable[V], Iterable[W]))]): RDD[(K, (JIterable[V], JIterable[W]))] = {
    rddToPairRDDFunctions(rdd).mapValues(x => (x._1.asJava, x._2.asJava))
  }

  private[spark]
  def cogroupResult2ToJava[K: ClassTag, V, W1, W2](
      rdd: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))])
      : RDD[(K, (JIterable[V], JIterable[W1], JIterable[W2]))] = {
    rddToPairRDDFunctions(rdd).mapValues(x => (x._1.asJava, x._2.asJava, x._3.asJava))
  }

  private[spark]
  def cogroupResult3ToJava[K: ClassTag, V, W1, W2, W3](
      rdd: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))])
  : RDD[(K, (JIterable[V], JIterable[W1], JIterable[W2], JIterable[W3]))] = {
    rddToPairRDDFunctions(rdd).mapValues(x => (x._1.asJava, x._2.asJava, x._3.asJava, x._4.asJava))
  }

  def fromRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): JavaPairRDD[K, V] = {
    new JavaPairRDD[K, V](rdd)
  }

  implicit def toRDD[K, V](rdd: JavaPairRDD[K, V]): RDD[(K, V)] = rdd.rdd

  private[spark]
  implicit def toScalaFunction2[T1, T2, R](fun: JFunction2[T1, T2, R]): Function2[T1, T2, R] = {
    (x: T1, x1: T2) => fun.call(x, x1)
  }

  private[spark] implicit def toScalaFunction[T, R](fun: JFunction[T, R]): T => R = x => fun.call(x)

  private[spark]
  implicit def pairFunToScalaFun[A, B, C](x: PairFunction[A, B, C]): A => (B, C) = y => x.call(y)

  /** Convert a JavaRDD of key-value pairs to JavaPairRDD. */
  def fromJavaRDD[K, V](rdd: JavaRDD[(K, V)]): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = fakeClassTag
    implicit val ctagV: ClassTag[V] = fakeClassTag
    new JavaPairRDD[K, V](rdd.rdd)
  }

}
