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

package org.apache.spark.streaming.dstream

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext.rddToFileName
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf}

/**
 * Extra functions available on DStream of (key, value) pairs through an implicit conversion.
 */
class PairDStreamFunctions[K, V](self: DStream[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K])
  extends Serializable {
  private[streaming] def ssc = self.ssc

  private[streaming] def sparkContext = self.context.sparkContext

  private[streaming] def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   */
  def groupByKey(): DStream[(K, Iterable[V])] = ssc.withScope {
    groupByKey(defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with `numPartitions` partitions.
   */
  def groupByKey(numPartitions: Int): DStream[(K, Iterable[V])] = ssc.withScope {
    groupByKey(defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying `groupByKey` on each RDD. The supplied
   * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   */
  def groupByKey(partitioner: Partitioner): DStream[(K, Iterable[V])] = ssc.withScope {
    val createCombiner = (v: V) => ArrayBuffer[V](v)
    val mergeValue = (c: ArrayBuffer[V], v: V) => (c += v)
    val mergeCombiner = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => (c1 ++ c2)
    combineByKey(createCombiner, mergeValue, mergeCombiner, partitioner)
      .asInstanceOf[DStream[(K, Iterable[V])]]
  }

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the associative and commutative reduce function. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   */
  def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)] = ssc.withScope {
    reduceByKey(reduceFunc, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the supplied reduce function. Hash partitioning is used to generate the RDDs
   * with `numPartitions` partitions.
   */
  def reduceByKey(
      reduceFunc: (V, V) => V,
      numPartitions: Int): DStream[(K, V)] = ssc.withScope {
    reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the supplied reduce function. org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def reduceByKey(
      reduceFunc: (V, V) => V,
      partitioner: Partitioner): DStream[(K, V)] = ssc.withScope {
    combineByKey((v: V) => v, reduceFunc, reduceFunc, partitioner)
  }

  /**
   * Combine elements of each key in DStream's RDDs using custom functions. This is similar to the
   * combineByKey for RDDs. Please refer to combineByKey in
   * org.apache.spark.rdd.PairRDDFunctions in the Spark core documentation for more information.
   */
  def combineByKey[C: ClassTag](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiner: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true): DStream[(K, C)] = ssc.withScope {
    val cleanedCreateCombiner = sparkContext.clean(createCombiner)
    val cleanedMergeValue = sparkContext.clean(mergeValue)
    val cleanedMergeCombiner = sparkContext.clean(mergeCombiner)
    new ShuffledDStream[K, V, C](
      self,
      cleanedCreateCombiner,
      cleanedMergeValue,
      cleanedMergeCombiner,
      partitioner,
      mapSideCombine)
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window. This is similar to
   * `DStream.groupByKey()` but applies it over a sliding window. The new DStream generates RDDs
   * with the same interval as this DStream. Hash partitioning is used to generate the RDDs with
   * Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration): DStream[(K, Iterable[V])] = ssc.withScope {
    groupByKeyAndWindow(windowDuration, self.slideDuration, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window. Similar to
   * `DStream.groupByKey()`, but applies it over a sliding window. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration)
      : DStream[(K, Iterable[V])] = ssc.withScope {
    groupByKeyAndWindow(windowDuration, slideDuration, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.groupByKey()`, but applies it over a sliding window.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  number of partitions of each RDD in the new DStream; if not specified
   *                       then Spark's default number of partitions will be used
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, Iterable[V])] = ssc.withScope {
    groupByKeyAndWindow(windowDuration, slideDuration, defaultPartitioner(numPartitions))
  }

  /**
   * Create a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.groupByKey()`, but applies it over a sliding window.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner    partitioner for controlling the partitioning of each RDD in the new
   *                       DStream.
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, Iterable[V])] = ssc.withScope {
    val createCombiner = (v: Iterable[V]) => new ArrayBuffer[V] ++= v
    val mergeValue = (buf: ArrayBuffer[V], v: Iterable[V]) => buf ++= v
    val mergeCombiner = (buf1: ArrayBuffer[V], buf2: ArrayBuffer[V]) => buf1 ++= buf2
    self.groupByKey(partitioner)
        .window(windowDuration, slideDuration)
        .combineByKey[ArrayBuffer[V]](createCombiner, mergeValue, mergeCombiner, partitioner)
        .asInstanceOf[DStream[(K, Iterable[V])]]
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.reduceByKey()`, but applies it over a sliding window. The new DStream
   * generates RDDs with the same interval as this DStream. Hash partitioning is used to generate
   * the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration
    ): DStream[(K, V)] = ssc.withScope {
    reduceByKeyAndWindow(reduceFunc, windowDuration, self.slideDuration, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. This is similar to
   * `DStream.reduceByKey()` but applies it over a sliding window. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[(K, V)] = ssc.withScope {
    reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. This is similar to
   * `DStream.reduceByKey()` but applies it over a sliding window. Hash partitioning is used to
   * generate the RDDs with `numPartitions` partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  number of partitions of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, V)] = ssc.withScope {
    reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration,
      defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. Similar to
   * `DStream.reduceByKey()`, but applies it over a sliding window.
   * @param reduceFunc associative and commutative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner    partitioner for controlling the partitioning of each RDD
   *                       in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, V)] = ssc.withScope {
    self.reduceByKey(reduceFunc, partitioner)
        .window(windowDuration, slideDuration)
        .reduceByKey(reduceFunc, partitioner)
  }

  /**
   * Return a new DStream by applying incremental `reduceByKey` over a sliding window.
   * The reduced value of over a new window is calculated using the old window's reduced value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   *
   * This is more efficient than reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param invReduceFunc inverse reduce function; such that for all y, invertible x:
   *                      `invReduceFunc(reduceFunc(x, y), x) = y`
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param filterFunc     Optional function to filter expired key-value pairs;
   *                       only pairs that satisfy the function are retained
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration = self.slideDuration,
      numPartitions: Int = ssc.sc.defaultParallelism,
      filterFunc: ((K, V)) => Boolean = null
    ): DStream[(K, V)] = ssc.withScope {
    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowDuration,
      slideDuration, defaultPartitioner(numPartitions), filterFunc
    )
  }

  /**
   * Return a new DStream by applying incremental `reduceByKey` over a sliding window.
   * The reduced value of over a new window is calculated using the old window's reduced value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient than reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * @param reduceFunc     associative and commutative reduce function
   * @param invReduceFunc  inverse reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner    partitioner for controlling the partitioning of each RDD in the new
   *                       DStream.
   * @param filterFunc     Optional function to filter expired key-value pairs;
   *                       only pairs that satisfy the function are retained
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner,
      filterFunc: ((K, V)) => Boolean
    ): DStream[(K, V)] = ssc.withScope {

    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    val cleanedInvReduceFunc = ssc.sc.clean(invReduceFunc)
    val cleanedFilterFunc = if (filterFunc != null) Some(ssc.sc.clean(filterFunc)) else None
    new ReducedWindowedDStream[K, V](
      self, cleanedReduceFunc, cleanedInvReduceFunc, cleanedFilterFunc,
      windowDuration, slideDuration, partitioner
    )
  }

  /**
   * Return a [[MapWithStateDStream]] by applying a function to every key-value element of
   * `this` stream, while maintaining some state data for each unique key. The mapping function
   * and other specification (e.g. partitioners, timeouts, initial state data, etc.) of this
   * transformation can be specified using `StateSpec` class. The state data is accessible in
   * as a parameter of type `State` in the mapping function.
   *
   * Example of using `mapWithState`:
   * {{{
   *    // A mapping function that maintains an integer state and return a String
   *    def mappingFunction(key: String, value: Option[Int], state: State[Int]): Option[String] = {
   *      // Use state.exists(), state.get(), state.update() and state.remove()
   *      // to manage state, and return the necessary string
   *    }
   *
   *    val spec = StateSpec.function(mappingFunction).numPartitions(10)
   *
   *    val mapWithStateDStream = keyValueDStream.mapWithState[StateType, MappedType](spec)
   * }}}
   *
   * @param spec          Specification of this transformation
   * @tparam StateType    Class type of the state data
   * @tparam MappedType   Class type of the mapped data
   */
  def mapWithState[StateType: ClassTag, MappedType: ClassTag](
      spec: StateSpec[K, V, StateType, MappedType]
    ): MapWithStateDStream[K, V, StateType, MappedType] = {
    new MapWithStateDStreamImpl[K, V, StateType, MappedType](
      self,
      spec.asInstanceOf[StateSpecImpl[K, V, StateType, MappedType]]
    )
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S]
    ): DStream[(K, S)] = ssc.withScope {
    updateStateByKey(updateFunc, defaultPartitioner())
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param numPartitions Number of partitions of each RDD in the new DStream.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      numPartitions: Int
    ): DStream[(K, S)] = ssc.withScope {
    updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * [[org.apache.spark.Partitioner]] is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner
    ): DStream[(K, S)] = ssc.withScope {
    val cleanedUpdateF = sparkContext.clean(updateFunc)
    val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
      iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
    }
    updateStateByKey(newUpdateFunc, partitioner, true)
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * [[org.apache.spark.Partitioner]] is used to control the partitioning of each RDD.
   * @param updateFunc State update function. Note, that this function may generate a different
   *                   tuple with a different key than the input key. Therefore keys may be removed
   *                   or added in this way. It is up to the developer to decide whether to
   *                   remember the partitioner despite the key being changed.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream
   * @param rememberPartitioner Whether to remember the partitioner object in the generated RDDs.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean): DStream[(K, S)] = ssc.withScope {
    val cleanedFunc = ssc.sc.clean(updateFunc)
    val newUpdateFunc = (_: Time, it: Iterator[(K, Seq[V], Option[S])]) => {
      cleanedFunc(it)
    }
    new StateDStream(self, newUpdateFunc, partitioner, rememberPartitioner, None)
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   * @param initialRDD initial state value of each key.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner,
      initialRDD: RDD[(K, S)]
    ): DStream[(K, S)] = ssc.withScope {
    val cleanedUpdateF = sparkContext.clean(updateFunc)
    val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
      iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
    }
    updateStateByKey(newUpdateFunc, partitioner, true, initialRDD)
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   * @param updateFunc State update function. Note, that this function may generate a different
   *                   tuple with a different key than the input key. Therefore keys may be removed
   *                   or added in this way. It is up to the developer to decide whether to
   *                   remember the  partitioner despite the key being changed.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream
   * @param rememberPartitioner Whether to remember the partitioner object in the generated RDDs.
   * @param initialRDD initial state value of each key.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean,
      initialRDD: RDD[(K, S)]): DStream[(K, S)] = ssc.withScope {
    val cleanedFunc = ssc.sc.clean(updateFunc)
    val newUpdateFunc = (_: Time, it: Iterator[(K, Seq[V], Option[S])]) => {
      cleanedFunc(it)
    }
    new StateDStream(self, newUpdateFunc, partitioner, rememberPartitioner, Some(initialRDD))
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key.
   * In every batch the updateFunc will be called for each state even if there are no new values.
   * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassTag](updateFunc: (Time, K, Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner,
      rememberPartitioner: Boolean,
      initialRDD: Option[RDD[(K, S)]] = None): DStream[(K, S)] = ssc.withScope {
    val cleanedFunc = ssc.sc.clean(updateFunc)
    val newUpdateFunc = (time: Time, iterator: Iterator[(K, Seq[V], Option[S])]) => {
      iterator.flatMap(t => cleanedFunc(time, t._1, t._2, t._3).map(s => (t._1, s)))
    }
    new StateDStream(self, newUpdateFunc, partitioner, rememberPartitioner, initialRDD)
  }

  /**
   * Return a new DStream by applying a map function to the value of each key-value pairs in
   * 'this' DStream without changing the key.
   */
  def mapValues[U: ClassTag](mapValuesFunc: V => U): DStream[(K, U)] = ssc.withScope {
    new MapValuedDStream[K, V, U](self, sparkContext.clean(mapValuesFunc))
  }

  /**
   * Return a new DStream by applying a flatmap function to the value of each key-value pairs in
   * 'this' DStream without changing the key.
   */
  def flatMapValues[U: ClassTag](
      flatMapValuesFunc: V => IterableOnce[U]
    ): DStream[(K, U)] = ssc.withScope {
    new FlatMapValuedDStream[K, V, U](self, sparkContext.clean(flatMapValuesFunc))
  }

  /**
   * Return a new DStream by applying 'cogroup' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number
   * of partitions.
   */
  def cogroup[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (Iterable[V], Iterable[W]))] = ssc.withScope {
    cogroup(other, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying 'cogroup' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   */
  def cogroup[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int): DStream[(K, (Iterable[V], Iterable[W]))] = ssc.withScope {
    cogroup(other, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying 'cogroup' between RDDs of `this` DStream and `other` DStream.
   * The supplied org.apache.spark.Partitioner is used to partition the generated RDDs.
   */
  def cogroup[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Iterable[V], Iterable[W]))] = ssc.withScope {
    self.transformWith(
      other,
      (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.cogroup(rdd2, partitioner)
    )
  }

  /**
   * Return a new DStream by applying 'join' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   */
  def join[W: ClassTag](other: DStream[(K, W)]): DStream[(K, (V, W))] = ssc.withScope {
    join[W](other, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying 'join' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   */
  def join[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int): DStream[(K, (V, W))] = ssc.withScope {
    join[W](other, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying 'join' between RDDs of `this` DStream and `other` DStream.
   * The supplied org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   */
  def join[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (V, W))] = ssc.withScope {
    self.transformWith(
      other,
      (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.join(rdd2, partitioner)
    )
  }

  /**
   * Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   */
  def leftOuterJoin[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (V, Option[W]))] = ssc.withScope {
    leftOuterJoin[W](other, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions`
   * partitions.
   */
  def leftOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int
    ): DStream[(K, (V, Option[W]))] = ssc.withScope {
    leftOuterJoin[W](other, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
   * `other` DStream. The supplied org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def leftOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (V, Option[W]))] = ssc.withScope {
    self.transformWith(
      other,
      (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.leftOuterJoin(rdd2, partitioner)
    )
  }

  /**
   * Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   */
  def rightOuterJoin[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (Option[V], W))] = ssc.withScope {
    rightOuterJoin[W](other, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions`
   * partitions.
   */
  def rightOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int
    ): DStream[(K, (Option[V], W))] = ssc.withScope {
    rightOuterJoin[W](other, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
   * `other` DStream. The supplied org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def rightOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Option[V], W))] = ssc.withScope {
    self.transformWith(
      other,
      (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.rightOuterJoin(rdd2, partitioner)
    )
  }

  /**
   * Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   */
  def fullOuterJoin[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (Option[V], Option[W]))] = ssc.withScope {
    fullOuterJoin[W](other, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions`
   * partitions.
   */
  def fullOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int
    ): DStream[(K, (Option[V], Option[W]))] = ssc.withScope {
    fullOuterJoin[W](other, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
   * `other` DStream. The supplied org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def fullOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Option[V], Option[W]))] = ssc.withScope {
    self.transformWith(
      other,
      (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.fullOuterJoin(rdd2, partitioner)
    )
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval
   * is generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix"
   */
  def saveAsHadoopFiles[F <: OutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassTag[F]): Unit = ssc.withScope {
    saveAsHadoopFiles(prefix, suffix, keyClass, valueClass,
      fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval
   * is generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix"
   */
  def saveAsHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(ssc.sparkContext.hadoopConfiguration)
    ): Unit = ssc.withScope {
    // Wrap conf in SerializableWritable so that ForeachDStream can be serialized for checkpoints
    val serializableConf = new SerializableJobConf(conf)
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      rdd.saveAsHadoopFile(file, keyClass, valueClass, outputFormatClass,
        new JobConf(serializableConf.value))
    }
    self.foreachRDD(saveFunc)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassTag[F]): Unit = ssc.withScope {
    saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass,
      fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = ssc.sparkContext.hadoopConfiguration
    ): Unit = ssc.withScope {
    // Wrap conf in SerializableWritable so that ForeachDStream can be serialized for checkpoints
    val serializableConf = new SerializableConfiguration(conf)
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      rdd.saveAsNewAPIHadoopFile(
        file, keyClass, valueClass, outputFormatClass, serializableConf.value)
    }
    self.foreachRDD(saveFunc)
  }

  private def keyClass: Class[_] = kt.runtimeClass

  private def valueClass: Class[_] = vt.runtimeClass
}
