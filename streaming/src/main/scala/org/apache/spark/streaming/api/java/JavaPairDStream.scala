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

package org.apache.spark.streaming.api.java

import java.{lang => jl}
import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}

import org.apache.spark.Partitioner
import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext, JavaUtils, Optional}
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{FlatMapFunction, Function => JFunction,
  Function2 => JFunction2}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

/**
 * A Java-friendly interface to a DStream of key-value pairs, which provides extra methods
 * like `reduceByKey` and `join`.
 */
class JavaPairDStream[K, V](val dstream: DStream[(K, V)])(
    implicit val kManifest: ClassTag[K],
    implicit val vManifest: ClassTag[V])
    extends AbstractJavaDStreamLike[(K, V), JavaPairDStream[K, V], JavaPairRDD[K, V]] {

  override def wrapRDD(rdd: RDD[(K, V)]): JavaPairRDD[K, V] = JavaPairRDD.fromRDD(rdd)

  // =======================================================================
  // Methods common to all DStream's
  // =======================================================================

  /** Return a new DStream containing only the elements that satisfy a predicate. */
  def filter(f: JFunction[(K, V), java.lang.Boolean]): JavaPairDStream[K, V] =
    dstream.filter((x => f.call(x).booleanValue()))

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaPairDStream[K, V] = dstream.cache()

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaPairDStream[K, V] = dstream.persist()

  /** Persist the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaPairDStream[K, V] = dstream.persist(storageLevel)

  /**
   * Return a new DStream with an increased or decreased level of parallelism. Each RDD in the
   * returned DStream has exactly numPartitions partitions.
   */
  def repartition(numPartitions: Int): JavaPairDStream[K, V] = dstream.repartition(numPartitions)

  /** Method that generates an RDD for the given Duration */
  def compute(validTime: Time): JavaPairRDD[K, V] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaPairRDD(rdd)
      case None => null
    }
  }

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * The new DStream generates RDDs with the same interval as this DStream.
   * @param windowDuration width of the window; must be a multiple of this DStream's interval.
   * @return
   */
  def window(windowDuration: Duration): JavaPairDStream[K, V] =
    dstream.window(windowDuration)

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * @param windowDuration duration (i.e., width) of the window;
   *                   must be a multiple of this DStream's interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                   the new DStream will generate RDDs); must be a multiple of this
   *                   DStream's interval
   */
  def window(windowDuration: Duration, slideDuration: Duration): JavaPairDStream[K, V] =
    dstream.window(windowDuration, slideDuration)

  /**
   * Return a new DStream by unifying data of another DStream with this DStream.
   * @param that Another DStream having the same interval (i.e., slideDuration) as this DStream.
   */
  def union(that: JavaPairDStream[K, V]): JavaPairDStream[K, V] =
    dstream.union(that.dstream)

  // =======================================================================
  // Methods only for PairDStream's
  // =======================================================================

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   */
  def groupByKey(): JavaPairDStream[K, JIterable[V]] =
    dstream.groupByKey().mapValues(_.asJava)

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with `numPartitions` partitions.
   */
  def groupByKey(numPartitions: Int): JavaPairDStream[K, JIterable[V]] =
    dstream.groupByKey(numPartitions).mapValues(_.asJava)

  /**
   * Return a new DStream by applying `groupByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs are grouped into a
   * single sequence to generate the RDDs of the new DStream. org.apache.spark.Partitioner
   * is used to control the partitioning of each RDD.
   */
  def groupByKey(partitioner: Partitioner): JavaPairDStream[K, JIterable[V]] =
    dstream.groupByKey(partitioner).mapValues(_.asJava)

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the associative and commutative reduce function. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   */
  def reduceByKey(func: JFunction2[V, V, V]): JavaPairDStream[K, V] =
    dstream.reduceByKey(func)

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the supplied reduce function. Hash partitioning is used to generate the RDDs
   * with `numPartitions` partitions.
   */
  def reduceByKey(func: JFunction2[V, V, V], numPartitions: Int): JavaPairDStream[K, V] =
    dstream.reduceByKey(func, numPartitions)

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the supplied reduce function. org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def reduceByKey(func: JFunction2[V, V, V], partitioner: Partitioner): JavaPairDStream[K, V] = {
    dstream.reduceByKey(func, partitioner)
  }

  /**
   * Combine elements of each key in DStream's RDDs using custom function. This is similar to the
   * combineByKey for RDDs. Please refer to combineByKey in
   * org.apache.spark.rdd.PairRDDFunctions for more information.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
      mergeValue: JFunction2[C, V, C],
      mergeCombiners: JFunction2[C, C, C],
      partitioner: Partitioner
    ): JavaPairDStream[K, C] = {
    implicit val cm: ClassTag[C] = fakeClassTag
    dstream.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
  }

  /**
   * Combine elements of each key in DStream's RDDs using custom function. This is similar to the
   * combineByKey for RDDs. Please refer to combineByKey in
   * org.apache.spark.rdd.PairRDDFunctions for more information.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
      mergeValue: JFunction2[C, V, C],
      mergeCombiners: JFunction2[C, C, C],
      partitioner: Partitioner,
      mapSideCombine: Boolean
    ): JavaPairDStream[K, C] = {
    implicit val cm: ClassTag[C] = fakeClassTag
    dstream.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine)
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window. This is similar to
   * `DStream.groupByKey()` but applies it over a sliding window. The new DStream generates RDDs
   * with the same interval as this DStream. Hash partitioning is used to generate the RDDs with
   * Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration): JavaPairDStream[K, JIterable[V]] = {
    dstream.groupByKeyAndWindow(windowDuration).mapValues(_.asJava)
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
  : JavaPairDStream[K, JIterable[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration).mapValues(_.asJava)
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
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration, numPartitions: Int)
    : JavaPairDStream[K, JIterable[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration, numPartitions).mapValues(_.asJava)
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.groupByKey()`, but applies it over a sliding window.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): JavaPairDStream[K, JIterable[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration, partitioner).mapValues(_.asJava)
  }

  /**
   * Create a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.reduceByKey()`, but applies it over a sliding window. The new DStream
   * generates RDDs with the same interval as this DStream. Hash partitioning is used to generate
   * the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def reduceByKeyAndWindow(reduceFunc: JFunction2[V, V, V], windowDuration: Duration)
    : JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration)
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
      reduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration
    ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration)
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
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, numPartitions)
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. Similar to
   * `DStream.reduceByKey()`, but applies it over a sliding window.
   * @param reduceFunc associative rand commutative educe function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, partitioner)
  }

  /**
   * Return a new DStream by reducing over a using incremental computation.
   * The reduced value of over a new window is calculated using the old window's reduce value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient that reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param invReduceFunc inverse function; such that for all y, invertible x:
   *                      `invReduceFunc(reduceFunc(x, y), x) = y`
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: JFunction2[V, V, V],
      invReduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration
    ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration)
  }

  /**
   * Return a new DStream by applying incremental `reduceByKey` over a sliding window.
   * The reduced value of over a new window is calculated using the old window's reduce value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient that reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param reduceFunc associative and commutative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  number of partitions of each RDD in the new DStream.
   * @param filterFunc     function to filter expired key-value pairs;
   *                       only pairs that satisfy the function are retained
   *                       set this to null if you do not want to filter
   */
  def reduceByKeyAndWindow(
      reduceFunc: JFunction2[V, V, V],
      invReduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int,
      filterFunc: JFunction[(K, V), java.lang.Boolean]
    ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(
        reduceFunc,
        invReduceFunc,
        windowDuration,
        slideDuration,
        numPartitions,
        (p: (K, V)) => filterFunc(p).booleanValue()
    )
  }

  /**
   * Return a new DStream by applying incremental `reduceByKey` over a sliding window.
   * The reduced value of over a new window is calculated using the old window's reduce value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient that reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * @param reduceFunc associative and commutative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   * @param filterFunc     function to filter expired key-value pairs;
   *                       only pairs that satisfy the function are retained
   *                       set this to null if you do not want to filter
   */
  def reduceByKeyAndWindow(
      reduceFunc: JFunction2[V, V, V],
      invReduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner,
      filterFunc: JFunction[(K, V), java.lang.Boolean]
  ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(
        reduceFunc,
        invReduceFunc,
        windowDuration,
        slideDuration,
        partitioner,
        (p: (K, V)) => filterFunc(p).booleanValue()
    )
  }

  /**
   * Return a [[JavaMapWithStateDStream]] by applying a function to every key-value element of
   * `this` stream, while maintaining some state data for each unique key. The mapping function
   * and other specification (e.g. partitioners, timeouts, initial state data, etc.) of this
   * transformation can be specified using `StateSpec` class. The state data is accessible in
   * as a parameter of type `State` in the mapping function.
   *
   * Example of using `mapWithState`:
   * {{{
   *   // A mapping function that maintains an integer state and return a string
   *   Function3<String, Optional<Integer>, State<Integer>, String> mappingFunction =
   *       new Function3<String, Optional<Integer>, State<Integer>, String>() {
   *           @Override
   *           public Optional<String> call(Optional<Integer> value, State<Integer> state) {
   *               // Use state.exists(), state.get(), state.update() and state.remove()
   *               // to manage state, and return the necessary string
   *           }
   *       };
   *
   *    JavaMapWithStateDStream<String, Integer, Integer, String> mapWithStateDStream =
   *        keyValueDStream.mapWithState(StateSpec.function(mappingFunc));
   *}}}
   *
   * @param spec          Specification of this transformation
   * @tparam StateType    Class type of the state data
   * @tparam MappedType   Class type of the mapped data
   */
  def mapWithState[StateType, MappedType](spec: StateSpec[K, V, StateType, MappedType]):
    JavaMapWithStateDStream[K, V, StateType, MappedType] = {
    new JavaMapWithStateDStream(dstream.mapWithState(spec)(
      JavaSparkContext.fakeClassTag,
      JavaSparkContext.fakeClassTag))
  }

  private def convertUpdateStateFunction[S](in: JFunction2[JList[V], Optional[S], Optional[S]]):
  (Seq[V], Option[S]) => Option[S] = {
    val scalaFunc: (Seq[V], Option[S]) => Option[S] = (values, state) => {
      val list: JList[V] = values.asJava
      val scalaState: Optional[S] = JavaUtils.optionToOptional(state)
      val result: Optional[S] = in.apply(list, scalaState)
      if (result.isPresent) {
        Some(result.get())
      } else {
        None
      }
    }
    scalaFunc
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @tparam S State type
   */
  def updateStateByKey[S](updateFunc: JFunction2[JList[V], Optional[S], Optional[S]])
  : JavaPairDStream[K, S] = {
    implicit val cm: ClassTag[S] = fakeClassTag
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc))
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param numPartitions Number of partitions of each RDD in the new DStream.
   * @tparam S State type
   */
  def updateStateByKey[S](
      updateFunc: JFunction2[JList[V], Optional[S], Optional[S]],
      numPartitions: Int)
  : JavaPairDStream[K, S] = {
    implicit val cm: ClassTag[S] = fakeClassTag
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc), numPartitions)
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key.
   * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   * @tparam S State type
   */
  def updateStateByKey[S](
      updateFunc: JFunction2[JList[V], Optional[S], Optional[S]],
      partitioner: Partitioner
  ): JavaPairDStream[K, S] = {
    implicit val cm: ClassTag[S] = fakeClassTag
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc), partitioner)
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key.
   * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
   *                    DStream.
   * @param initialRDD initial state value of each key.
   * @tparam S State type
   */
  def updateStateByKey[S](
      updateFunc: JFunction2[JList[V], Optional[S], Optional[S]],
      partitioner: Partitioner,
      initialRDD: JavaPairRDD[K, S]
  ): JavaPairDStream[K, S] = {
    implicit val cm: ClassTag[S] = fakeClassTag
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc), partitioner, initialRDD)
  }

  /**
   * Return a new DStream by applying a map function to the value of each key-value pairs in
   * 'this' DStream without changing the key.
   */
  def mapValues[U](f: JFunction[V, U]): JavaPairDStream[K, U] = {
    implicit val cm: ClassTag[U] = fakeClassTag
    dstream.mapValues(f)
  }

  /**
   * Return a new DStream by applying a flatmap function to the value of each key-value pairs in
   * 'this' DStream without changing the key.
   */
  def flatMapValues[U](f: FlatMapFunction[V, U]): JavaPairDStream[K, U] = {
    def fn: (V) => Iterator[U] = (x: V) => f.call(x).asScala
    implicit val cm: ClassTag[U] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[U]]
    dstream.flatMapValues(fn)
  }

  /**
   * Return a new DStream by applying 'cogroup' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number
   * of partitions.
   */
  def cogroup[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (JIterable[V], JIterable[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    dstream.cogroup(other.dstream).mapValues(t => (t._1.asJava, t._2.asJava))
  }

  /**
   * Return a new DStream by applying 'cogroup' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   */
  def cogroup[W](
      other: JavaPairDStream[K, W],
      numPartitions: Int
    ): JavaPairDStream[K, (JIterable[V], JIterable[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    dstream.cogroup(other.dstream, numPartitions).mapValues(t => (t._1.asJava, t._2.asJava))
  }

  /**
   * Return a new DStream by applying 'cogroup' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   */
  def cogroup[W](
      other: JavaPairDStream[K, W],
      partitioner: Partitioner
    ): JavaPairDStream[K, (JIterable[V], JIterable[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    dstream.cogroup(other.dstream, partitioner).mapValues(t => (t._1.asJava, t._2.asJava))
  }

  /**
   * Return a new DStream by applying 'join' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   */
  def join[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    dstream.join(other.dstream)
  }

  /**
   * Return a new DStream by applying 'join' between RDDs of `this` DStream and `other` DStream.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   */
  def join[W](other: JavaPairDStream[K, W], numPartitions: Int): JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    dstream.join(other.dstream, numPartitions)
  }

  /**
   * Return a new DStream by applying 'join' between RDDs of `this` DStream and `other` DStream.
   * The supplied org.apache.spark.Partitioner is used to control the partitioning of each RDD.
   */
  def join[W](
      other: JavaPairDStream[K, W],
      partitioner: Partitioner
    ): JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    dstream.join(other.dstream, partitioner)
  }

  /**
   * Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   */
  def leftOuterJoin[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (V, Optional[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.leftOuterJoin(other.dstream)
    joinResult.mapValues{case (v, w) => (v, JavaUtils.optionToOptional(w))}
  }

  /**
   * Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions`
   * partitions.
   */
  def leftOuterJoin[W](
      other: JavaPairDStream[K, W],
      numPartitions: Int
    ): JavaPairDStream[K, (V, Optional[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.leftOuterJoin(other.dstream, numPartitions)
    joinResult.mapValues{case (v, w) => (v, JavaUtils.optionToOptional(w))}
  }

  /**
   * Return a new DStream by applying 'left outer join' between RDDs of `this` DStream and
   * `other` DStream. The supplied org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def leftOuterJoin[W](
      other: JavaPairDStream[K, W],
      partitioner: Partitioner
    ): JavaPairDStream[K, (V, Optional[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.leftOuterJoin(other.dstream, partitioner)
    joinResult.mapValues{case (v, w) => (v, JavaUtils.optionToOptional(w))}
  }

  /**
   * Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   */
  def rightOuterJoin[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (Optional[V], W)] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.rightOuterJoin(other.dstream)
    joinResult.mapValues{case (v, w) => (JavaUtils.optionToOptional(v), w)}
  }

  /**
   * Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions`
   * partitions.
   */
  def rightOuterJoin[W](
      other: JavaPairDStream[K, W],
      numPartitions: Int
    ): JavaPairDStream[K, (Optional[V], W)] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.rightOuterJoin(other.dstream, numPartitions)
    joinResult.mapValues{case (v, w) => (JavaUtils.optionToOptional(v), w)}
  }

  /**
   * Return a new DStream by applying 'right outer join' between RDDs of `this` DStream and
   * `other` DStream. The supplied org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def rightOuterJoin[W](
      other: JavaPairDStream[K, W],
      partitioner: Partitioner
    ): JavaPairDStream[K, (Optional[V], W)] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.rightOuterJoin(other.dstream, partitioner)
    joinResult.mapValues{case (v, w) => (JavaUtils.optionToOptional(v), w)}
  }

  /**
   * Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   */
  def fullOuterJoin[W](other: JavaPairDStream[K, W])
      : JavaPairDStream[K, (Optional[V], Optional[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.fullOuterJoin(other.dstream)
    joinResult.mapValues{ case (v, w) =>
      (JavaUtils.optionToOptional(v), JavaUtils.optionToOptional(w))
    }
  }

  /**
   * Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
   * `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions`
   * partitions.
   */
  def fullOuterJoin[W](
      other: JavaPairDStream[K, W],
      numPartitions: Int
    ): JavaPairDStream[K, (Optional[V], Optional[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.fullOuterJoin(other.dstream, numPartitions)
    joinResult.mapValues{ case (v, w) =>
      (JavaUtils.optionToOptional(v), JavaUtils.optionToOptional(w))
    }
  }

  /**
   * Return a new DStream by applying 'full outer join' between RDDs of `this` DStream and
   * `other` DStream. The supplied org.apache.spark.Partitioner is used to control
   * the partitioning of each RDD.
   */
  def fullOuterJoin[W](
      other: JavaPairDStream[K, W],
      partitioner: Partitioner
    ): JavaPairDStream[K, (Optional[V], Optional[W])] = {
    implicit val cm: ClassTag[W] = fakeClassTag
    val joinResult = dstream.fullOuterJoin(other.dstream, partitioner)
    joinResult.mapValues{ case (v, w) =>
      (JavaUtils.optionToOptional(v), JavaUtils.optionToOptional(w))
    }
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsHadoopFiles(prefix: String, suffix: String): Unit = {
    dstream.saveAsHadoopFiles(prefix, suffix)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsHadoopFiles[F <: OutputFormat[_, _]](
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F]): Unit = {
    dstream.saveAsHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsHadoopFiles[F <: OutputFormat[_, _]](
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F],
      conf: JobConf): Unit = {
    dstream.saveAsHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles(prefix: String, suffix: String): Unit = {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[_, _]](
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F]): Unit = {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[_, _]](
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[F],
      conf: Configuration = dstream.context.sparkContext.hadoopConfiguration): Unit = {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf)
  }

  /** Convert to a JavaDStream */
  def toJavaDStream(): JavaDStream[(K, V)] = {
    new JavaDStream[(K, V)](dstream)
  }

  override val classTag: ClassTag[(K, V)] = fakeClassTag
}

object JavaPairDStream {
  implicit def fromPairDStream[K: ClassTag, V: ClassTag](dstream: DStream[(K, V)])
  : JavaPairDStream[K, V] = {
    new JavaPairDStream[K, V](dstream)
  }

  def fromJavaDStream[K, V](dstream: JavaDStream[(K, V)]): JavaPairDStream[K, V] = {
    implicit val cmk: ClassTag[K] = fakeClassTag
    implicit val cmv: ClassTag[V] = fakeClassTag
    new JavaPairDStream[K, V](dstream.dstream)
  }

  def scalaToJavaLong[K: ClassTag](dstream: JavaPairDStream[K, Long])
  : JavaPairDStream[K, jl.Long] = {
    DStream.toPairDStreamFunctions(dstream.dstream).mapValues(jl.Long.valueOf)
  }
}
