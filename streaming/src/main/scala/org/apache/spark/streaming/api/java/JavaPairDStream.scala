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

import java.util.{List => JList}
import java.lang.{Long => JLong}

import scala.collection.JavaConversions._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.spark.Partitioner
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.{JavaUtils, JavaRDD, JavaPairRDD}
import org.apache.spark.storage.StorageLevel
import com.google.common.base.Optional
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

class JavaPairDStream[K, V](val dstream: DStream[(K, V)])(
    implicit val kManifiest: ClassManifest[K],
    implicit val vManifest: ClassManifest[V])
    extends JavaDStreamLike[(K, V), JavaPairDStream[K, V], JavaPairRDD[K, V]] {

  override def wrapRDD(rdd: RDD[(K, V)]): JavaPairRDD[K, V] = JavaPairRDD.fromRDD(rdd)

  // =======================================================================
  // Methods common to all DStream's
  // =======================================================================

  /** Return a new DStream containing only the elements that satisfy a predicate. */
  def filter(f: JFunction[(K, V), java.lang.Boolean]): JavaPairDStream[K, V] =
    dstream.filter((x => f(x).booleanValue()))

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaPairDStream[K, V] = dstream.cache()

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaPairDStream[K, V] = dstream.persist()

  /** Persist the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaPairDStream[K, V] = dstream.persist(storageLevel)

  /** Method that generates a RDD for the given Duration */
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
  def groupByKey(): JavaPairDStream[K, JList[V]] =
    dstream.groupByKey().mapValues(seqAsJavaList _)

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with `numPartitions` partitions.
   */
  def groupByKey(numPartitions: Int): JavaPairDStream[K, JList[V]] =
    dstream.groupByKey(numPartitions).mapValues(seqAsJavaList _)

  /**
   * Return a new DStream by applying `groupByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs are grouped into a
   * single sequence to generate the RDDs of the new DStream. [[org.apache.spark.Partitioner]]
   * is used to control the partitioning of each RDD.
   */
  def groupByKey(partitioner: Partitioner): JavaPairDStream[K, JList[V]] =
    dstream.groupByKey(partitioner).mapValues(seqAsJavaList _)

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the associative reduce function. Hash partitioning is used to generate the RDDs
   * with Spark's default number of partitions.
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
   * merged using the supplied reduce function. [[org.apache.spark.Partitioner]] is used to control the
   * partitioning of each RDD.
   */
  def reduceByKey(func: JFunction2[V, V, V], partitioner: Partitioner): JavaPairDStream[K, V] = {
    dstream.reduceByKey(func, partitioner)
  }

  /**
   * Combine elements of each key in DStream's RDDs using custom function. This is similar to the
   * combineByKey for RDDs. Please refer to combineByKey in [[PairRDDFunctions]] for more
   * information.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
      mergeValue: JFunction2[C, V, C],
      mergeCombiners: JFunction2[C, C, C],
      partitioner: Partitioner
    ): JavaPairDStream[K, C] = {
    implicit val cm: ClassManifest[C] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[C]]
    dstream.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window. This is similar to
   * `DStream.groupByKey()` but applies it over a sliding window. The new DStream generates RDDs
   * with the same interval as this DStream. Hash partitioning is used to generate the RDDs with
   * Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration): JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration).mapValues(seqAsJavaList _)
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
  : JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration).mapValues(seqAsJavaList _)
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
  :JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration, numPartitions)
      .mapValues(seqAsJavaList _)
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.groupByKey()`, but applies it over a sliding window.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ):JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration, partitioner)
      .mapValues(seqAsJavaList _)
  }

  /**
   * Create a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.reduceByKey()`, but applies it over a sliding window. The new DStream
   * generates RDDs with the same interval as this DStream. Hash partitioning is used to generate
   * the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowDuration: Duration)
  :JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration)
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. This is similar to
   * `DStream.reduceByKey()` but applies it over a sliding window. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: Function2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration
    ):JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration)
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. This is similar to
   * `DStream.reduceByKey()` but applies it over a sliding window. Hash partitioning is used to
   * generate the RDDs with `numPartitions` partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: Function2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, numPartitions)
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window. Similar to
   * `DStream.reduceByKey()`, but applies it over a sliding window.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: Function2[V, V, V],
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
   * @param reduceFunc associative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: Function2[V, V, V],
      invReduceFunc: Function2[V, V, V],
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
   * @param reduceFunc associative reduce function
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
      reduceFunc: Function2[V, V, V],
      invReduceFunc: Function2[V, V, V],
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
   * @param reduceFunc associative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   * @param filterFunc     function to filter expired key-value pairs;
   *                       only pairs that satisfy the function are retained
   *                       set this to null if you do not want to filter
   */
  def reduceByKeyAndWindow(
      reduceFunc: Function2[V, V, V],
      invReduceFunc: Function2[V, V, V],
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

  private def convertUpdateStateFunction[S](in: JFunction2[JList[V], Optional[S], Optional[S]]):
  (Seq[V], Option[S]) => Option[S] = {
    val scalaFunc: (Seq[V], Option[S]) => Option[S] = (values, state) => {
      val list: JList[V] = values
      val scalaState: Optional[S] = JavaUtils.optionToOptional(state)
      val result: Optional[S] = in.apply(list, scalaState)
      result.isPresent match {
        case true => Some(result.get())
        case _ => None
      }
    }
    scalaFunc
  }

  /**
   * Create a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @tparam S State type
   */
  def updateStateByKey[S](updateFunc: JFunction2[JList[V], Optional[S], Optional[S]])
  : JavaPairDStream[K, S] = {
    implicit val cm: ClassManifest[S] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[S]]
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc))
  }

  /**
   * Create a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param numPartitions Number of partitions of each RDD in the new DStream.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassManifest](
      updateFunc: JFunction2[JList[V], Optional[S], Optional[S]],
      numPartitions: Int)
  : JavaPairDStream[K, S] = {
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc), numPartitions)
  }

  /**
   * Create a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key.
   * [[org.apache.spark.Partitioner]] is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassManifest](
      updateFunc: JFunction2[JList[V], Optional[S], Optional[S]],
      partitioner: Partitioner
  ): JavaPairDStream[K, S] = {
    dstream.updateStateByKey(convertUpdateStateFunction(updateFunc), partitioner)
  }

  def mapValues[U](f: JFunction[V, U]): JavaPairDStream[K, U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    dstream.mapValues(f)
  }

  def flatMapValues[U](f: JFunction[V, java.lang.Iterable[U]]): JavaPairDStream[K, U] = {
    import scala.collection.JavaConverters._
    def fn = (x: V) => f.apply(x).asScala
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    dstream.flatMapValues(fn)
  }

  /**
   * Cogroup `this` DStream with `other` DStream. For each key k in corresponding RDDs of `this`
   * or `other` DStreams, the generated RDD will contains a tuple with the list of values for that
   * key in both RDDs. HashPartitioner is used to partition each generated RDD into default number
   * of partitions.
   */
  def cogroup[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (JList[V], JList[W])] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.cogroup(other.dstream).mapValues(t => (seqAsJavaList(t._1), seqAsJavaList((t._2))))
  }

  /**
   * Cogroup `this` DStream with `other` DStream. For each key k in corresponding RDDs of `this`
   * or `other` DStreams, the generated RDD will contains a tuple with the list of values for that
   * key in both RDDs. Partitioner is used to partition each generated RDD.
   */
  def cogroup[W](other: JavaPairDStream[K, W], partitioner: Partitioner)
  : JavaPairDStream[K, (JList[V], JList[W])] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.cogroup(other.dstream, partitioner)
        .mapValues(t => (seqAsJavaList(t._1), seqAsJavaList((t._2))))
  }

  /**
   * Join `this` DStream with `other` DStream. HashPartitioner is used
   * to partition each generated RDD into default number of partitions.
   */
  def join[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.join(other.dstream)
  }

  /**
   * Join `this` DStream with `other` DStream, that is, each RDD of the new DStream will
   * be generated by joining RDDs from `this` and other DStream. Uses the given
   * Partitioner to partition each generated RDD.
   */
  def join[W](other: JavaPairDStream[K, W], partitioner: Partitioner)
  : JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.join(other.dstream, partitioner)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsHadoopFiles[F <: OutputFormat[K, V]](prefix: String, suffix: String) {
    dstream.saveAsHadoopFiles(prefix, suffix)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]]) {
    dstream.saveAsHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf) {
    dstream.saveAsHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](prefix: String, suffix: String) {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix)
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
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]]) {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass)
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
      conf: Configuration = new Configuration) {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf)
  }

  override val classManifest: ClassManifest[(K, V)] =
    implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]
}

object JavaPairDStream {
  implicit def fromPairDStream[K: ClassManifest, V: ClassManifest](dstream: DStream[(K, V)])
  :JavaPairDStream[K, V] =
    new JavaPairDStream[K, V](dstream)

  def fromJavaDStream[K, V](dstream: JavaDStream[(K, V)]): JavaPairDStream[K, V] = {
    implicit val cmk: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val cmv: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    new JavaPairDStream[K, V](dstream.dstream)
  }

  def scalaToJavaLong[K: ClassManifest](dstream: JavaPairDStream[K, Long])
  : JavaPairDStream[K, JLong] = {
    StreamingContext.toPairDStreamFunctions(dstream.dstream).mapValues(new JLong(_))
  }
}
