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

package org.apache.spark.streaming

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{ReducedWindowedDStream, StateDStream}
import org.apache.spark.streaming.dstream.{CoGroupedDStream, ShuffledDStream}
import org.apache.spark.streaming.dstream.{MapValuedDStream, FlatMapValuedDStream}

import org.apache.spark.{Partitioner, HashPartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{Manifests, RDD, PairRDDFunctions}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration

class PairDStreamFunctions[K: ClassManifest, V: ClassManifest](self: DStream[(K,V)])
extends Serializable {

  private[streaming] def ssc = self.ssc

  private[streaming] def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with Spark's default number of partitions.
   */
  def groupByKey(): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `groupByKey` to each RDD. Hash partitioning is used to
   * generate the RDDs with `numPartitions` partitions.
   */
  def groupByKey(numPartitions: Int): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying `groupByKey` on each RDD. The supplied [[org.apache.spark.Partitioner]]
   * is used to control the partitioning of each RDD.
   */
  def groupByKey(partitioner: Partitioner): DStream[(K, Seq[V])] = {
    val createCombiner = (v: V) => ArrayBuffer[V](v)
    val mergeValue = (c: ArrayBuffer[V], v: V) => (c += v)
    val mergeCombiner = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => (c1 ++ c2)
    combineByKey(createCombiner, mergeValue, mergeCombiner, partitioner)
      .asInstanceOf[DStream[(K, Seq[V])]]
  }

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the associative reduce function. Hash partitioning is used to generate the RDDs
   * with Spark's default number of partitions.
   */
  def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner())
  }

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the supplied reduce function. Hash partitioning is used to generate the RDDs
   * with `numPartitions` partitions.
   */
  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
  }

  /**
   * Return a new DStream by applying `reduceByKey` to each RDD. The values for each key are
   * merged using the supplied reduce function. [[org.apache.spark.Partitioner]] is used to control the
   * partitioning of each RDD.
   */
  def reduceByKey(reduceFunc: (V, V) => V, partitioner: Partitioner): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, partitioner)
  }

  /**
   * Combine elements of each key in DStream's RDDs using custom functions. This is similar to the
   * combineByKey for RDDs. Please refer to combineByKey in
   * [[org.apache.spark.rdd.PairRDDFunctions]] for more information.
   */
  def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner) : DStream[(K, C)] = {
    new ShuffledDStream[K, V, C](self, createCombiner, mergeValue, mergeCombiner, partitioner)
  }

  /**
   * Return a new DStream by applying `groupByKey` over a sliding window. This is similar to
   * `DStream.groupByKey()` but applies it over a sliding window. The new DStream generates RDDs
   * with the same interval as this DStream. Hash partitioning is used to generate the RDDs with
   * Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration): DStream[(K, Seq[V])] = {
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
  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration): DStream[(K, Seq[V])] = {
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
    ): DStream[(K, Seq[V])] = {
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
   * @param partitioner    partitioner for controlling the partitioning of each RDD in the new DStream.
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, Seq[V])] = {
    self.window(windowDuration, slideDuration).groupByKey(partitioner)
  }

  /**
   * Return a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * Similar to `DStream.reduceByKey()`, but applies it over a sliding window. The new DStream
   * generates RDDs with the same interval as this DStream. Hash partitioning is used to generate
   * the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowDuration, self.slideDuration, defaultPartitioner())
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
      reduceFunc: (V, V) => V, 
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, defaultPartitioner())
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
   * @param numPartitions  number of partitions of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, defaultPartitioner(numPartitions))
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
   * @param partitioner    partitioner for controlling the partitioning of each RDD
   *                       in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    self.reduceByKey(cleanedReduceFunc, partitioner)
        .window(windowDuration, slideDuration)
        .reduceByKey(cleanedReduceFunc, partitioner)
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
   * @param reduceFunc associative reduce function
   * @param invReduceFunc inverse reduce function
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
    ): DStream[(K, V)] = {

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
   * @param reduceFunc     associative reduce function
   * @param invReduceFunc  inverse reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner    partitioner for controlling the partitioning of each RDD in the new DStream.
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
    ): DStream[(K, V)] = {

    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    val cleanedInvReduceFunc = ssc.sc.clean(invReduceFunc)
    val cleanedFilterFunc = if (filterFunc != null) Some(ssc.sc.clean(filterFunc)) else None
    new ReducedWindowedDStream[K, V](
      self, cleanedReduceFunc, cleanedInvReduceFunc, cleanedFilterFunc,
      windowDuration, slideDuration, partitioner
    )
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S]
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner())
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
  def updateStateByKey[S: ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      numPartitions: Int
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
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
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner
    ): DStream[(K, S)] = {
    val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }
    updateStateByKey(newUpdateFunc, partitioner, true)
  }

  /**
   * Return a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of each key.
   * [[org.apache.spark.Partitioner]] is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated. Note, that
   *                   this function may generate a different a tuple with a different key
   *                   than the input key. It is up to the developer to decide whether to
   *                   remember the partitioner despite the key being changed.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   * @param rememberPartitioner Whether to remember the paritioner object in the generated RDDs.
   * @tparam S State type
   */
  def updateStateByKey[S: ClassManifest](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean
    ): DStream[(K, S)] = {
     new StateDStream(self, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner)
  }


  def mapValues[U: ClassManifest](mapValuesFunc: V => U): DStream[(K, U)] = {
    new MapValuedDStream[K, V, U](self, mapValuesFunc)
  }

  def flatMapValues[U: ClassManifest](
      flatMapValuesFunc: V => TraversableOnce[U]
    ): DStream[(K, U)] = {
    new FlatMapValuedDStream[K, V, U](self, flatMapValuesFunc)
  }

  /**
   * Cogroup `this` DStream with `other` DStream. For each key k in corresponding RDDs of `this`
   * or `other` DStreams, the generated RDD will contains a tuple with the list of values for that
   * key in both RDDs. HashPartitioner is used to partition each generated RDD into default number
   * of partitions.
   */
  def cogroup[W: ClassManifest](other: DStream[(K, W)]): DStream[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner())
  }

  /**
   * Cogroup `this` DStream with `other` DStream using a partitioner. For each key k in corresponding RDDs of `this`
   * or `other` DStreams, the generated RDD will contains a tuple with the list of values for that
   * key in both RDDs. Partitioner is used to partition each generated RDD.
   */
  def cogroup[W: ClassManifest](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Seq[V], Seq[W]))] = {

    val cgd = new CoGroupedDStream[K](
      Seq(self.asInstanceOf[DStream[(K, _)]], other.asInstanceOf[DStream[(K, _)]]),
      partitioner
    )
    val pdfs = new PairDStreamFunctions[K, Seq[Seq[_]]](cgd)(
      classManifest[K],
      Manifests.seqSeqManifest
    )
    pdfs.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }

  /**
   * Join `this` DStream with `other` DStream. HashPartitioner is used
   * to partition each generated RDD into default number of partitions.
   */
  def join[W: ClassManifest](other: DStream[(K, W)]): DStream[(K, (V, W))] = {
    join[W](other, defaultPartitioner())
  }

  /**
   * Join `this` DStream with `other` DStream, that is, each RDD of the new DStream will
   * be generated by joining RDDs from `this` and other DStream. Uses the given
   * Partitioner to partition each generated RDD.
   */
  def join[W: ClassManifest](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (V, W))] = {
    this.cogroup(other, partitioner)
        .flatMapValues{
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is generated
   * based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix"
   */
  def saveAsHadoopFiles[F <: OutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassManifest[F]) {
    saveAsHadoopFiles(prefix, suffix, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is generated
   * based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix"
   */
  def saveAsHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf
    ) {  
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      rdd.saveAsHadoopFile(file, keyClass, valueClass, outputFormatClass, conf)
    }
    self.foreach(saveFunc)
  }

  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassManifest[F])  {
    saveAsNewAPIHadoopFiles(prefix, suffix, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
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
      conf: Configuration = new Configuration
    ) {
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      rdd.saveAsNewAPIHadoopFile(file, keyClass, valueClass, outputFormatClass, conf)
    }
    self.foreach(saveFunc)
  }

  private def getKeyClass() = implicitly[ClassManifest[K]].erasure

  private def getValueClass() = implicitly[ClassManifest[V]].erasure
}


