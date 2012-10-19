package spark.streaming

import scala.collection.mutable.ArrayBuffer
import spark.Partitioner
import spark.HashPartitioner
import spark.streaming.StreamingContext._
import javax.annotation.Nullable

class PairDStreamFunctions[K: ClassManifest, V: ClassManifest](stream: DStream[(K,V)])
extends Serializable {
 
  def ssc = stream.ssc

  def defaultPartitioner(numPartitions: Int = stream.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  /* ---------------------------------- */
  /* DStream operations for key-value pairs */
  /* ---------------------------------- */

  def groupByKey(): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner())
  }

  def groupByKey(numPartitions: Int): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner(numPartitions))
  }

  def groupByKey(partitioner: Partitioner): DStream[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer[V](v)
    def mergeValue(c: ArrayBuffer[V], v: V) = (c += v)
    def mergeCombiner(c1: ArrayBuffer[V], c2: ArrayBuffer[V]) = (c1 ++ c2)
    combineByKey(createCombiner _, mergeValue _, mergeCombiner _, partitioner).asInstanceOf[DStream[(K, Seq[V])]]
  }

  def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner())
  }

  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
  }

  def reduceByKey(reduceFunc: (V, V) => V, partitioner: Partitioner): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, partitioner)
  }

  private def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner) : ShuffledDStream[K, V, C] = {
    new ShuffledDStream[K, V, C](stream, createCombiner, mergeValue, mergeCombiner, partitioner)
  }

  def groupByKeyAndWindow(windowTime: Time, slideTime: Time): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowTime, slideTime, defaultPartitioner())
  }

  def groupByKeyAndWindow(
      windowTime: Time, 
      slideTime: Time, 
      numPartitions: Int
    ): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowTime, slideTime, defaultPartitioner(numPartitions))
  }

  def groupByKeyAndWindow(
      windowTime: Time,
      slideTime: Time,
      partitioner: Partitioner
    ): DStream[(K, Seq[V])] = {
    stream.window(windowTime, slideTime).groupByKey(partitioner)
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowTime: Time
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowTime, stream.slideTime, defaultPartitioner())
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V, 
      windowTime: Time, 
      slideTime: Time
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowTime, slideTime, defaultPartitioner())
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V, 
      windowTime: Time, 
      slideTime: Time, 
      numPartitions: Int
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowTime, slideTime, defaultPartitioner(numPartitions))
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time,
      partitioner: Partitioner
    ): DStream[(K, V)] = {
    stream.window(windowTime, slideTime).reduceByKey(ssc.sc.clean(reduceFunc), partitioner)
  }

  // This method is the efficient sliding window reduce operation,
  // which requires the specification of an inverse reduce function,
  // so that new elements introduced in the window can be "added" using
  // reduceFunc to the previous window's result and old elements can be
  // "subtracted using invReduceFunc.

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time
    ): DStream[(K, V)] = {

    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowTime, slideTime, defaultPartitioner())
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time,
      numPartitions: Int
    ): DStream[(K, V)] = {

    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowTime, slideTime, defaultPartitioner(numPartitions))
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time,
      partitioner: Partitioner
    ): DStream[(K, V)] = {

    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    val cleanedInvReduceFunc = ssc.sc.clean(invReduceFunc)
    new ReducedWindowedDStream[K, V](
      stream, cleanedReduceFunc, cleanedInvReduceFunc, windowTime, slideTime, partitioner)
  }

  // TODO:
  //
  //
  //
  //
  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], S) => S
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner())
  }

  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], S) => S,
      numPartitions: Int
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
  }

  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], S) => S,
      partitioner: Partitioner
    ): DStream[(K, S)] = {
    val func = (iterator: Iterator[(K, Seq[V], S)]) => {
      iterator.map(tuple => (tuple._1, updateFunc(tuple._2, tuple._3)))
    }
    updateStateByKey(func, partitioner, true)
  }

  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Iterator[(K, Seq[V], S)]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean
    ): DStream[(K, S)] = {
     new StateDStream(stream, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner)
  }
}


