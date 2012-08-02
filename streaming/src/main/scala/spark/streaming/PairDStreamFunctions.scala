package spark.streaming

import scala.collection.mutable.ArrayBuffer
import spark.streaming.SparkStreamContext._

class PairDStreamFunctions[K: ClassManifest, V: ClassManifest](stream: DStream[(K,V)])
extends Serializable {
 
  def ssc = stream.ssc

  /* ---------------------------------- */
  /* DStream operations for key-value pairs */
  /* ---------------------------------- */
  
  def groupByKey(numPartitions: Int = 0): ShuffledDStream[K, V, ArrayBuffer[V]] = {
    def createCombiner(v: V) = ArrayBuffer[V](v)
    def mergeValue(c: ArrayBuffer[V], v: V) = (c += v)
    def mergeCombiner(c1: ArrayBuffer[V], c2: ArrayBuffer[V]) = (c1 ++ c2)
    combineByKey[ArrayBuffer[V]](createCombiner, mergeValue, mergeCombiner, numPartitions)
  }
  
  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int = 0): ShuffledDStream[K, V, V] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey[V]((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, numPartitions)  
  }

  private def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    numPartitions: Int) : ShuffledDStream[K, V, C] = {
    new ShuffledDStream[K, V, C](stream, createCombiner, mergeValue, mergeCombiner, numPartitions)
  }

  def groupByKeyAndWindow(
    windowTime: Time, 
    slideTime: Time, 
    numPartitions: Int = 0): ShuffledDStream[K, V, ArrayBuffer[V]] = {
    stream.window(windowTime, slideTime).groupByKey(numPartitions)
  }

  def reduceByKeyAndWindow(
    reduceFunc: (V, V) => V, 
    windowTime: Time, 
    slideTime: Time, 
    numPartitions: Int = 0): ShuffledDStream[K, V, V] = {
    stream.window(windowTime, slideTime).reduceByKey(ssc.sc.clean(reduceFunc), numPartitions)
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
    slideTime: Time,
    numPartitions: Int): ReducedWindowedDStream[K, V] = {

    new ReducedWindowedDStream[K, V](
      stream,
      ssc.sc.clean(reduceFunc),
      ssc.sc.clean(invReduceFunc),
      windowTime,
      slideTime,
      numPartitions)
  }
}


