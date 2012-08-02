package spark.streaming

import scala.collection.mutable.ArrayBuffer
import spark.streaming.SparkStreamContext._

class PairRDSFunctions[K: ClassManifest, V: ClassManifest](rds: RDS[(K,V)])
extends Serializable {
 
  def ssc = rds.ssc

  /* ---------------------------------- */
  /* RDS operations for key-value pairs */
  /* ---------------------------------- */
  
  def groupByKey(numPartitions: Int = 0): ShuffledRDS[K, V, ArrayBuffer[V]] = {
    def createCombiner(v: V) = ArrayBuffer[V](v)
    def mergeValue(c: ArrayBuffer[V], v: V) = (c += v)
    def mergeCombiner(c1: ArrayBuffer[V], c2: ArrayBuffer[V]) = (c1 ++ c2)
    combineByKey[ArrayBuffer[V]](createCombiner, mergeValue, mergeCombiner, numPartitions)
  }
  
  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int = 0): ShuffledRDS[K, V, V] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey[V]((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, numPartitions)  
  }

  private def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    numPartitions: Int) : ShuffledRDS[K, V, C] = {
    new ShuffledRDS[K, V, C](rds, createCombiner, mergeValue, mergeCombiner, numPartitions)
  }

  def groupByKeyAndWindow(
    windowTime: Time, 
    slideTime: Time, 
    numPartitions: Int = 0): ShuffledRDS[K, V, ArrayBuffer[V]] = {
    rds.window(windowTime, slideTime).groupByKey(numPartitions)
  }

  def reduceByKeyAndWindow(
    reduceFunc: (V, V) => V, 
    windowTime: Time, 
    slideTime: Time, 
    numPartitions: Int = 0): ShuffledRDS[K, V, V] = {
    rds.window(windowTime, slideTime).reduceByKey(ssc.sc.clean(reduceFunc), numPartitions)
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
    numPartitions: Int): ReducedWindowedRDS[K, V] = {

    new ReducedWindowedRDS[K, V](
      rds, 
      ssc.sc.clean(reduceFunc),
      ssc.sc.clean(invReduceFunc),
      windowTime,
      slideTime,
      numPartitions)
  }
}


