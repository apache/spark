package org.apache.spark.shuffle

import java.util

import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.MutableList
import scala.collection.Iterator

/**
 * Created by vladio on 7/14/15.
 */
private[spark] class ShuffleAggregationManager[K, V](
  iterator: Iterator[Product2[K, V]]) {

  private[this] var isSpillEnabled = true
  private[this] var partialAggCheckInterval = 10000
  private[this] var partialAggReduction = 0.5
  private[this] var partialAggEnabled = true

  private[this] var uniqueKeysMap = createNormalMap[K, Boolean]
  private[this] var iteratedElements = MutableList[Product2[K, V]]()
  private[this] var numIteratedRecords = 0

  private[spark] def withConf(conf: SparkConf): this.type = {
    isSpillEnabled = conf.getBoolean("spark.shuffle.spill", true)
    partialAggCheckInterval = conf.getInt("spark.partialAgg.interval", 10000)
    partialAggReduction = conf.getDouble("spark.partialAgg.reduction", 0.5)
    this
  }

  // Functions for creating an ExternalAppendOnlyMap
  def createCombiner[T](i: T) = ArrayBuffer[T](i)
  def mergeValue[T](buffer: ArrayBuffer[T], i: T): ArrayBuffer[T] = buffer += i
  def mergeCombiners[T](buf1: ArrayBuffer[T], buf2: ArrayBuffer[T]): ArrayBuffer[T] =
    buf1 ++= buf2

  def createExternalMap[K, T] = new ExternalAppendOnlyMap[K, T, ArrayBuffer[T]](
    createCombiner[T], mergeValue[T], mergeCombiners[T])

  def createNormalMap[K, T] = new AppendOnlyMap[K, T]()

  if (SparkEnv.get != null) {
    withConf(SparkEnv.get.conf)
  }

  def getRestoredIterator(): Iterator[Product2[K, V]] = {
    if (iterator.hasNext)
      iteratedElements.toIterator ++ iterator
    else
      iteratedElements.toIterator
  }

  def enableAggregation(): Boolean = {
    var ok : Boolean = true
    while (iterator.hasNext && partialAggEnabled && ok) {
      val kv = iterator.next()

      iteratedElements += kv
      numIteratedRecords += 1

      uniqueKeysMap.update(kv._1, true)

      if (numIteratedRecords == partialAggCheckInterval) {
        val partialAggSize = uniqueKeysMap.size
        if (partialAggSize > numIteratedRecords * partialAggReduction) {
          partialAggEnabled = false
        }

        ok = false
      }
    }

    partialAggEnabled
  }
}
