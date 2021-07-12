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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegralType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.collection.{BitSet, OpenHashMap, Utils}
import org.apache.spark.util.random.XORShiftRandom


trait ShuffleExecAccumulator extends AccumulatorV2[InternalRow, String] {

  private[sql] var mapId = -1

  private[sql] def location: String = {
    if (mapId == -1) "driver" else s"task_$mapId"
  }

  private[sql] def onTaskStart(): Unit = {
    val context = TaskContext.get
    mapId = if (context == null) -1 else context.partitionId
  }

  private[sql] def onTaskEnd(): Unit = { }
}


private[sql] object KeySketcher {

  // value computation at driver maybe non-trivial, update cached value per 1 min
  val UI_DRIVER_REFRESH_INTERVAL = 60000000000L

  // number of skewed keys to show on SparkUI
  val UI_FREQUENT_KEYS = 16

  val DEBUG_SIZE_DRIVER = 256

  val DEBUG_SIZE_EXECUTOR = 64

  val DEBUG_SIZE_MAX_FACTOR = 2

  // only store keys which are sampled more than this times
  val DEBUG_MIN_SAMPLED_EXECUTOR = 2

  val MAP_SIZE_FACTOR = 0.75
}



/**
 * An accumulator to sample joined keys in ShuffleExchangeExec. The sampled keys then could be used
 * in following AQE rules.
 * To control the memory footprint, a 64-bit hash is used instead of the original key. The
 * partition id can be obtained from the first 32-bit.
 * On executors, KeySketcher first try to count the keys exactly by keeping a countMap, when the
 * countMap is too larger, fallback to reservoir sampling.
 * At driver, sampled keys and their counts are weighted and gathered in sketchMap.
 * @param rowSchema       the input row schema.
 * @param partitionIdExpr the expression to compute partition id.
 * @param numMappers      number of map tasks.
 * @param arraySize       number of sampled keys in reservoir sampling.
 * @param exchangeId      id of corresponding ShuffleExchangeExec, used to get the sampling seed.
 * @param debug           whether to show debug info on the SparkUI. If true, KeySketcher will
 *                        maintain a mapping from hash64 to original string for better readability.
 */
// TODO: specialized implementation for simple key types like IntegerType/LongType/etc
private[sql] class KeySketcher(
    val rowSchema: Seq[Attribute],
    val partitionIdExpr: Expression,
    val numMappers: Int,
    val arraySize: Int,
    val exchangeId: Int,
    val debug: Boolean)
  extends ShuffleExecAccumulator with Logging {
  import KeySketcher._

  require(arraySize > 0)
  private val mapSize = (arraySize * MAP_SIZE_FACTOR).ceil.toInt


  /* ------------------------------------------------------ *
   | Private variables and methods                          |
   * ------------------------------------------------------ */

  // (row: UnsafeRow) -> (key hash64: Long)
  @transient private lazy val hashFunc = {
    partitionIdExpr match {
      case Pmod(h: Murmur3Hash, _, _) =>
        val hash64 = BitwiseOr(
          ShiftLeft(
            Cast(h, LongType),
            Literal(32)
          ),
          BitwiseAnd(
            // make sure two Murmur3Hash use different seeds
            Cast(h.copy(seed = -1 - h.seed), LongType),
            Literal(0xFFFFFFFFL)
          )
        )
        val project = UnsafeProjection.create(Seq(hash64), rowSchema)
        (row: InternalRow) => project(row).getLong(0)

      case _ => throw new UnsupportedOperationException(
        s"partExpression Must be Pmod(Murmur3Hash, num), but got $partitionIdExpr")
    }
  }

  // (row: UnsafeRow) -> (str: UTF8String)
  // UnsafeRow.toString returns a HexString, which lacks readability.
  @transient private lazy val strFunc = {
    partitionIdExpr match {
      case Pmod(Murmur3Hash(keys: Seq[Expression], _), _, _) =>
        val strExpr = if (keys.length == 1) {
          Cast(keys.head, StringType)
        } else {
          Cast(CreateStruct.create(keys), StringType)
        }
        val project = UnsafeProjection.create(Seq(strExpr), rowSchema)
        (row: InternalRow) => project(row).getUTF8String(0).copy()

      case _ => throw new UnsupportedOperationException(
        s"partExpression Must be Pmod(Murmur3Hash, num), but got $partitionIdExpr")
    }
  }

  private[sql] def numPartitions: Int = {
    partitionIdExpr match {
      case Pmod(_: Murmur3Hash, Literal(n: Int, _: IntegralType), _) => n

      case _ => throw new UnsupportedOperationException(
        s"partExpression Must be Pmod(Murmur3Hash, num), but got $partitionIdExpr")
    }
  }

  override def isZero: Boolean = {
    if (mapId < 0) sketchMap == null else sampledHashes == null
  }

  private[sql] var count = 0L

  private[sql] var sampled = 0L

  @transient private var debugStrMap: OpenHashMap[Long, UTF8String] = _

  private var taskDuration = 0L

  private var accumulationDuration = 0L

  @transient private var lastValue: String = _

  @transient private var lastValueTimestamp = System.nanoTime

  override def value: String = this.synchronized {
    if (mapId < 0) {
      if (lastValue == null || System.nanoTime - lastValueTimestamp > UI_DRIVER_REFRESH_INTERVAL) {
        lastValue = doValue
        lastValueTimestamp = System.nanoTime
      }
      lastValue
    } else {
      doValue
    }
  }

  private def doValue: String = {
    if (isZero) {
      s"$location, Unavailable"
    } else if (count == 0) {
      s"$location, Empty"
    } else if ((mapId < 0 && sampled == sketchMap.size) || (mapId >= 0 && sampledCounts == null)) {
      s"$location, $count counted, $sampled sampled, Distinct, " +
        f"cost=${accumulationDuration.toDouble / taskDuration * 100}%.3f%%"
    } else {

      val start = System.nanoTime

      val percentScale = 100.0 / count
      val hotKeyStr = if (mapId < 0) {
        Utils.takeOrdered(sketchMap.iterator, UI_FREQUENT_KEYS)(Ordering.by(-_._2))
          .map { case (hash64, w) =>
            if (debugStrMap != null && debugStrMap.contains(hash64)) {
              f"${debugStrMap(hash64)}:${w * percentScale}%.2f%%"
            } else {
              f"${w * percentScale}%.2f%%"
            }
          }.mkString("{", ", ", "}")
      } else {
        decompressWeightIter.take(UI_FREQUENT_KEYS).zipWithIndex
          .map { case (w, i) =>
            if (sampledDebugStrs != null && i < sampledDebugStrs.length &&
              sampledDebugStrs(i) != null) {
              f"${sampledDebugStrs(i)}:${w * percentScale}%.2f%%"
            } else {
              f"${w * percentScale}%.2f%%"
            }
          }.mkString("{", ", ", "}")
      }

      val hashed = if (mapId < 0) sketchMap.size else sampledHashes.length

      val valueStr = s"$location, $count counted, $sampled sampled, $hashed hashed, " +
        s"hot keys: $hotKeyStr, " +
        f"cost=${accumulationDuration.toDouble / taskDuration * 100}%.3f%%"

      logDebug(f"Exchange #$exchangeId, $location, value computation takes " +
        f"${(System.nanoTime - start) * 1e-9}%.3f sec, " +
        f"cost=${accumulationDuration.toDouble / taskDuration * 100}%.3f%%")

      valueStr
    }
  }


  /* ------------------------------------------------------ *
   | Private variables and methods used only at driver      |
   * ------------------------------------------------------ */

  // key hash64 -> weight
  @transient private[sql] var sketchMap: OpenHashMap[Long, Float] = _

  @transient private lazy val mergedMapIds = new BitSet(numMappers)

  override def copy(): KeySketcher = throw new UnsupportedOperationException("Not implemented")

  override def reset(): Unit = this.synchronized {
    count = 0L
    sampled = 0L

    sketchMap = null
    mergedMapIds.clear()

    counting = false
    countMap = null
    samplingRNG = null
    samplingArray = null
    sampledHashes = null
    sampledCounts = null

    debugStrMap = null
    sampledDebugStrs = null

    accumulationDuration = 0L
    taskDuration = 0L
  }

  override def copyAndReset(): KeySketcher = {
    new KeySketcher(rowSchema, partitionIdExpr, numMappers, arraySize, exchangeId, debug)
  }


  override def merge(other: AccumulatorV2[InternalRow, String]): Unit = {
    other match {
      case o: KeySketcher =>
        // other KeySketchers are expected to be compressed and sent from executors
        if (o.count > 0 && o.mapId >= 0 && o.sampledHashes != null && !mergedMapIds.get(o.mapId)) {
          val start = System.nanoTime
          val infoStr = s"Exchange #$exchangeId, this(${this.location}, " +
            s"sampled=${this.sampled}/${this.count}) " +
            s"+= other(${o.location}, sampled=${o.sampled}/${o.count})"

          this.synchronized {
            if (this.sketchMap == null) {
              this.sketchMap = new OpenHashMap[Long, Float](mapSize)
            }
            o.sampledHashes.iterator.zip(o.decompressWeightIter)
              .foreach { case (hash64, w) => this.sketchMap.changeValue(hash64, w, _ + w) }
            this.count += o.count
            this.sampled += o.sampled
            this.taskDuration += o.taskDuration
            this.accumulationDuration += o.accumulationDuration
            this.mergedMapIds.set(o.mapId)

            if (debug && o.sampledDebugStrs != null) {
              if (this.debugStrMap == null) {
                this.debugStrMap = new OpenHashMap[Long, UTF8String](DEBUG_SIZE_DRIVER)
              }

              o.sampledHashes.iterator.zip(o.sampledDebugStrs.iterator)
                .foreach { case (hash64, str) =>
                  if (str != null && !this.debugStrMap.contains(hash64)) {
                    this.debugStrMap.update(hash64, str)
                  }
                }

              if (this.debugStrMap.size > DEBUG_SIZE_DRIVER * DEBUG_SIZE_MAX_FACTOR) {
                val newDebugMap = new OpenHashMap[Long, UTF8String](DEBUG_SIZE_DRIVER)
                this.debugStrMap.iterator
                  .filter { case (hash64, _) => this.sketchMap.contains(hash64) }
                  .map { case (hash64, str) => ((hash64, str), this.sketchMap(hash64)) }
                  .toArray.sortBy(-_._2).iterator
                  .map(_._1).take(DEBUG_SIZE_DRIVER)
                  .foreach { case (hash64, str) => newDebugMap.update(hash64, str) }
                this.debugStrMap = newDebugMap
              }
            }
          }

          logDebug(f"$infoStr, merge takes ${(System.nanoTime - start) * 1e-9}%.3f sec, " +
            f"cost=${accumulationDuration.toDouble / taskDuration * 100}%.3f%%")
        }

      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }


  /* ------------------------------------------------------ *
   | Private variables and methods used only in executors   |
   * ------------------------------------------------------ */


  override private[sql] def onTaskStart(): Unit = {
    super.onTaskStart()
    taskStart = System.nanoTime
    sketchMap = null
    counting = true
    countMap = new collection.mutable.OpenHashMap[Long, Int](mapSize)
    debugStrMap = if (debug) new OpenHashMap[Long, UTF8String](DEBUG_SIZE_EXECUTOR) else null
    accumulationDuration = 0L
    taskDuration = 0L
  }

  @transient private var taskStart: Long = _

  @transient private var counting: Boolean = _

  @transient private var countMap: collection.mutable.OpenHashMap[Long, Int] = _

  @transient private var samplingRNG: XORShiftRandom = _

  @transient private var samplingArray: Array[Long] = _

  private var sampledHashes: Array[Long] = _

  private var sampledCounts: Array[Int] = _

  private var sampledDebugStrs: Array[UTF8String] = _

  override def add(row: InternalRow): Unit = this.synchronized {
    val start = System.nanoTime

    var hash64Opt = Option.empty[Long]
    if (counting) {
      hash64Opt = addToMap(row)
      if (hash64Opt.isEmpty) {
        fallbackToArray()
        require(!counting)
        hash64Opt = addToArray(row)
      }
    } else {
      hash64Opt = addToArray(row)
    }

    if (debug && hash64Opt.nonEmpty) {
      val hash64 = hash64Opt.get
      if (!debugStrMap.contains(hash64) &&
        countMap.getOrElse(hash64, 0) >= DEBUG_MIN_SAMPLED_EXECUTOR) {
        debugStrMap.update(hash64, strFunc(row))

        // if debugStrMap is too large, drop keys with low freq
        if (debugStrMap.size > DEBUG_SIZE_EXECUTOR * DEBUG_SIZE_MAX_FACTOR) {
          val newDebugMap = new OpenHashMap[Long, UTF8String](DEBUG_SIZE_EXECUTOR)
          countMap.toArray.sortBy(-_._2).iterator
            .map(_._1).filter(debugStrMap.contains)
            .take(DEBUG_SIZE_EXECUTOR)
            .foreach(hash64 => newDebugMap.update(hash64, debugStrMap(hash64)))
          debugStrMap = newDebugMap
        }
      }
    }

    accumulationDuration += System.nanoTime - start
  }

  private def addToMap(row: InternalRow): Option[Long] = {
    val hash64 = hashFunc(row)
    if (countMap.size < mapSize || countMap.contains(hash64)) {
      countMap.update(hash64, countMap.getOrElse(hash64, 0) + 1)
      count += 1L
      Some(hash64)
    } else {
      None
    }
  }

  private def addToArray(row: InternalRow): Option[Long] = {
    if (count < arraySize) {
      val hash64 = hashFunc(row)
      samplingArray(count.toInt) = hash64
      count += 1L
      if (debug) {
        // if debug, still maintain countMap in sampling mode, in order to avoid
        // calling strFunc for keys with freq less than DEBUG_MIN_SAMPLED_EXECUTOR
        countMap.update(hash64, countMap.getOrElse(hash64, 0) + 1)
      }
      Some(hash64)

    } else {

      count += 1L
      val replacementIndex = (samplingRNG.nextDouble * count).toLong
      if (replacementIndex < arraySize) {
        val hash64 = hashFunc(row)

        if (debug) {
          // if debug, still maintain countMap in sampling mode
          val prevHash64 = samplingArray(replacementIndex.toInt)
          val prevCount = countMap.getOrElse(prevHash64, 0)
          if (prevCount > 1) {
            countMap.update(prevHash64, prevCount - 1)
          } else {
            countMap.remove(prevHash64)
          }
          countMap.update(hash64, countMap.getOrElse(hash64, 0) + 1)
        }

        samplingArray(replacementIndex.toInt) = hash64
        Some(hash64)
      } else {
        None
      }
    }
  }

  private def fallbackToArray(): Unit = {
    val start = System.nanoTime

    val prevCount = count
    count = 0L
    samplingRNG = new XORShiftRandom(exchangeId + mapId)
    samplingArray = Array.ofDim[Long](arraySize)
    countMap.foreach { case (hash64, c) =>
      var j = 0
      while (j < c) {
        if (count < arraySize) {
          samplingArray(count.toInt) = hash64
          count += 1L
        } else {
          count += 1L
          val replacementIndex = (samplingRNG.nextDouble * count).toLong
          if (replacementIndex < arraySize) {
            samplingArray(replacementIndex.toInt) = hash64
          }
        }
        j += 1
      }
    }
    require(count == prevCount)
    counting = false

    if (debug) {
      // if debug, still maintain countMap in sampling mode
      countMap.clear()
      Iterator.range(0, math.min(count, arraySize).toInt).foreach { i =>
        val hash64 = samplingArray(i)
        countMap.update(hash64, countMap.getOrElse(hash64, 0) + 1)
      }
    } else {
      countMap = null
    }

    logDebug(s"Exchange #$exchangeId, $location, " +
      s"countMap reaches maxMapSize=$mapSize at count=$count, " +
      f"fallbackToArray takes ${(System.nanoTime - start) * 1e-9}%.3f sec")
  }


  private def compress(): Unit = {
    if (sampledHashes == null) {
      if (counting) {
        sampled = count
      } else {
        sampled = math.min(count, arraySize)

        countMap = new collection.mutable.OpenHashMap[Long, Int](mapSize)
        Iterator.range(0, sampled.toInt).foreach { i =>
          val hash64 = samplingArray(i)
          countMap.update(hash64, countMap.getOrElse(hash64, 0) + 1)
        }
      }

      val (hashes, counts) = countMap.toArray.sortBy(-_._2).unzip
      // avoid sending count=1
      sampledCounts = if (counts.forall(_ == 1)) null else counts.filter(_ != 1)
      sampledHashes = hashes

      if (debug && sampledHashes.nonEmpty && sampledCounts != null &&
        sampledCounts.nonEmpty && sampledCounts.head >= DEBUG_MIN_SAMPLED_EXECUTOR) {

        var lastIndex = -1
        sampledDebugStrs = sampledHashes.iterator.zip(sampledCounts.iterator)
          .take(UI_FREQUENT_KEYS * DEBUG_SIZE_MAX_FACTOR)
          .zipWithIndex
          .map { case ((hash64, _), i) =>
             if (debugStrMap.contains(hash64)) {
               lastIndex = i
               debugStrMap(hash64)
             } else null
          }.toArray

        if (lastIndex == -1) {
          sampledDebugStrs = null
        } else {
          // strip tailing nulls
          sampledDebugStrs = sampledDebugStrs.take(lastIndex + 1)
        }
      }
    }
  }

  private def decompressWeightIter: Iterator[Float] = {
    val scale = count.toFloat / sampled
    val n = if (sampledCounts == null) 0 else sampledCounts.length
    require(n <= sampledHashes.length)
    Iterator.tabulate(n)(i => sampledCounts(i) * scale) ++
      Iterator.fill(sampledHashes.length - n)(scale)
  }

  override private[sql] def onTaskEnd(): Unit = this.synchronized {
    val start = System.nanoTime
    compress()
    sketchMap = null
    debugStrMap = null
    counting = false
    countMap = null
    samplingRNG = null
    samplingArray = null
    accumulationDuration += System.nanoTime - start
    taskDuration = System.nanoTime - taskStart
  }
}
