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

package org.apache.spark.util.random

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, Map => MMap}
import scala.reflect.ClassTag

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.{Logging, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Auxiliary functions and data structures for the sampleByKey method in PairRDDFunctions.
 *
 * For more theoretical background on the sampling technqiues used here, please refer to
 * http://jmlr.org/proceedings/papers/v28/meng13a.html
 */
private[spark] object StratifiedSampler extends Logging {

  /**
   * A version of {@link #aggregate()} that passes the TaskContext to the function that does
   * aggregation for each partition. This function avoids creating an extra depth in the RDD
   * lineage, as opposed to using mapPartitionsWithIndex, which results in slightly improved
   * run time.
   */
  def aggregateWithContext[U: ClassTag, T: ClassTag](zeroValue: U)
      (rdd: RDD[T],
       seqOp: ((TaskContext, U), T) => U,
       combOp: (U, U) => U): U = {
    val sc: SparkContext = rdd.sparkContext
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    // pad seqOp and combOp with taskContext to conform to aggregate's signature in TraversableOnce
    val paddedSeqOp = (arg1: (TaskContext, U), item: T) => (arg1._1, seqOp(arg1, item))
    val paddedCombOp = (arg1: (TaskContext, U), arg2: (TaskContext, U)) =>
      (arg1._1, combOp(arg1._2, arg1._2))
    val cleanSeqOp = sc.clean(paddedSeqOp)
    val cleanCombOp = sc.clean(paddedCombOp)
    val aggregatePartition = (tc: TaskContext, it: Iterator[T]) =>
      (it.aggregate(tc, zeroValue)(cleanSeqOp, cleanCombOp))._2
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(rdd, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Returns the function used by aggregate to collect sampling statistics for each partition.
   */
  def getSeqOp[K, V](withReplacement: Boolean,
      fractions: Map[K, Double],
      counts: Option[Map[K, Long]]): ((TaskContext, Result[K]), (K, V)) => Result[K] = {
    val delta = 5e-5
    (output: (TaskContext, Result[K]), item: (K, V)) => {
      val result = output._2
      val tc = output._1
      val rng = result.getRand(tc.partitionId)
      val fraction = fractions(item._1)
      val stratum = result.getEntry(item._1)
      if (withReplacement) {
        // compute acceptBound and waitListBound only if they haven't been computed already
        // since they don't change from iteration to iteration.
        // TODO change this to the streaming version
        if (stratum.areBoundsEmpty) {
          val n = counts.get(item._1)
          val sampleSize = math.ceil(n * fraction).toLong
          val lmbd1 = PoissonBounds.getLowerBound(sampleSize)
          val minCount = PoissonBounds.getMinCount(lmbd1)
          val lmbd2 = if (lmbd1 == 0) {
            PoissonBounds.getUpperBound(sampleSize)
          } else {
            PoissonBounds.getUpperBound(sampleSize - minCount)
          }
          stratum.acceptBound = lmbd1 / n
          stratum.waitListBound = lmbd2 / n
        }
        val x1 = if (stratum.acceptBound == 0.0) 0L else rng.nextPoisson(stratum.acceptBound)
        if (x1 > 0) {
          stratum.incrNumAccepted(x1)
        }
        val x2 = rng.nextPoisson(stratum.waitListBound).toInt
        if (x2 > 0) {
          stratum.addToWaitList(ArrayBuffer.fill(x2)(rng.nextUniform(0.0, 1.0)))
        }
      } else {
        // We use the streaming version of the algorithm for sampling without replacement to avoid
        // using an extra pass over the RDD for computing the count.
        // Hence, acceptBound and waitListBound change on every iteration.
        val g1 = - math.log(delta) / stratum.numItems // gamma1
        val g2 = (2.0 / 3.0) * g1 // gamma 2
        stratum.acceptBound = math.max(0, fraction + g2 - math.sqrt((g2 * g2 + 3 * g2 * fraction)))
        stratum.waitListBound = math.min(1, fraction + g1 + math.sqrt(g1 * g1 + 2 * g1 * fraction))

        val x = rng.nextUniform(0.0, 1.0)
        if (x < stratum.acceptBound) {
          stratum.incrNumAccepted()
        } else if (x < stratum.waitListBound) {
          stratum.addToWaitList(x)
        }
      }
      stratum.incrNumItems()
      result
    }
  }

  /**
   * Returns the function used by aggregate to combine results from different partitions, as
   * returned by seqOp.
   */
  def getCombOp[K](): (Result[K], Result[K]) => Result[K] = {
    (r1: Result[K], r2: Result[K]) => {
      // take union of both key sets in case one partition doesn't contain all keys
      val keyUnion = r1.resultMap.keySet.union(r2.resultMap.keySet)

      // Use r2 to keep the combined result since r1 is usual empty
      for (key <- keyUnion) {
        val entry1 = r1.resultMap.get(key)
        if (r2.resultMap.contains(key)) {
          r2.resultMap(key).merge(entry1)
        } else {
          r2.addEntry(key, entry1)
        }
      }
      r2
    }
  }

  /**
   * Given the result returned by the aggregate function, determine the threshold for accepting
   * items to generate exact sample size.
   *
   * To do so, we compute sampleSize = math.ceil(size * samplingRate) for each stratum and compare
   * it to the number of items that were accepted instantly and the number of items in the waitlist
   * for that stratum. Most of the time, numAccepted <= sampleSize <= (numAccepted + numWaitlisted),
   * which means we need to sort the elements in the waitlist by their associated values in order
   * to find the value T s.t. |{elements in the stratum whose associated values <= T}| = sampleSize.
   * Note that all elements in the waitlist have values >= bound for instant accept, so a T value
   * in the waitlist range would allow all elements that were instantly accepted on the first pass
   * to be included in the sample.
   */
  def computeThresholdByKey[K](finalResult: Map[K, Stratum],
      fractions: Map[K, Double]):
    Map[K, Double] = {
    val thresholdByKey = new HashMap[K, Double]()
    for ((key, stratum) <- finalResult) {
      val s = math.ceil(stratum.numItems * fractions(key)).toLong
      if (stratum.numAccepted > s) {
        logWarning("Pre-accepted too many")
        thresholdByKey += (key -> stratum.acceptBound)
      } else {
        val numWaitListAccepted = (s - stratum.numAccepted).toInt
        if (numWaitListAccepted >= stratum.waitList.size) {
          logWarning("WaitList too short")
          thresholdByKey += (key -> stratum.waitListBound)
        } else {
          thresholdByKey += (key -> stratum.waitList.sorted.apply(numWaitListAccepted))
        }
      }
    }
    thresholdByKey
  }

  /**
   * Return the per partition sampling function used for sampling without replacement.
   *
   * When exact sample size is required, we make an additional pass over the RDD to determine the
   * exact sampling rate that guarantees sample size with high confidence.
   *
   * The sampling function has a unique seed per partition.
   */
  def getBernoulliSamplingFunction[K, V](rdd: RDD[(K,  V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    var samplingRateByKey = fractions
    if (exact) {
      // determine threshold for each stratum and resample
      val seqOp = getSeqOp[K, V](false, fractions, None)
      val combOp = getCombOp[K]()
      val zeroU = new Result[K](new HashMap[K, Stratum](), seed = seed)
      val finalResult = aggregateWithContext(zeroU)(rdd, seqOp, combOp).resultMap
      samplingRateByKey = computeThresholdByKey(finalResult, fractions)
    }
    (idx: Int, iter: Iterator[(K, V)]) => {
      val random = new RandomDataGenerator
      random.reSeed(seed + idx)
      iter.filter(t => random.nextUniform(0.0, 1.0) < samplingRateByKey(t._1))
    }
  }

  /**
   * Return the per partition sampling function used for sampling with replacement.
   *
   * When exact sample size is required, we make two additional passed over the RDD to determine
   * the exact sampling rate that guarantees sample size with high confidence. The first pass
   * counts the number of items in each stratum (group of items with the same key) in the RDD, and
   * the second pass uses the counts to determine exact sampling rates.
   *
   * The sampling function has a unique seed per partition.
   */
  def getPoissonSamplingFunction[K, V](rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      counts: Option[Map[K, Long]],
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    // TODO implement the streaming version of sampling w/ replacement that doesn't require counts
    if (exact) {
      val seqOp = getSeqOp[K, V](true, fractions, counts)
      val combOp = getCombOp[K]()
      val zeroU = new Result[K](new HashMap[K, Stratum](), seed=seed)
      val finalResult = aggregateWithContext(zeroU)(rdd, seqOp, combOp).resultMap
      val thresholdByKey = computeThresholdByKey(finalResult, fractions)
      (idx: Int, iter: Iterator[(K, V)]) => {
        val random = new RandomDataGenerator()
        random.reSeed(seed + idx)
        iter.flatMap { t =>
          val q1 = finalResult(t._1).acceptBound
          val q2 = finalResult(t._1).waitListBound
          val x1 = if (q1 == 0) 0L else random.nextPoisson(q1)
          val x2 = random.nextPoisson(q2).toInt
          val x = x1 + (0 until x2).count(i => random.nextUniform(0.0, 1.0) < thresholdByKey(t._1))
          if (x > 0) {
            Iterator.fill(x.toInt)(t)
          } else {
            Iterator.empty
          }
        }
      }
    } else {
      (idx: Int, iter: Iterator[(K, V)]) => {
        val random = new RandomDataGenerator()
        random.reSeed(seed + idx)
        iter.flatMap { t =>
          val count = random.nextPoisson(fractions(t._1)).toInt
          if (count > 0) {
            Iterator.fill(count)(t)
          } else {
            Iterator.empty
          }
        }
      }
    }
  }
}

/**
 * Object used by seqOp to keep track of the number of items accepted and items waitlisted per
 * stratum, as well as the bounds for accepting and waitlisting items.
 */
private class Stratum(var numItems: Long = 0L, var numAccepted: Long = 0L)
  extends Serializable {

  private val _waitList = new ArrayBuffer[Double]
  private var _acceptBound: Option[Double] = None // upper bound for accepting item instantly
  private var _waitListBound: Option[Double] = None // upper bound for adding item to waitlist

  def incrNumItems(by: Long = 1L) = numItems += by

  def incrNumAccepted(by: Long = 1L) = numAccepted += by

  def addToWaitList(elem: Double) = _waitList += elem

  def addToWaitList(elems: ArrayBuffer[Double]) = _waitList ++= elems

  def waitList = _waitList

  def acceptBound = _acceptBound.getOrElse(0.0)

  def acceptBound_= (value: Double) = _acceptBound = Some(value)

  def waitListBound = _waitListBound.getOrElse(0.0)

  def waitListBound_= (value: Double) = _waitListBound = Some(value)

  def areBoundsEmpty = _acceptBound.isEmpty || _waitListBound.isEmpty

  def merge(other: Option[Stratum]): Unit = {
    if (other.isDefined) {
      addToWaitList(other.get.waitList)
      incrNumAccepted(other.get.numAccepted)
      incrNumItems(other.get.numItems)
    }
  }
}

/**
 * Object used by seqOp and combOp to keep track of the sampling statistics for all strata.
 *
 * When used by seqOp for each partition, we also keep track of the partition ID in this object
 * to make sure a single random number generator with a unique seed is used for each partition.
 */
private[random] class Result[K](val resultMap: MMap[K, Stratum],
    var cachedPartitionId: Option[Int] = None,
    val seed: Long)
  extends Serializable {

  var rand = new RandomDataGenerator

  def getEntry(key: K, numItems: Long = 0L): Stratum = {
    if (!resultMap.contains(key)) {
      resultMap += (key -> new Stratum(numItems))
    }
    resultMap(key)
  }

  def addEntry(key: K, entry: Option[Stratum]): Unit = {
    if (entry.isDefined) {
      resultMap += (key -> entry.get)
    }
  }

  def getRand(partitionId: Int): RandomDataGenerator = {
    if (cachedPartitionId.isEmpty || cachedPartitionId.get != partitionId) {
      cachedPartitionId = Some(partitionId)
      rand.reSeed(seed + partitionId)
    }
    rand
  }
}
