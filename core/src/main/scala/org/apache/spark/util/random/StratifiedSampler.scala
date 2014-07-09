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

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * Auxiliary functions and data structures for the sampleByKey method in PairRDDFunctions.
 *
 * Essentially, when exact sample size is necessary, we make additional passes over the RDD to
 * compute the exact threshold value to use for each stratum to guarantee exact sample size with
 * high probability. This is achieved by maintaining a waitlist of size O(log(s)), where s is the
 * desired sample size for each stratum.
 *
 * Like in simple random sampling, we generate a random value for each item from the
 * uniform  distribution [0.0, 1.0]. All items with values <= min(values of items in the waitlist)
 * are accepted into the sample instantly. The threshold for instant accept is designed so that
 * s - numAccepted = O(log(s)), where s is again the desired sample size. Thus, by maintaining a
 * waitlist size = O(log(s)), we will be able to create a sample of the exact size s by adding
 * a portion of the waitlist to the set of items that are instantly accepted. The exact threshold
 * is computed by sorting the values in the waitlist and picking the value at (s - numAccepted).
 *
 * Note that since we use the same seed for the RNG when computing the thresholds and the actual
 * sample, our computed thresholds are guaranteed to produce the desired sample size.
 *
 * For more theoretical background on the sampling techniques used here, please refer to
 * http://jmlr.org/proceedings/papers/v28/meng13a.html
 */

private[spark] object StratifiedSampler extends Logging {

  /**
   * Count the number of items instantly accepted and generate the waitlist for each stratum.
   *
   * This is only invoked when exact sample size is required.
   */
  def getCounts[K, V](rdd: RDD[(K, V)],
      withReplacement: Boolean,
      fractions: Map[K, Double],
      counts: Option[Map[K, Long]],
      seed: Long): MMap[K, Stratum] = {
    val combOp = getCombOp[K]
    val mappedPartitionRDD = rdd.mapPartitionsWithIndex({ case (partition, iter) =>
      val zeroU: MMap[K, Stratum] = new HashMap[K, Stratum]()
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + partition)
      val seqOp = getSeqOp(withReplacement, fractions, rng, counts)
      Iterator(iter.aggregate(zeroU)(seqOp, combOp))
    }, preservesPartitioning=true)
    mappedPartitionRDD.reduce(combOp)
  }

  /**
   * Returns the function used by aggregate to collect sampling statistics for each partition.
   */
  def getSeqOp[K, V](withReplacement: Boolean,
      fractions: Map[K, Double],
      rng: RandomDataGenerator,
      counts: Option[Map[K, Long]]): (MMap[K, Stratum], (K, V)) => MMap[K, Stratum] = {
    val delta = 5e-5
    (result: MMap[K, Stratum], item: (K, V)) => {
      val key = item._1
      val fraction = fractions(key)
      if (!result.contains(key)) {
        result += (key -> new Stratum())
      }
      val stratum = result(key)

      if (withReplacement) {
        // compute acceptBound and waitListBound only if they haven't been computed already
        // since they don't change from iteration to iteration.
        // TODO change this to the streaming version
        if (stratum.areBoundsEmpty) {
          val n = counts.get(key)
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
        val acceptBound = stratum.acceptBound
        val copiesAccepted = if (acceptBound == 0.0) 0L else rng.nextPoisson(acceptBound)
        if (copiesAccepted > 0) {
          stratum.incrNumAccepted(copiesAccepted)
        }
        val copiesWaitlisted = rng.nextPoisson(stratum.waitListBound).toInt
        if (copiesWaitlisted > 0) {
          stratum.addToWaitList(ArrayBuffer.fill(copiesWaitlisted)(rng.nextUniform(0.0, 1.0)))
        }
      } else {
        // We use the streaming version of the algorithm for sampling without replacement to avoid
        // using an extra pass over the RDD for computing the count.
        // Hence, acceptBound and waitListBound change on every iteration.
        val gamma1 = - math.log(delta) / stratum.numItems
        val gamma2 = (2.0 / 3.0) * gamma1
        stratum.acceptBound = math.max(0,
          fraction + gamma2 - math.sqrt(gamma2 * gamma2 + 3 * gamma2 * fraction))
        stratum.waitListBound = math.min(1,
          fraction + gamma1 + math.sqrt(gamma1 * gamma1 + 2 * gamma1 * fraction))

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
   * Returns the function used combine results returned by seqOp from different partitions.
   */
  def getCombOp[K]: (MMap[K, Stratum], MMap[K, Stratum]) => MMap[K, Stratum] = {
    (result1: MMap[K, Stratum], result2: MMap[K, Stratum]) => {
      // take union of both key sets in case one partition doesn't contain all keys
      for (key <- result1.keySet.union(result2.keySet)) {
        // Use result2 to keep the combined result since r1 is usual empty
        val entry1 = result1.get(key)
        if (result2.contains(key)) {
          result2(key).merge(entry1)
        } else {
          if (entry1.isDefined) {
            result2 += (key -> entry1.get)
          }
        }
      }
      result2
    }
  }

  /**
   * Given the result returned by getCounts, determine the threshold for accepting items to
   * generate exact sample size.
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
      fractions: Map[K, Double]): Map[K, Double] = {
    val thresholdByKey = new HashMap[K, Double]()
    for ((key, stratum) <- finalResult) {
      val sampleSize = math.ceil(stratum.numItems * fractions(key)).toLong
      if (stratum.numAccepted > sampleSize) {
        logWarning("Pre-accepted too many")
        thresholdByKey += (key -> stratum.acceptBound)
      } else {
        val numWaitListAccepted = (sampleSize - stratum.numAccepted).toInt
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
      val finalResult = getCounts(rdd, false, fractions, None, seed)
      samplingRateByKey = computeThresholdByKey(finalResult, fractions)
    }
    (idx: Int, iter: Iterator[(K, V)]) => {
      val rng = new RandomDataGenerator
      rng.reSeed(seed + idx)
      iter.filter(t => rng.nextUniform(0.0, 1.0) < samplingRateByKey(t._1))
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
      val finalResult = getCounts(rdd, true, fractions, counts, seed)
      val thresholdByKey = computeThresholdByKey(finalResult, fractions)
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val key = item._1
          val acceptBound = finalResult(key).acceptBound
          val copiesAccepted = if (acceptBound == 0) 0L else rng.nextPoisson(acceptBound)
          val copiesWailisted = rng.nextPoisson(finalResult(key).waitListBound).toInt
          val copiesInSample = copiesAccepted +
            (0 until copiesWailisted).count(i => rng.nextUniform(0.0, 1.0) < thresholdByKey(key))
          if (copiesInSample > 0) {
            Iterator.fill(copiesInSample.toInt)(item)
          } else {
            Iterator.empty
          }
        }
      }
    } else {
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val count = rng.nextPoisson(fractions(item._1)).toInt
          if (count > 0) {
            Iterator.fill(count)(item)
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
 *
 * `[random]` here is necessary since it's in the return type signature of seqOp defined above
 */
private[random] class Stratum(var numItems: Long = 0L, var numAccepted: Long = 0L)
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
