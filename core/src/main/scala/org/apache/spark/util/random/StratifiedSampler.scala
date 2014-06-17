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

import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, Map}
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.util.random.{PoissonBounds => PB}
import scala.Some
import org.apache.spark.rdd.RDD

private[spark] object StratifiedSampler extends Logging {
  def getSeqOp[K, V](withReplacement: Boolean,
      fractionByKey: (K => Double),
      counts: Option[Map[K, Long]]): ((TaskContext, Result[K]),(K, V)) => Result[K] = {
    val delta = 5e-5
    (U: (TaskContext, Result[K]), item: (K, V)) => {
      val result = U._2
      val tc = U._1
      val rng = result.getRand(tc.partitionId)
      val fraction = fractionByKey(item._1)
      val stratum = result.getEntry(item._1)
      if (withReplacement) {
        // compute q1 and q2 only if they haven't been computed already
        // since they don't change from iteration to iteration.
        // TODO change this to the streaming version
        if (stratum.q1.isEmpty || stratum.q2.isEmpty) {
          val n = counts.get(item._1)
          val s = math.ceil(n * fraction).toLong
          val lmbd1 = PB.getLambda1(s)
          val minCount = PB.getMinCount(lmbd1)
          val lmbd2 = if (lmbd1 == 0) PB.getLambda2(s) else PB.getLambda2(s - minCount)
          val q1 = lmbd1 / n
          val q2 = lmbd2 / n
          stratum.q1 = Some(q1)
          stratum.q2 = Some(q2)
        }
        val x1 = if (stratum.q1.get == 0) 0L else rng.nextPoisson(stratum.q1.get)
        if (x1 > 0) {
          stratum.incrNumAccepted(x1)
        }
        val x2 = rng.nextPoisson(stratum.q2.get).toInt
        if (x2 > 0) {
          stratum.addToWaitList(ArrayBuffer.fill(x2)(rng.nextUniform(0.0, 1.0)))
        }
      } else {
        val g1 = - math.log(delta) / stratum.numItems
        val g2 = (2.0 / 3.0) * g1
        val q1 = math.max(0, fraction + g2 - math.sqrt((g2 * g2 + 3 * g2 * fraction)))
        val q2 = math.min(1, fraction + g1 + math.sqrt(g1 * g1 + 2 * g1 * fraction))

        val x = rng.nextUniform(0.0, 1.0)
        if (x < q1) {
          stratum.incrNumAccepted()
        } else if (x < q2) {
          stratum.addToWaitList(x)
        }
        stratum.q1 = Some(q1)
        stratum.q2 = Some(q2)
      }
      stratum.incrNumItems()
      result
    }
  }

  def getCombOp[K](): (Result[K], Result[K])  => Result[K] = {
    (r1: Result[K], r2: Result[K]) => {
      // take union of both key sets in case one partition doesn't contain all keys
      val keyUnion = r1.resultMap.keys.toSet.union(r2.resultMap.keys.toSet)

      // Use r2 to keep the combined result since r1 is usual empty
      for (key <- keyUnion) {
        val entry1 = r1.resultMap.get(key)
        val entry2 = r2.resultMap.get(key)
        if (entry2.isEmpty && entry1.isDefined) {
          r2.resultMap += (key -> entry1.get)
        } else if (entry1.isDefined && entry2.isDefined) {
          entry2.get.addToWaitList(entry1.get.waitList)
          entry2.get.incrNumAccepted(entry1.get.numAccepted)
          entry2.get.incrNumItems(entry1.get.numItems)
        }
      }
      r2
    }
  }

  def computeThresholdByKey[K](finalResult: Map[K, Stratum], fractionByKey: (K => Double)):
    (K => Double) = {
    val thresholdByKey = new mutable.HashMap[K, Double]()
    for ((key, stratum) <- finalResult) {
      val fraction = fractionByKey(key)
      val s = math.ceil(stratum.numItems * fraction).toLong
      if (stratum.numAccepted > s) {
        logWarning("Pre-accepted too many")
        thresholdByKey += (key -> stratum.q1.get)
      } else {
        val numWaitListAccepted = (s - stratum.numAccepted).toInt
        if (numWaitListAccepted >= stratum.waitList.size) {
          logWarning("WaitList too short")
          thresholdByKey += (key -> stratum.q2.get)
        } else {
          thresholdByKey += (key -> stratum.waitList.sorted.apply(numWaitListAccepted))
        }
      }
    }
    thresholdByKey
  }

  def computeThresholdByKey[K](finalResult: Map[K, String]): (K => String) = {
    finalResult
  }

  def getBernoulliSamplingFunction[K, V](rdd:RDD[(K,  V)],
      fractionByKey: K => Double,
      exact: Boolean,
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    var samplingRateByKey = fractionByKey
    if (exact) {
      // determine threshold for each stratum and resample
      val seqOp = StratifiedSampler.getSeqOp[K,V](false, fractionByKey, None)
      val combOp = StratifiedSampler.getCombOp[K]()
      val zeroU = new Result[K](Map[K, Stratum](), seed = seed)
      val finalResult = rdd.aggregateWithContext(zeroU)(seqOp, combOp).resultMap
      samplingRateByKey = StratifiedSampler.computeThresholdByKey(finalResult, fractionByKey)
    }
    (idx: Int, iter: Iterator[(K, V)]) => {
      val random = new RandomDataGenerator
      random.reSeed(seed + idx)
      iter.filter(t => random.nextUniform(0.0, 1.0) < samplingRateByKey(t._1))
    }
  }

  def getPoissonSamplingFunction[K, V](rdd:RDD[(K,  V)],
      fractionByKey: K => Double,
      exact: Boolean,
      counts: Option[Map[K, Long]],
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    // TODO implement the streaming version of sampling w/ replacement that doesn't require counts
    if (exact) {
      val seqOp = StratifiedSampler.getSeqOp[K,V](true, fractionByKey, counts)
      val combOp = StratifiedSampler.getCombOp[K]()
      val zeroU = new Result[K](Map[K, Stratum](), seed = seed)
      val finalResult = rdd.aggregateWithContext(zeroU)(seqOp, combOp).resultMap
      val thresholdByKey = StratifiedSampler.computeThresholdByKey(finalResult, fractionByKey)
      (idx: Int, iter: Iterator[(K, V)]) => {
        val random = new RandomDataGenerator()
        random.reSeed(seed + idx)
        iter.flatMap { t =>
          val q1 = finalResult.get(t._1).get.q1.getOrElse(0.0)
          val q2 = finalResult.get(t._1).get.q2.getOrElse(0.0)
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
          val count = random.nextPoisson(fractionByKey(t._1)).toInt
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

private[random] class Stratum(var numItems: Long = 0L, var numAccepted: Long = 0L)
  extends Serializable {

  var waitList: ArrayBuffer[Double] = new ArrayBuffer[Double]
  var q1: Option[Double] = None // upper bound for accepting item instantly
  var q2: Option[Double] = None // upper bound for adding item to waitlist

  def incrNumItems(by: Long = 1L) = numItems += by

  def incrNumAccepted(by: Long = 1L) = numAccepted += by

  def addToWaitList(elem: Double) = waitList += elem

  def addToWaitList(elems: ArrayBuffer[Double]) = waitList ++= elems

  override def toString() = {
    "numItems: " + numItems + " numAccepted: " + numAccepted + " q1: " + q1 + " q2: " + q2 +
      " waitListSize:" + waitList.size
  }
}

private[random] class Result[K](var resultMap: Map[K, Stratum],
                var cachedPartitionId: Option[Int] = None,
                val seed: Long)
  extends Serializable {

  var rand: RandomDataGenerator = new RandomDataGenerator

  def getEntry(key: K, numItems: Long = 0L): Stratum = {
    if (!resultMap.contains(key)) {
      resultMap += (key -> new Stratum(numItems))
    }
    resultMap(key)
  }

  def getRand(partitionId: Int): RandomDataGenerator = {
    if (cachedPartitionId.isEmpty || cachedPartitionId.get != partitionId) {
      cachedPartitionId = Some(partitionId)
      rand.reSeed(seed + partitionId)
    }
    rand
  }
}
