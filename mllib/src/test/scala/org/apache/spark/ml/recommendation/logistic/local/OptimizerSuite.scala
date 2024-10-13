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

package org.apache.spark.ml.recommendation.logistic.local

import java.util.Random
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{ACCURACY, EPR}
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.recommendation.logistic.pair.LongPairMulti
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}


class OptimizerSuite extends MLTest with DefaultReadWriteTest with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setCheckpointDir(tempDir.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("ParItr") {
    val random = new Random(238)
    val data = Array.fill(10000)(random.nextLong())
    val trueSum = data.sum

    withClue("Valid") {
      val accum = new AtomicLong(0L)
      ParItr.foreach(data.iterator, 5, (x: Long) => accum.addAndGet(x))
      assert(accum.get() == trueSum)
    }

    withClue("Invalid: / by zero exception") {
      val e = intercept[Exception] {
        val accum = new AtomicLong(0L)
        ParItr.foreach(data.iterator, 5, (x: Long) => accum.addAndGet(x / 0))
      }
      assert(e.getMessage != null)
      assert(e.getMessage.contains("/ by zero"))
    }

  }

  test("Optimizer explicit") {
    val random = new Random(239)
    val useBias = true
    val dim = 5
    val (trueUserFactors, trueItemFactors, trainData, testData) =
      OptimizerSuite.genData(4096, 32, 16, dim,
        useBias, implicitPrefs = false, random = random)

    val opts = Opts.explicitOpts(dim, useBias, 0.025f, 1f, 0.001f, verbose = false)
    val userCounts = trainData.groupMapReduce(_._1)(_ => 1L)(_ + _)
    val itemCounts = trainData.groupMapReduce(_._2)(_ => 1L)(_ + _)

    val optimizer = Optimizer(opts,
      trueUserFactors.map{case (i, f) => new ItemData(ItemData.TYPE_LEFT,
        i, userCounts.getOrElse(i, 0L),
        Optimizer.initEmbedding(opts.dim, opts.useBias, random))}.iterator ++
        trueItemFactors.map{case (i, f) => new ItemData(ItemData.TYPE_RIGHT,
          i, itemCounts.getOrElse(i, 0L),
          Optimizer.initEmbedding(opts.dim, opts.useBias, random))}.iterator)

    val batch = LongPairMulti(0, trainData.map(_._1), trainData.map(_._2),
      trainData.map(_._3), trainData.map(_._4))

    val rndUserFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_LEFT)
      .map(e => e.id -> e.f).toArray

    val rndItemFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_RIGHT)
      .map(e => e.id -> e.f).toArray

    (0 until 100).foreach{_ =>
      optimizer.optimize(Iterator(batch), 1, remapInplace = false);
    }

    val userFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_LEFT)
      .map(e => e.id -> e.f).toArray

    val itemFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_RIGHT)
      .map(e => e.id -> e.f).toArray

    val trueAcc = OptimizerSuite.accuracy(testData, useBias,
      trueUserFactors, trueItemFactors)

    val rndAcc = OptimizerSuite.accuracy(testData, opts.useBias, rndUserFactors, rndItemFactors)
    val acc = OptimizerSuite.accuracy(testData, opts.useBias, userFactors, itemFactors)

    logInfo(log"True test accuracy is ${MDC(ACCURACY, trueAcc)}.")
    logInfo(log"Random test accuracy is ${MDC(ACCURACY, rndAcc)}.")
    logInfo(log"Actual test accuracy is ${MDC(ACCURACY, acc)}.")

    assert(0.78 < trueAcc && trueAcc < 0.82)
    assert(0.48 < rndAcc && rndAcc < 0.52)
    assert(0.68 < acc && acc < 0.72)
  }

  test("Optimizer implicit") {
    val random = new Random(240)
    val dim = 5
    val useBias = true
    val (trueUserFactors, trueItemFactors, trainData, testData) =
      OptimizerSuite.genData(4096, 32, 16, dim,
        useBias, implicitPrefs = true, random = random)

    val opts = Opts.implicitOpts(dim, useBias, 10, 0f, 0.025f, 1f, 0.001f, 0.1f,
      verbose = false)
    val userCounts = trainData.groupMapReduce(_._1)(_ => 1L)(_ + _)
    val itemCounts = trainData.groupMapReduce(_._2)(_ => 1L)(_ + _)

    val optimizer = Optimizer(opts,
      trueUserFactors.map{case (i, f) => new ItemData(ItemData.TYPE_LEFT,
        i, userCounts.getOrElse(i, 0L),
        Optimizer.initEmbedding(opts.dim, opts.useBias, random))}.iterator ++
        trueItemFactors.map{case (i, f) => new ItemData(ItemData.TYPE_RIGHT,
          i, itemCounts.getOrElse(i, 0L),
          Optimizer.initEmbedding(opts.dim, opts.useBias, random))}.iterator)

    val batch = LongPairMulti(0, trainData.map(_._1), trainData.map(_._2),
      null, trainData.map(_._4))

    val rndUserFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_LEFT)
      .map(e => e.id -> e.f).toArray

    val rndItemFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_RIGHT)
      .map(e => e.id -> e.f).toArray

    (0 until 100).foreach{_ =>
      optimizer.optimize(Iterator(batch), 1, remapInplace = false);
    }

    val userFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_LEFT)
      .map(e => e.id -> e.f).toArray

    val itemFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_RIGHT)
      .map(e => e.id -> e.f).toArray

    val trueEpr = OptimizerSuite.epr(testData, useBias,
      trueUserFactors, trueItemFactors)
    val rndEpr = OptimizerSuite.epr(testData, opts.useBias, rndUserFactors, rndItemFactors)
    val epr = OptimizerSuite.epr(testData, opts.useBias, userFactors, itemFactors)

    logInfo(log"True test epr is ${MDC(EPR, trueEpr)}.")
    logInfo(log"Random test epr is ${MDC(EPR, rndEpr)}.")
    logInfo(log"Actual test epr is ${MDC(EPR, epr)}.")

    assert(0.87 < trueEpr && trueEpr < 0.91)
    assert(0.48 < rndEpr && rndEpr < 0.52)
    assert(0.84 < epr && epr < 0.88)
  }

}

object OptimizerSuite extends Logging {

  private def genFactors(
                          size: Int,
                          rank: Int,
                          useBias: Boolean,
                          stddev: Double,
                          random: Random): Array[(Long, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    val n = if (useBias) rank + 1 else rank
    Array.fill(size)(random.nextLong()).zip(
      Array.fill(size)((0 until n).map{i =>
        if (i < rank) random.nextGaussian() * stddev
        else random.nextDouble() * 2 - 1
      }.map(_.toFloat).toArray))
  }

  private def sample(weights: Array[Double], random: Random): Int = {
    val sum = weights.sum
    val t = random.nextDouble()
    var i = 0
    var s = 0.0
    while (s <= sum * t) {
      s += weights(i)
      i += 1
    }

    i - 1
  }

  private def getLogits(userFactors: Array[(Long, Array[Float])],
                        itemFactors: Array[(Long, Array[Float])],
                        useBias: Boolean) = {
    val r = Array.fill(userFactors.size)(Array.fill(itemFactors.size)(0f))
    userFactors.indices
      .foreach(i => itemFactors.indices
        .foreach{j =>
          val n = if (!useBias) userFactors(i)._2.length else userFactors(i)._2.length - 1
          var e = BLAS.nativeBLAS.sdot(n,
            userFactors(i)._2, 1, itemFactors(j)._2, 1)

          if (useBias) {
            e += userFactors(i)._2(n) + itemFactors(j)._2(n)
          }
          r(i)(j) = e
        })
    r
  }

  private def sigmoid(x: Double): Double = {
    1 / (1 + Math.exp(-x))
  }

  private[recommendation] def epr(data: Iterable[(Long, Long, Float, Float)],
                                  useBias: Boolean,
                                  userFactors: Array[(Long, Array[Float])],
                                  itemFactors: Array[(Long, Array[Float])]) = {

    val user2i = userFactors.map(_._1).zipWithIndex.toMap
    val item2i = itemFactors.map(_._1).zipWithIndex.toMap

    val logits = getLogits(userFactors, itemFactors, useBias)
    data.map{case (u, i, _, w) =>
      itemFactors.indices
        .sortBy(logits(user2i(u)))
        .zipWithIndex
        .find(_._1 == item2i.getOrElse(i, -1))
        .map(_._2).getOrElse(0)
        .toDouble / itemFactors.length * w
    }.sum / data.map(_._4).sum
  }

  private[recommendation] def accuracy(data: Iterable[(Long, Long, Float, Float)],
                                       useBias: Boolean,
                                       userFactors: Array[(Long, Array[Float])],
                                       itemFactors: Array[(Long, Array[Float])]) = {

    val user2i = userFactors.map(_._1).zipWithIndex.toMap
    val item2i = itemFactors.map(_._1).zipWithIndex.toMap

    val logits = getLogits(userFactors, itemFactors, useBias)
    data.map{case (u, i, l, w) =>
      val e = logits(user2i(u))(item2i(i))
      if (e >= 0 && l > 0 || e < 0 && l == 0) {
        1.0 * w
      } else {
        0.0
      }
    }.sum / data.map(_._4).sum
  }

  private[recommendation] def genData(
                                       numUsers: Int,
                                       numItems: Int,
                                       numSamples: Int,
                                       rank: Int,
                                       useBias: Boolean,
                                       implicitPrefs: Boolean,
                                       random: Random) = {

    val userFactors = genFactors(numUsers, rank, useBias, 1, random)
    val itemFactors = genFactors(numItems, rank, useBias, 1, random)

    val logits = getLogits(userFactors, itemFactors, useBias)
    val trainData = ArrayBuffer.empty[(Long, Long, Float, Float)]
    val testData = ArrayBuffer.empty[(Long, Long, Float, Float)]

    (0 until numUsers).foreach{i =>
      val n = numSamples
      val denom = logits(i).map(Math.exp(_)).sum
      val softmax = logits(i).map(Math.exp(_)).map(_ / denom)

      (0 until n)
        .foreach{_ =>
          val entry = if (implicitPrefs) {
            (userFactors(i)._1, itemFactors(sample(softmax, random))._1, 1f, 1f)
          } else {
            val j = random.nextInt(numItems)
            (userFactors(i)._1, itemFactors(j)._1,
              {if (random.nextDouble() < sigmoid(logits(i)(j))) 1f else 0f}, 1f)
          }

          if (random.nextDouble() < 0.8) {
            trainData += entry
          } else {
            testData += entry
          }
        }
    }

    (userFactors, itemFactors,
      trainData.groupMapReduce(e => (e._1, e._2, e._3))(_._4)(_ + _)
        .map(e => (e._1._1, e._1._2, e._1._3, e._2)).toArray,
      testData.groupMapReduce(e => (e._1, e._2, e._3))(_._4)(_ + _)
        .map(e => (e._1._1, e._1._2, e._1._3, e._2)).toArray)
  }
}
