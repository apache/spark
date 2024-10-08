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

package org.apache.spark.ml.recommendation

import java.util.Random

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{ACCURACY, EPR}
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.recommendation.logistic.local.{ItemData, Optimizer, Opts}
import org.apache.spark.ml.recommendation.logistic.pair.LongPairMulti
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class LMFSuite extends MLTest with DefaultReadWriteTest with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setCheckpointDir(tempDir.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("LMF validate input dataset") {
    import testImplicits._

    withClue("Valid Integer Ids") {
      val df = sc.parallelize(Seq(
        (123, 1),
        (111, 2)
      )).toDF("item", "user")
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Valid Long Ids") {
      val df = sc.parallelize(Seq(
        (1231L, 12L),
        (1112L, 21L)
      )).toDF("item", "user")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        new LMF().setMaxIter(1).fit(df)
      }
    }

    withClue("Valid Double Ids") {
      val df = sc.parallelize(Seq(
        (123.0, 12.0),
        (111.0, 21.0)
      )).toDF("item", "user")
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Valid Decimal Ids") {
      val df = sc.parallelize(Seq(
          (1231L, 12L),
          (1112L, 21L)
        )).toDF("item", "user")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user"))
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Invalid Double: fractional part") {
      val df = sc.parallelize(Seq(
        (123.1, 12.0),
        (111.0, 21.0)
      )).toDF("item", "user")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF only supports non-Null values"))
    }

    withClue("Invalid Decimal: fractional part") {
      val df = sc.parallelize(Seq(
          (123.1, 12L),
          (1112.0, 21L)
        )).toDF("item", "user")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user")
        )
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF only supports non-Null values"))
    }

    withClue("Invalid Type") {
      val df = sc.parallelize(Seq(
        ("123.0", 12.0),
        ("111", 21.0)
      )).toDF("item", "user")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Column item must be of type numeric"))
    }

    withClue("Valid implicit with weights") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 1.0),
        (1112L, 21L, 2.0)
      )).toDF("item", "user", "weight")
      new LMF().setWeightCol("weight").setMaxIter(1).fit(df)
    }

    withClue("Invalid implicit with weights") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, -1f),
        (1112L, 21L, 2f)
      )).toDF("item", "user", "weight")
      val e = intercept[Exception] { new LMF().setWeightCol("weight").setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Weights MUST NOT be Negative or Infinity"))
    }

    withClue("Invalid implicit with labels") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 1f),
        (1112L, 21L, 0f)
      )).toDF("item", "user", "label")
      val e = intercept[Exception] { new LMF().setLabelCol("label").setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF does not support the labelCol in implicitPrefs mode."))
    }

    withClue("Valid explicit labels") {
      val df = sc.parallelize(Seq(
        (123L, 321L, 0.0),
        (1234L, 4321L, 1.0)
      )).toDF("item", "user", "label")
      new LMF().setLabelCol("label").setImplicitPrefs(false).setMaxIter(1).fit(df)
    }

    withClue("Invalid explicit labels") {
      val df = sc.parallelize(Seq(
        (123L, 321L, 0.5),
        (1234L, 4321L, 1.0)
      )).toDF("item", "user", "label")
      val e = intercept[Exception] { new LMF().setLabelCol("label")
        .setImplicitPrefs(false).setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Labels MUST be in {0, 1}"))
    }

    withClue("Invalid explicit without labels") {
      val df = sc.parallelize(Seq(
        (123L, 321L),
        (1234L, 4321L)
      )).toDF("item", "user")
      val e = intercept[Exception] { new LMF()
        .setImplicitPrefs(false).setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("The labelCol must be set in explicit mode."))
    }
  }

  test("LMF optimizer explicit") {
    val random = new Random(239)
    val dim = 5
    val (trueUserFactors, trueItemFactors, trainData, testData) =
      LMFSuite.genData(4096, 32, 16, dim,
        implicitPrefs = false, useBias = true, random = random)

    val opts = Opts.explicitOpts(dim, useBias = true, 0.025f, 1f, 0.01f, verbose = false)
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

    (0 until 100).foreach{_ =>
      optimizer.optimize(Iterator(batch), 1, remapInplace = false);
    }

    val userFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_LEFT)
      .map(e => e.id -> e.f).toArray

    val itemFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_RIGHT)
      .map(e => e.id -> e.f).toArray

    val trueAcc = LMFSuite.accuracy(testData, useBias = true,
      trueUserFactors, trueItemFactors)
    val acc = LMFSuite.accuracy(testData, opts.useBias, userFactors, itemFactors)


    logInfo(log"True test accuracy is ${MDC(ACCURACY, trueAcc)}.")
    logInfo(log"Actual test accuracy is ${MDC(ACCURACY, acc)}.")
    assert(acc > 0.6)
  }

  test("LMF optimizer implicit") {
    val random = new Random(239)
    val dim = 5
    val (trueUserFactors, trueItemFactors, trainData, testData) =
      LMFSuite.genData(4096, 32, 16, dim,
        implicitPrefs = true, useBias = true, random = random)

    val opts = Opts.implicitOpts(dim, useBias = true, 10, 0f, 0.025f, 1f, 0.01f, 0.1f,
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

    (0 until 100).foreach{_ =>
      optimizer.optimize(Iterator(batch), 1, remapInplace = false);
    }

    val userFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_LEFT)
      .map(e => e.id -> e.f).toArray

    val itemFactors = optimizer.flush()
      .filter(_.t == ItemData.TYPE_RIGHT)
      .map(e => e.id -> e.f).toArray

    val trueEpr = LMFSuite.epr(testData, useBias = true,
      trueUserFactors, trueItemFactors)
    val epr = LMFSuite.epr(testData, opts.useBias, userFactors, itemFactors)

    logInfo(log"True test epr is ${MDC(EPR, trueEpr)}.")
    logInfo(log"Actual test epr is ${MDC(EPR, epr)}.")
    assert(epr > 0.65)
  }

}

object LMFSuite extends Logging {

  private def genFactors(
                          size: Int,
                          rank: Int,
                          random: Random): Array[(Long, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    Array.fill(size)(random.nextLong()).zip(
      Array.fill(size)(Array.fill(rank)(random.nextFloat() * 2 - 1)))
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

  private def epr(data: Iterable[(Long, Long, Float, Float)],
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
        .filter(_._1 == item2i(i))
        .head._2.toDouble / itemFactors.length * w
    }.sum / data.map(_._4).sum
  }

  private def accuracy(data: Iterable[(Long, Long, Float, Float)],
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

  private def genData(
                           numUsers: Int,
                           numItems: Int,
                           numSamples: Int,
                           rank: Int,
                           useBias: Boolean,
                           implicitPrefs: Boolean,
                           random: Random) = {

    val userFactors = genFactors(numUsers, if (useBias) rank + 1 else rank, random)
    val itemFactors = genFactors(numItems, if (useBias) rank + 1 else rank, random)

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
