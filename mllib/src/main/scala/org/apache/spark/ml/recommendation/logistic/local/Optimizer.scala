/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import java.util
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.Nullable

import scala.collection.mutable.ArrayBuffer

import com.google.common.util.concurrent.AtomicDouble
import dev.ludovic.netlib.blas.{BLAS => NetlibBLAS}

import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.recommendation.logistic.pair.LongPairMulti
import org.apache.spark.util.collection.OpenHashMap

private[logistic] object Optimizer {
  private val UNIGRAM_TABLE_SIZE = 100000000

  private object ExpTable {
    private val EXP_TABLE_SIZE = 1000
    private val MAX_EXP = 6
    private val INSTANCE = apply()

    private def apply(): ExpTable = {
      val expTable = new ExpTable(
        Array.fill(EXP_TABLE_SIZE)(0f),
        Array.fill(EXP_TABLE_SIZE)(0f),
        Array.fill(EXP_TABLE_SIZE)(0f))

      var i = 0
      while (i < ExpTable.EXP_TABLE_SIZE) {
        val tmp = Math.exp((2.0 * i / ExpTable.EXP_TABLE_SIZE - 1.0) * ExpTable.MAX_EXP)
        expTable.sigm(i) = (tmp / (tmp + 1.0)).toFloat
        expTable.loss0(i) = Math.log(1 - expTable.sigm(i)).toFloat
        expTable.loss1(i) = Math.log(expTable.sigm(i)).toFloat
        i += 1
      }

      expTable
    }

    def getInstance: Optimizer.ExpTable = INSTANCE
  }

  private class ExpTable(private val sigm: Array[Float],
                         private val loss0: Array[Float],
                         private val loss1: Array[Float]) {

    def sigmoid(f: Float): Float = {
      if (f > ExpTable.MAX_EXP) {
        1.0f
      } else if (f < -ExpTable.MAX_EXP) {
        0.0f
      } else {
        val ind = ((f + ExpTable.MAX_EXP) *
          (ExpTable.EXP_TABLE_SIZE / ExpTable.MAX_EXP / 2.0)).toInt
        this.sigm(ind)
      }
    }

    def logloss(f: Float, label: Float): Float = {
      if (f > ExpTable.MAX_EXP) {
        if (label > 0) 0f else 6.00247569f
      } else if (f < -ExpTable.MAX_EXP) {
        if (label > 0) 6.00247569f else 0f
      } else {
        val ind = ((f + ExpTable.MAX_EXP) *
          (ExpTable.EXP_TABLE_SIZE / ExpTable.MAX_EXP / 2.0)).toInt
        -(if (label > 0) this.loss1(ind) else this.loss0(ind))
      }
    }
  }

  private def initUnigramTable(cn: Array[Long], pow: Double): Array[Int] = {
    val table = Array.fill(UNIGRAM_TABLE_SIZE)(0)
    val n = cn.length

    var a = 0
    var trainWordsPow = 0.0

    while (a < n) {
      trainWordsPow += Math.pow(cn(a), pow)
      a += 1
    }

    var i = 0
    a = 0
    var d1 = Math.pow(cn(i), pow) / trainWordsPow

    while (a < table.length && i < n) {
      table(a) = i
      if (a > d1 * table.length) {
        i += 1
        d1 += Math.pow(cn(i), pow) / trainWordsPow
      }
      a += 1
    }

    table
  }

  def initEmbedding(dim: Int, useBias: Boolean, rnd: java.util.Random): Array[Float] = {
    val f = Array.fill(if (useBias) dim + 1 else dim)(0f)
    (0 until dim).foreach{f(_) = (rnd.nextFloat - 0.5f) / dim}
    f
  }

  private def shuffle(l: Array[Long],
                      r: Array[Long],
                      @Nullable w: Array[Float],
                      rnd: java.util.Random): Unit = {
    var i = 0
    val n = l.length
    var t = 0L
    var t1 = 0f

    while (i < n - 1) {
      val j = i + rnd.nextInt(n - i)
      t = l(j)
      l(j) = l(i)
      l(i) = t

      t = r(j)
      r(j) = r(i)
      r(i) = t

      if (w != null) {
        t1 = w(j)
        w(j) = w(i)
        w(i) = t1
      }

      i += 1
    }
  }


  def apply(opts: Opts, dataIter: Iterator[ItemData]): Optimizer = {

    val vocabL = new OpenHashMap[Long, Int]()
    val vocabR = new OpenHashMap[Long, Int]()

    var rawCnL = ArrayBuffer.empty[Long]
    var rawCnR = ArrayBuffer.empty[Long]

    var rawSyn0 = ArrayBuffer.empty[Float]
    var rawSyn1neg = ArrayBuffer.empty[Float]

    while (dataIter.hasNext) {
      val itemData = dataIter.next()

      if (itemData.t == ItemData.TYPE_LEFT) {
        val i = vocabL.size
        vocabL.update(itemData.id, i)
        rawCnL += itemData.cn
        rawSyn0 ++= itemData.f
      } else {
        val i = vocabR.size
        vocabR.update(itemData.id, i)
        rawCnR += itemData.cn
        rawSyn1neg ++= itemData.f
      }
    }

    val cnL = rawCnL.toArray
    rawCnL = null

    val cnR = rawCnR.toArray
    rawCnR = null

    var i2R = null.asInstanceOf[Array[Long]]
    var unigramTable = null.asInstanceOf[Array[Int]]

    if (opts.implicitPref) {
      i2R = Array.fill(vocabR.size)(0L)
      vocabR.iterator.foreach{case (k, i) => i2R(i) = k}

      if (opts.pow > 0) {
        unigramTable = initUnigramTable(cnR, opts.pow)
      }
    }

    val syn0 = rawSyn0.toArray
    rawSyn0 = null
    val syn1neg = rawSyn1neg.toArray
    rawSyn1neg = null

    new Optimizer(opts, vocabL, vocabR, i2R, cnL, cnR, syn0, syn1neg, unigramTable)
  }
}

private[logistic] class Optimizer(private val opts: Opts,
                                  private var vocabL: OpenHashMap[Long, Int],
                                  private var vocabR: OpenHashMap[Long, Int],
                                  private var i2R: Array[Long],
                                  private var cnL: Array[Long],
                                  private var cnR: Array[Long],
                                  private var syn0: Array[Float],
                                  private var syn1neg: Array[Float],
                                  private var unigramTable: Array[Int]) {

  private val blas: NetlibBLAS = BLAS.nativeBLAS
  private val random: ThreadLocalRandom = ThreadLocalRandom.current()

  val loss: AtomicDouble = new AtomicDouble(0)
  val lossReg: AtomicDouble = new AtomicDouble(0)
  val lossn: AtomicLong = new AtomicLong(0)
  val lossnReg: AtomicLong = new AtomicLong(0)

  private def optimizeImplicitBatchRemapped(l: Array[Long],
                                            r: Array[Long],
                                            @Nullable w: Array[Float]): Unit = {
    assert(l.length == r.length)
    Optimizer.shuffle(l, r, w, random)

    var lloss = 0.0
    var llossReg = 0.0
    var llossn = 0L
    var llossnReg = 0L

    var pos = 0
    var word = -1
    var lastWord = -1

    val neu1e = Array.fill(opts.vectorSize)(0f)
    val expTable = Optimizer.ExpTable.getInstance

    while (pos < l.length) {
      lastWord = l(pos).toInt
      word = r(pos).toInt

      if (word != -1 && lastWord != -1) {
        val l1 = lastWord * opts.vectorSize
        util.Arrays.fill(neu1e, 0)
        var target = -1
        var label = 0
        var weight = 0f
        var d = 0

        while (d < opts.negative + 1) {
          if (d == 0) {
            target = word
            label = 1
            weight = if (w == null) 1f else w(pos)
          } else {
            if (unigramTable != null) {
              target = unigramTable(random.nextInt(unigramTable.length))
              while (target == -1 || l(pos) == i2R(target)) {
                target = unigramTable(random.nextInt(unigramTable.length))
              }
            } else {
              target = random.nextInt(vocabR.size)
              while (l(pos) == i2R(target)) {
                target = random.nextInt(vocabR.size)
              }
            }
            weight = opts.gamma
            label = 0
          }
          val l2 = target * opts.vectorSize
          var f = blas.sdot(opts.dim, syn0, l1, 1, syn1neg, l2, 1)
          if (opts.useBias) {
            f += syn0(l1 + opts.dim)
            f += syn1neg(l2 + opts.dim)
          }

          val sigm = expTable.sigmoid(f)
          lloss += expTable.logloss(f, label) * weight
          llossn += 1

          val g = ((label - sigm) * opts.lr * weight).toFloat

          if (opts.lambda > 0 && label > 0) {
            llossReg += opts.lambda * blas.sdot(opts.dim, syn0, l1, 1, syn0, l1, 1)
            llossReg += opts.lambda * blas.sdot(opts.dim, syn1neg, l2, 1, syn1neg, l2, 1)
            llossnReg += 1
          }

          if (opts.lambda > 0 && label > 0) {
            blas.saxpy(opts.dim, (-opts.lambda * opts.lr).toFloat, syn0, l1, 1, neu1e, 0, 1)
          }
          blas.saxpy(opts.dim, g, syn1neg, l2, 1, neu1e, 0, 1)
          if (opts.useBias) {
            neu1e(opts.dim) += g * 1
          }

          if (opts.lambda > 0 && label > 0) {
            blas.saxpy(opts.dim, (-opts.lambda * opts.lr).toFloat, syn1neg, l2, 1, syn1neg, l2, 1)
          }
          blas.saxpy(opts.dim, g, syn0, l1, 1, syn1neg, l2, 1)
          if (opts.useBias) {
            syn1neg(l2 + opts.dim) += g * 1
          }
          d += 1
        }
        blas.saxpy(opts.vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
      }
      pos += 1
    }


    loss.addAndGet(lloss)
    lossReg.addAndGet(llossReg)
    lossn.addAndGet(llossn)
    lossnReg.addAndGet(llossnReg)
  }

  private def optimizeExplicitBatchRemapped(l: Array[Long],
                                            r: Array[Long],
                                            w: Array[Float]): Unit = {
    assert(l.length == r.length)
    Optimizer.shuffle(l, r, w, random)

    var lloss = 0.0
    var llossReg = 0.0
    var llossn = 0L
    var llossnReg = 0L

    var pos = 0
    var word = -1
    var lastWord = -1

    val neu1e = Array.fill(opts.vectorSize)(0f)
    val expTable = Optimizer.ExpTable.getInstance

    while (pos < l.length) {
      lastWord = l(pos).toInt
      word = r(pos).toInt

      if (word != -1 && lastWord != -1) {
        val l1 = lastWord * opts.vectorSize
        val l2 = word * opts.vectorSize

        util.Arrays.fill(neu1e, 0)
        val label = w(pos)
        val weight = 1f
        assert(label == 0f || label == 1f)

        var f = blas.sdot(opts.dim, syn0, l1, 1, syn1neg, l2, 1)
        if (opts.useBias) {
          f += syn0(l1 + opts.dim)
          f += syn1neg(l2 + opts.dim)
        }

        val sigm = expTable.sigmoid(f)
        lloss += expTable.logloss(f, label) * weight
        llossn += 1

        val g = ((label - sigm) * opts.lr * weight).toFloat

        if (opts.lambda > 0 && label > 0) {
          llossReg += opts.lambda * blas.sdot(opts.dim, syn0, l1, 1, syn0, l1, 1)
          llossReg += opts.lambda * blas.sdot(opts.dim, syn1neg, l2, 1, syn1neg, l2, 1)
          llossnReg += 1
        }

        if (opts.lambda > 0 && label > 0) {
          blas.saxpy(opts.dim, (-opts.lambda * opts.lr).toFloat, syn0, l1, 1, neu1e, 0, 1)
        }
        blas.saxpy(opts.dim, g, syn1neg, l2, 1, neu1e, 0, 1)
        if (opts.useBias) {
          neu1e(opts.dim) += g * 1
        }

        if (opts.lambda > 0 && label > 0) {
          blas.saxpy(opts.dim, (-opts.lambda * opts.lr).toFloat, syn1neg, l2, 1, syn1neg, l2, 1)
        }
        blas.saxpy(opts.dim, g, syn0, l1, 1, syn1neg, l2, 1)
        if (opts.useBias) {
          syn1neg(l2 + opts.dim) += g * 1
        }
        blas.saxpy(opts.vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
      }
      pos += 1
    }

    loss.addAndGet(lloss)
    lossReg.addAndGet(llossReg)
    lossn.addAndGet(llossn)
    lossnReg.addAndGet(llossnReg)
  }

  private def remap(pair: LongPairMulti): LongPairMulti = {
    pair.left.indices.foreach{i =>
      pair.left(i) = vocabL.get(pair.left(i)).getOrElse(-1)
      pair.right(i) = vocabR.get(pair.right(i)).getOrElse(-1)
    }

    pair
  }

  def optimize(data: Iterator[LongPairMulti], cpus: Int): Unit = {
    ParItr.foreach(data.map(remap), cpus, if (opts.implicitPref) {
      pair: LongPairMulti => optimizeImplicitBatchRemapped(pair.left, pair.right, pair.rating)
    } else {
      pair: LongPairMulti => optimizeExplicitBatchRemapped(pair.left, pair.right, pair.rating)
    })
  }

  def flush(): Iterator[ItemData] = {
    vocabL.iterator.map{case (id, i) =>
      new ItemData(ItemData.TYPE_LEFT, id, cnL(i),
        util.Arrays.copyOfRange(syn0, opts.vectorSize * i, opts.vectorSize * (i + 1)))
    } ++ vocabR.iterator.map{case (id, i) =>
      new ItemData(ItemData.TYPE_RIGHT, id, cnR(i),
        util.Arrays.copyOfRange(syn1neg, opts.vectorSize * i, opts.vectorSize * (i + 1)))
    }
  }

}
