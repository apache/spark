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
package org.apache.spark.mllib.feature

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.BLAS.{nativeBLAS => blas}
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashMap


private object ParItr {

  def foreach[A](i: Iterator[A], cpus: Int, batch: Int)(f: A => Unit): Unit = {
    val support = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(cpus))
    i.grouped(batch).foreach{arr =>
      val parr = arr.par
      parr.tasksupport = support
      parr.foreach(f)
    }
  }

  def map[A, B](i: Iterator[A], cpus: Int, batch: Int)(f: A => B): Iterator[B] = {
    val support = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(cpus))
    i.grouped(batch).flatMap{arr =>
      val parr = arr.par
      parr.tasksupport = support
      parr.map(f)
    }
  }
}

private class CyclicBuffer(size: Int) {
  val arr = Array.fill(size)(-1)
  val ind = Array.fill(size)(-1)
  var version = -1

  var ptr = 0
  var iter = -1

  def push(v: Int, i: Int): Unit = {
    ptr = (ptr - 1 + size) % size
    arr(ptr) = v
    ind(ptr) = i
  }

  def reset(): Unit = {
    ptr = 0
    iter = -1
    ind(ptr) = -1
    arr(ptr) = -1
  }

  def resetIter(): Unit = {
    iter = ptr
  }

  def headInd(): Int = {
    if (iter == -1) {
      -1
    } else {
      ind(iter)
    }
  }

  def headVal(): Int = {
    if (iter == -1) {
      -1
    } else {
      arr(iter)
    }
  }

  def next(): Unit = {
    if (iter == (ptr - 1 + size) % size) {
      iter = -1
    } else {
      iter = (iter + 1) % size
    }
  }

  def checkVersion(v: Int): Unit = {
    if (v != version) {
      version = v
      reset()
    }
  }
}

private[spark] object SkipGram {

  def getPartition(i: Int, salt: Int, nPart: Int): Int = {
    var h = (salt.toLong << 32) | i
    h ^= h >>> 33
    h *= 0xff51afd7ed558ccdL
    h ^= h >>> 33
    h *= 0xc4ceb9fe1a85ec53L
    h ^= h >>> 33
    (Math.abs(h) % nPart).toInt
  }

  private def skip(n: Long,
                   sample: Double,
                   random: Random,
                   trainWordsCount: Long): Boolean = {
    if (sample > 0 && n > 0) {
      val ran = Math.sqrt(n / (sample * trainWordsCount) + 1) *
        (sample * trainWordsCount) / n
      if (ran < random.nextFloat()) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  private def pairs(sent: RDD[Array[Int]],
                    salt: Int,
                    countBC: Broadcast[OpenHashMap[Int, Long]],
                    sample: Double,
                    trainWordsCount: Long,
                    window: Int,
                    numPartitions: Int,
                    numThread: Int): RDD[(Int, (Array[Int], Array[Int]))] = {
    sent.mapPartitions { it =>
      ParItr.map(it.grouped(numPartitions * numPartitions), numThread, numThread) { sG =>
        val l = Array.fill(numPartitions)(ArrayBuffer.empty[Int])
        val r = Array.fill(numPartitions)(ArrayBuffer.empty[Int])
        val buffers = Array.fill(numPartitions)(new CyclicBuffer(window))
        val count = countBC.value
        var v = 0
        val random = new java.util.Random()
        var seed = salt
        sG.foreach { s =>
          var i = 0
          var skipped = 0
          random.setSeed(seed)
          while (i < s.length) {
            val cn = count(s(i))
            if (skip(cn, sample, random, trainWordsCount)) {
              skipped += 1
            } else if (cn > 0) {
              val a = getPartition(s(i), salt, numPartitions)
              val buffer = buffers(a)
              buffer.checkVersion(v)
              buffer.resetIter()
              while (buffer.headInd() != -1 && buffer.headInd() >= (i - skipped) - window) {
                val v = buffer.headVal()
                l(a).append(s(i))
                r(a).append(v)

                l(a).append(v)
                r(a).append(s(i))
                buffer.next()
              }
              buffers(a).push(s(i), i - skipped)
            }
            seed += s(i)
            i += 1
          }
          v += 1
        }
        (0 until numPartitions).map(i => i -> (l(i).toArray, r(i).toArray))
      }.flatten
    }
  }
}

@Since("3.4.0")
class SkipGram extends Serializable with Logging {
  val EXP_TABLE_SIZE = 1000
  val MAX_EXP = 6
  val UNIGRAM_TABLE_SIZE = 100000000

  private var vectorSize: Int = 100
  private var window: Int = 5
  private var negative: Int = 5
  private var numIterations: Int = 1
  private var learningRate: Double = 0.025
  private var minCount: Int = 1
  private var numThread: Int = 1
  private var numPartitions: Int = 1
  private var sample: Double = 0
  private var pow: Double = 0
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /**
   * Sets vector size (default: 100).
   */
  @Since("3.4.0")
  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.vectorSize = vectorSize
    this
  }

  /**
   * Sets initial learning rate (default: 0.025).
   */
  @Since("3.4.0")
  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate
    this
  }


  /**
   * Sets number of partitions (default: 1). Use a small number for accuracy.
   */
  @Since("3.4.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  /**
   * Sets number of iterations (default: 1), which should be smaller than or equal to number of
   * partitions.
   */
  @Since("3.4.0")
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations >= 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  /**
   * Sets the window of words (default: 5)
   */
  @Since("3.4.0")
  def setWindowSize(window: Int): this.type = {
    require(window > 0,
      s"Window of words must be positive but got ${window}")
    this.window = window
    this
  }

  /**
   * Sets minCount, the minimum number of times a token must appear to be included in the word2vec
   * model's vocabulary (default: 5).
   */
  @Since("3.4.0")
  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"Minimum number of times must be nonnegative but got ${minCount}")
    this.minCount = minCount
    this
  }

  /**
   * Sets negative, the number of negative samples (default: 5)
   */
  @Since("3.4.0")
  def setNegative(negative: Int): this.type = {
    require(negative >= 0,
      s"Number of negative samples ${negative}")
    this.negative = negative
    this
  }

  /**
   * Sets numThreads, the number of threads (default: 5)
   */
  @Since("3.4.0")
  def setNumThread(numThread: Int): this.type = {
    require(numThread >= 0,
      s"Number of threads ${numThread}")
    this.numThread = numThread
    this
  }

  /**
   * Sets storage level for intermediate RDDs. The default value is
   * `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g., `MEMORY_AND_DISK_SER` and
   * set `spark.rdd.compress` to `true` to reduce the space requirement, at the cost of speed.
   */
  @Since("3.4.0")
  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(storageLevel != StorageLevel.NONE,
      "SkipGram is not designed to run without persisting intermediate RDDs.")
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  /**
   * Sets storage level for final RDDs. The default
   * value is `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g.
   * `MEMORY_AND_DISK_SER` and set `spark.rdd.compress` to `true` to reduce the space requirement,
   * at the cost of speed.
   */
  @Since("3.4.0")
  def setFinalRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(finalRDDStorageLevel != StorageLevel.NONE,
      "SkipGram is not designed to run without persisting intermediate RDDs.")
    this.finalRDDStorageLevel = storageLevel
    this
  }

  private def cacheAndCount[T](rdd: RDD[T]): RDD[T] = {
    val r = rdd.persist(intermediateRDDStorageLevel)
    r.count()
    r
  }

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  private def initUnigramTable(cn: ArrayBuffer[Long]): Array[Int] = {
    var a = 0
    val power = 0.75
    var trainWordsPow = 0.0
    val table = Array.fill(UNIGRAM_TABLE_SIZE)(-1)
    while (a < cn.length) {
      trainWordsPow += Math.pow(cn(a), power)
      a += 1
    }
    var i = 0
    var d1 = Math.pow(cn(a), power) / trainWordsPow
    a = 0
    while (a < table.length && i < cn.length) {
      table(a) = i
      if (a.toDouble / table.length > d1) {
        i += 1
        d1 += Math.pow(cn(i), power) / trainWordsPow
      }
      a += 1
    }
    table
  }

  /**
   * Computes the vector representation of each word in vocabulary.
   * @param dataset an RDD of sentences,
   *                each sentence is expressed as an iterable collection of words
   * @return a Word2VecModel
   */
  @Since("3.4.0")
  def fit(dataset: RDD[Array[Int]]): Unit = {
    val sc = dataset.context
    val expTable = sc.broadcast(createExpTable())
    try {
      doFit(dataset, sc, expTable)
    } finally {
      expTable.destroy()
    }
  }

  private def doFit(dataset: RDD[Array[Int]],
                    sc: SparkContext,
                    expTable: Broadcast[Array[Float]]): Unit = {
    import SkipGram._

    val sent = cacheAndCount(dataset)
    val countRDD = cacheAndCount(sent.flatMap(identity(_)).map(_ -> 1L)
      .reduceByKey(_ + _).filter(_._2 >= minCount))

    var trainWordsCount = 0L
    val countBC = {
      val count = new OpenHashMap[Int, Long]()
      countRDD.toLocalIterator.foreach { case (v, n) =>
        count.update(v, n)
        trainWordsCount += n
      }
      sc.broadcast(count)
    }

    var emb = countRDD.mapPartitions{it =>
      val rnd = new Random(0)
      it.map{case (v, n) =>
        rnd.setSeed(v)
        v -> (n, Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat),
          Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat))
      }
    }
    cacheAndCount(emb)

    (0 until numPartitions * numIterations).foreach{pI =>
      val partitioner1 = new HashPartitioner(numPartitions) {
        override def getPartition(key: Any): Int = {
          SkipGram.getPartition(key.asInstanceOf[Int], pI, numPartitions)
        }
      }
      val partitioner2 = new HashPartitioner(numPartitions) {
        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }
      emb = emb.partitionBy(partitioner1)
      val cur = pairs(sent, pI, countBC, sample, trainWordsCount, window,
        numPartitions, numThread).partitionBy(partitioner2).values

      val newEmb = cacheAndCount(cur.zipPartitions(emb) {case (sIt, eIt) =>
        val vocab = new OpenHashMap[Int, Int]()
        val rSyn0 = ArrayBuffer.empty[Array[Float]]
        val rSyn1Neg = ArrayBuffer.empty[Array[Float]]
        var seed = 0
        val cn = ArrayBuffer.empty[Long]

        eIt.foreach{case (v, (n, f1, f2)) =>
          val i = vocab.size
          vocab.update(v, i)
          cn.append(n)
          rSyn0 += f1;
          rSyn1Neg += f2
          seed = seed * 239017 + v
        }

        val table = if (pow > 0) {
          initUnigramTable(cn)
        } else {
          Array.empty[Int]
        }

        val syn0 = Array.fill(rSyn0.length * vectorSize)(0f)
        rSyn0.iterator.zipWithIndex.foreach{case (f, i) =>
          System.arraycopy(f, 0, syn0, i * vectorSize, vectorSize)
        }
        rSyn0.clear()

        val syn1Neg = Array.fill(rSyn1Neg.length * vectorSize)(0f)
        rSyn1Neg.iterator.zipWithIndex.foreach{case (f, i) =>
          System.arraycopy(f, 0, syn1Neg, i * vectorSize, vectorSize)
        }
        rSyn1Neg.clear()
        val lExpTable = expTable.value
        val random = new java.util.Random(seed)

        ParItr.foreach(sIt.zipWithIndex, numThread, 100000)({case ((l, r), ii) =>
          var pos = 0
          while (pos < l.length) {
            if (vocab.contains(l(pos)) && vocab.contains(r(pos))) {
              val word = vocab(l(pos))
              val lastWord = vocab(r(pos))
              val l1 = lastWord * vectorSize
              val neu1e = new Array[Float](vectorSize)
              var target = -1
              var label = -1
              var d = 0
              while (d < negative + 1) {
                if (d == 0) {
                  target = word
                  label = 1
                } else {
                  if (pow > 0) {
                    target = table(random.nextInt(table.length))
                    while (target == word || target == -1) {
                      target = table(random.nextInt(table.length))
                    }
                  } else {
                    target = random.nextInt(vocab.size)
                    while (target == word) {
                      target = random.nextInt(vocab.size)
                    }
                  }
                  label = 0
                }
                val l2 = target * vectorSize
                val f = blas.sdot(vectorSize, syn0, l1, 1, syn1Neg, l2, 1)
                var g = 0.0
                if (f > MAX_EXP) {
                  g = (label - 1) * learningRate
                } else if (f < -MAX_EXP) {
                  g = (label - 0) * learningRate
                } else {
                  val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
                  g = (label - lExpTable(ind)) * learningRate
                }
                blas.saxpy(vectorSize, g.toFloat, syn1Neg, l2, 1, neu1e, 0, 1)
                blas.saxpy(vectorSize, g.toFloat, syn0, l1, 1, syn1Neg, l2, 1)
                d += 1
              }
              blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
            }
            pos += 1
          }
        })

        vocab.iterator.map{case (k, v) =>
          k -> (cn(v), syn0.slice(v * vectorSize, (v + 1) * vectorSize),
            syn1Neg.slice(v * vectorSize, (v + 1) * vectorSize))
        }
      })
      emb.unpersist()
      emb = newEmb
    }

    sent.unpersist()
    new SkipGramModel(emb.map(x => x._1 -> x._2._2))
  }
}

/**
 * SkipGramModel model
 * @param emb array of length numWords * vectorSize, vector corresponding
 *                    to the word mapped with index i can be retrieved by the slice
 *                    (i * vectorSize, i * vectorSize + vectorSize)
 */
@Since("1.1.0")
class SkipGramModel private[spark] (
    private[spark] val emb: RDD[(Int, Array[Float])]) extends Serializable with Saveable {
  /**
   * Save this model to the given path.
   *
   * This saves:
   *  - human-readable (JSON) model metadata to path/metadata/
   *  - Parquet formatted data to path/data/
   *
   * The model may be loaded using `Loader.load`.
   *
   * @param sc   Spark context used to save model data.
   * @param path Path specifying the directory in which to save this model.
   *             If the directory already exists, this method throws an exception.
   */
  override def save(sc: SparkContext, path: String): Unit = {
    emb.map(x => x._1 + " " + x._2.mkString(" ")).saveAsTextFile(path)
  }
}