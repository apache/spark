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
/* #if scala-2.13
import scala.collection.parallel.CollectionConverters._
#endif scala-2.13 */
import scala.collection.parallel.ForkJoinTaskSupport

import collection.JavaConverters._
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.linalg.BLAS.{nativeBLAS => blas}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashMap



object ParItr {

  def grouped(inIt: Iterator[Array[Int]], maxLen: Int): Iterator[Array[Array[Int]]] = {
    val it = inIt.buffered
    new Iterator[Array[Array[Int]]] {
      val data = ArrayBuffer.empty[Array[Int]]

      override def hasNext: Boolean = it.hasNext

      override def next(): Array[Array[Int]] = {
        var sum = 0
        data.clear()
        while (it.hasNext && (sum + it.head.length < maxLen || data.isEmpty)) {
          data.append(it.head)
          sum += it.head.length
          it.next()
        }
        data.toArray
      }
    }
  }


  def foreach[A](i: Iterator[A], cpus: Int)(f: A => Unit): Unit = {
    val support = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(cpus))
    val parr = (0 until cpus).toArray.par
    parr.tasksupport = support
    parr.foreach{_ =>
      var x: A = null.asInstanceOf[A]

      while (synchronized{
        if (i.hasNext) {
          x = i.next()
          true
        } else {
          x = null.asInstanceOf[A]
          false
        }
      }) {
        f(x)
      }
    }
  }

  def map[A, B](i: Iterator[A], cpus: Int, batch: Int)(f: A => B): Iterator[B] = {
    val support = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(cpus))
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

  private def shuffle(l: ArrayBuffer[Int], r: ArrayBuffer[Int], rnd: java.util.Random): Unit = {
    var i = 0
    val n = l.length
    var t = 0
    while (i < n - 1) {
      val j = i + rnd.nextInt(n - i)
      t = l(j)
      l(j) = l(i)
      l(i) = t

      t = r(j)
      r(j) = r(i)
      r(i) = t

      i += 1
    }
  }

  private def pairs(sent: RDD[Array[Int]],
                    salt: Int,
                    sampleProbBC: Broadcast[Int2IntOpenHashMap],
                    window: Int,
                    numPartitions: Int,
                    numThread: Int): RDD[(Int, (Array[Int], Array[Int]))] = {
    sent.mapPartitions { it =>
      ParItr.map(ParItr.grouped(it, 1000000), numThread, numThread) { sG =>
        val l = Array.fill(numPartitions)(ArrayBuffer.empty[Int])
        val r = Array.fill(numPartitions)(ArrayBuffer.empty[Int])
        val buffers = Array.fill(numPartitions)(new CyclicBuffer(window))
        val sampleProb = sampleProbBC.value
        var v = 0
        val random = new java.util.Random()
        var seed = salt
        sG.foreach { s =>
          var i = 0
          var skipped = 0
          random.setSeed(seed)
          while (i < s.length) {
            val prob = if (sampleProb.size() > 0) {
              sampleProb.getOrDefault(s(i), Int.MaxValue)
            } else {
              Int.MaxValue
            }

            if (prob < Int.MaxValue && prob < random.nextInt()) {
              skipped += 1
            } else {
              val a = getPartition(s(i), salt, numPartitions)
              val buffer = buffers(a)
              buffer.checkVersion(v)
              buffer.resetIter()
              while (buffer.headInd() != -1 && buffer.headInd() >= (i - skipped) - window) {
                val w = buffer.headVal()
                if (s(i) != w) {
                  l(a).append(s(i))
                  r(a).append(w)
                }

                buffer.next()
              }
              buffers(a).push(s(i), i - skipped)
            }
            seed = seed * 239017 + s(i)
            i += 1
          }
          v += 1
        }
        (0 until numPartitions).map{i =>
          shuffle(l(i), r(i), random)
          i -> (l(i).toArray, r(i).toArray)
        }
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
   * Sets the negative sampling word frequency power (default: 0)
   */
  @Since("3.4.0")
  def setPow(pow: Double): this.type = {
    require(pow >= 0,
      s"Pow must be positive but got ${pow}")
    this.pow = pow
    this
  }

  /**
   * Sets the frequent word subsample ratio (default: 0)
   */
  @Since("3.4.0")
  def setSample(sample: Double): this.type = {
    require(sample >= 0 && sample <= 1,
      s"sample must be between 0 and 1 but got $sample")
    this.sample = sample
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
    var trainWordsPow = 0.0
    val table = Array.fill(UNIGRAM_TABLE_SIZE)(-1)
    while (a < cn.length) {
      trainWordsPow += Math.pow(cn(a), pow)
      a += 1
    }
    var i = 0
    a = 0
    var d1 = Math.pow(cn(i), pow) / trainWordsPow
    while (a < table.length && i < cn.length) {
      table(a) = i
      if (a.toDouble / table.length > d1) {
        i += 1
        d1 += Math.pow(cn(i), pow) / trainWordsPow
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
  def fit(dataset: RDD[Array[String]]): SkipGramModel = {
    val sc = dataset.context

    val countRDD = cacheAndCount(dataset.flatMap(identity(_)).map(_ -> 1L)
      .reduceByKey(_ + _).filter(_._2 >= minCount))

    val (vocabBC, sampleProbBC, invVocab) = {
      val map = new OpenHashMap[String, Int]()
      val sampleProb = new Int2IntOpenHashMap()
      val invVocab = ArrayBuffer.empty[String]

      val trainWordsCount = countRDD.map(_._2).reduce(_ + _)
      countRDD.map{ case (v, n) =>
        val prob = if (sample > 0) {
          (Math.sqrt(n / (sample * trainWordsCount)) + 1) * (sample * trainWordsCount) / n
        } else {
          1.0
        }
        (v, if (prob >= 1) Int.MaxValue else (prob * Int.MaxValue).toInt)
      }.collect().foreach{case (w, p) =>
        val i = map.size
        map.update(w, i)
        invVocab.append(w)
        if (p < Int.MaxValue) {
          sampleProb.put(i, p)
        }
      }
      (sc.broadcast(map), sc.broadcast(sampleProb), invVocab.toArray)
    }

    val sent = cacheAndCount(dataset.mapPartitions{it =>
      val vocab = vocabBC.value
      it.map(_.filter(vocab.contains).map(vocab(_)))
    }.repartition(numPartitions * 5))
    val expTable = sc.broadcast(createExpTable())

    val emb = cacheAndCount(countRDD.mapPartitions{it =>
      val rnd = new Random(0)
      val vocab = vocabBC.value
      it.map{case (w, n) =>
        val v = vocab(w)
        rnd.setSeed(v)
        v -> (n, Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat),
          Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat))
      }
    })

    try {
      val result = doFit(sent, emb, sampleProbBC, sc, expTable)
      val invVocabBC = sc.broadcast(invVocab)
      new SkipGramModel(result.map(x => invVocabBC.value(x._1) -> x._2._2))
    } finally {
      // expTable.destroy()
      // sampleProbBC.destroy()
      sent.unpersist()
      countRDD.unpersist()
    }
  }

  private def doFit(sent: RDD[Array[Int]],
                    inEmb: RDD[(Int, (Long, Array[Float], Array[Float]))],
                    sampleProbBC: Broadcast[Int2IntOpenHashMap],
                    sc: SparkContext,
                    expTable: Broadcast[Array[Float]]
                   ): RDD[(Int, (Long, Array[Float], Array[Float]))] = {
    import SkipGram._

    var emb = inEmb

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
      val cur = pairs(sent, pI, sampleProbBC, window,
        numPartitions, numThread).partitionBy(partitioner2).values

      val newEmb = cacheAndCount(cur.zipPartitions(emb) {case (sIt, eIt) =>
        val vocab = new Int2IntOpenHashMap()
        val rSyn0 = ArrayBuffer.empty[Array[Float]]
        val rSyn1Neg = ArrayBuffer.empty[Array[Float]]
        var seed = 0
        val cn = ArrayBuffer.empty[Long]

        eIt.foreach{case (v, (n, f1, f2)) =>
          val i = vocab.size
          vocab.put(v, i)
          cn.append(n)
          rSyn0 += f1
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

        ParItr.foreach(sIt, numThread)({case ((l, r)) =>
          var pos = 0
          while (pos < l.length * 2) {
            var word = vocab.getOrDefault(l(pos / 2), -1)
            var lastWord = vocab.getOrDefault(r(pos / 2), -1)
            if (pos % 2 == 1) {
              val t = word
              word = lastWord
              lastWord = t
            }
            if (word != -1 && lastWord != -1) {
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

        vocab.int2IntEntrySet().fastIterator().asScala.map{e =>
          val k = e.getIntKey
          val v = e.getIntValue
          k -> (cn(v), syn0.slice(v * vectorSize, (v + 1) * vectorSize),
            syn1Neg.slice(v * vectorSize, (v + 1) * vectorSize))
        }
      })
      emb.unpersist()
      emb = newEmb
    }

    sent.unpersist()
    emb
  }
}

/**
 * SkipGramModel model
 * @param emb array of length numWords * vectorSize, vector corresponding
 *                    to the word mapped with index i can be retrieved by the slice
 *                    (i * vectorSize, i * vectorSize + vectorSize)
 */
@Since("3.4.0")
class SkipGramModel private[spark] (
    private[spark] val emb: RDD[(String, Array[Float])]
                                   ) extends Serializable with Saveable {
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

  @Since("3.4.0")
  def save(sc: SparkContext, path: String): Unit = {
    SkipGramModel.SaveLoadV1_0.save(sc, path, emb)
  }

  /**
   * Find synonyms of a word; do not include the word itself in results.
   * @param word a word
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
   */
  @Since("3.4.0")
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val f = emb.filter(_._1 == word).collect()
    if (f.isEmpty) {
      throw new IllegalStateException(s"$word not in vocabulary")
    }
    findSynonyms(f.head._2, num)
  }

  /**
   * Find synonyms of a word; do not include the word itself in results.
   * @param vector word embedding
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
   */
  @Since("3.4.0")
  def findSynonyms(vector: Array[Float], num: Int): Array[(String, Double)] = {
    val vecNorm = BLAS.nativeBLAS.snrm2(vector.length, vector, 1)
    if (vecNorm == 0) {
      Array.empty[(String, Double)]
    } else {
      BLAS.nativeBLAS.sscal(vector.length, 1f / vecNorm, vector, 0, 1)
      emb.map{case (w, f) =>
        val wNorm = BLAS.nativeBLAS.snrm2(f.length, f, 1)
        if (wNorm > 0) {
          BLAS.nativeBLAS.sscal(f.length, 1f / wNorm, f, 0, 1)
          w -> blas.sdot(f.length, f, 0, 1, vector, 0, 1).toDouble
        } else {
          w -> -1.0
        }
      }.takeOrdered(num)(Ordering.by(_._2))
    }
  }

  def getVectors: RDD[(String, Array[Float])] = emb
}

@Since("3.4.0")
object SkipGramModel extends Loader[SkipGramModel] {
  private object SaveLoadV1_0 {

    val formatVersionV1_0 = "1.0"

    val classNameV1_0 = "org.apache.spark.mllib.feature.SkipGramModel"

    case class Data(word: String, embedding: Array[Float])

    def load(sc: SparkContext, path: String): SkipGramModel = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      import spark.sqlContext.implicits._
      val dataFrame = spark.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Data](dataFrame.schema)
      new SkipGramModel(dataFrame.select("word", "embedding")
        .as[(String, Array[Float])].rdd)
    }

    def save(sc: SparkContext, path: String,
             emb: RDD[(String, Array[Float])]): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      import spark.sqlContext.implicits._
      val vectorSize = emb.first()._2.length
      val numWords = emb.count()
      val metadata = compact(render(
        ("class" -> classNameV1_0) ~ ("version" -> formatVersionV1_0) ~
          ("vectorSize" -> vectorSize) ~ ("numWords" -> numWords)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      emb.map(x => Data(x._1, x._2)).toDF()
        .write.parquet(Loader.dataPath(path))
    }
  }

  @Since("3.4.0")
  override def load(sc: SparkContext, path: String): SkipGramModel = {

    val (loadedClassName, loadedVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats = DefaultFormats
    val expectedVectorSize = (metadata \ "vectorSize").extract[Int]
    val expectedNumWords = (metadata \ "numWords").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.classNameV1_0
    (loadedClassName, loadedVersion) match {
      case (classNameV1_0, "1.0") =>
        val model = SaveLoadV1_0.load(sc, path)
        val vectorSize = model.emb.first()._2.length
        val numWords = model.emb.count()
        require(expectedVectorSize == vectorSize,
          s"SkipGramModel requires each word to be mapped to a vector of size " +
            s"$expectedVectorSize, got vector of size $vectorSize")
        require(expectedNumWords == numWords,
          s"SkipGramModel requires $expectedNumWords words, but got $numWords")
        model
      case _ => throw new Exception(
        s"SkipGramModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $loadedVersion).  Supported:\n" +
          s"  ($classNameV1_0, 1.0)")
    }
  }
}
