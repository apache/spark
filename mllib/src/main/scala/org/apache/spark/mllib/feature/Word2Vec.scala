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

import java.lang.{Iterable => JavaIterable}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.common.collect.{Ordering => GuavaOrdering}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{ALPHA, COUNT, NUM_TRAIN_WORD, VOCAB_SIZE}
import org.apache.spark.internal.config.Kryo.KRYO_SERIALIZER_MAX_BUFFER_SIZE
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{Utils => CUtils}
import org.apache.spark.util.random.XORShiftRandom

/**
 *  Entry in vocabulary
 */
private case class VocabWord(
  var word: String,
  var cn: Long,
  var point: Array[Int],
  var code: Array[Int],
  var codeLen: Int
)

/**
 * Word2Vec creates vector representation of words in a text corpus.
 * The algorithm first constructs a vocabulary from the corpus
 * and then learns vector representation of words in the vocabulary.
 * The vector representation can be used as features in
 * natural language processing and machine learning algorithms.
 *
 * We used skip-gram model in our implementation and hierarchical softmax
 * method to train the model. The variable names in the implementation
 * matches the original C implementation.
 *
 * For original C implementation, see https://code.google.com/p/word2vec/
 * For research papers, see
 * Efficient Estimation of Word Representations in Vector Space
 * and
 * Distributed Representations of Words and Phrases and their Compositionality.
 */
@Since("1.1.0")
class Word2Vec extends Serializable with Logging {

  private var vectorSize = 100
  private var learningRate = 0.025
  private var numPartitions = 1
  private var numIterations = 1
  private var seed = Utils.random.nextLong()
  private var minCount = 5
  private var maxSentenceLength = 1000

  /**
   * Sets the maximum length (in words) of each sentence in the input data.
   * Any sentence longer than this threshold will be divided into chunks of
   * up to `maxSentenceLength` size (default: 1000)
   */
  @Since("2.0.0")
  def setMaxSentenceLength(maxSentenceLength: Int): this.type = {
    require(maxSentenceLength > 0,
      s"Maximum length of sentences must be positive but got ${maxSentenceLength}")
    this.maxSentenceLength = maxSentenceLength
    this
  }

  /**
   * Sets vector size (default: 100).
   */
  @Since("1.1.0")
  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.vectorSize = vectorSize
    this
  }

  /**
   * Sets initial learning rate (default: 0.025).
   */
  @Since("1.1.0")
  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate
    this
  }

  /**
   * Sets number of partitions (default: 1). Use a small number for accuracy.
   */
  @Since("1.1.0")
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
  @Since("1.1.0")
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations >= 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  /**
   * Sets random seed (default: a random long integer).
   */
  @Since("1.1.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Sets the window of words (default: 5)
   */
  @Since("1.6.0")
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
  @Since("1.3.0")
  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"Minimum number of times must be nonnegative but got ${minCount}")
    this.minCount = minCount
    this
  }

  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6
  private val MAX_CODE_LENGTH = 40

  /** context words from [-window, window] */
  private var window = 5

  private var trainWordsCount = 0L
  private var vocabSize = 0
  @transient private var vocab: Array[VocabWord] = null
  @transient private val vocabHash = mutable.HashMap.empty[String, Int]

  private def learnVocab[S <: Iterable[String]](dataset: RDD[S]): Unit = {
    val words = dataset.flatMap(x => x)

    vocab = words.map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .map(x => VocabWord(
        x._1,
        x._2,
        new Array[Int](MAX_CODE_LENGTH),
        new Array[Int](MAX_CODE_LENGTH),
        0))
      .collect()
      .sortBy(_.cn)(Ordering[Long].reverse)

    vocabSize = vocab.length
    require(vocabSize > 0, "The vocabulary size should be > 0. You may need to check " +
      "the setting of minCount, which could be large enough to remove all your words in sentences.")

    var a = 0
    while (a < vocabSize) {
      vocabHash += vocab(a).word -> a
      trainWordsCount += vocab(a).cn
      a += 1
    }
    logInfo(log"vocabSize = ${MDC(VOCAB_SIZE, vocabSize)}," +
      log" trainWordsCount = ${MDC(NUM_TRAIN_WORD, trainWordsCount)}")
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

  private def createBinaryTree(): Unit = {
    val count = new Array[Long](vocabSize * 2 + 1)
    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0
    while (a < vocabSize) {
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = Long.MaxValue
      a += 1
    }
    var pos1 = vocabSize - 1
    var pos2 = vocabSize

    var min1i = 0
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      assert(count(min1i) < Long.MaxValue)
      assert(count(min2i) < Long.MaxValue)
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0
    while (a < vocabSize) {
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }
  }

  /**
   * Computes the vector representation of each word in vocabulary.
   * @param dataset an RDD of sentences,
   *                each sentence is expressed as an iterable collection of words
   * @return a Word2VecModel
   */
  @Since("1.1.0")
  def fit[S <: Iterable[String]](dataset: RDD[S]): Word2VecModel = {

    learnVocab(dataset)

    createBinaryTree()

    val sc = dataset.context

    val expTable = sc.broadcast(createExpTable())
    val bcVocab = sc.broadcast(vocab)
    val bcVocabHash = sc.broadcast(vocabHash)
    try {
      doFit(dataset, sc, expTable, bcVocab, bcVocabHash)
    } finally {
      expTable.destroy()
      bcVocab.destroy()
      bcVocabHash.destroy()
    }
  }

  private def doFit[S <: Iterable[String]](
    dataset: RDD[S], sc: SparkContext,
    expTable: Broadcast[Array[Float]],
    bcVocab: Broadcast[Array[VocabWord]],
    bcVocabHash: Broadcast[mutable.HashMap[String, Int]]) = {
    // each partition is a collection of sentences,
    // will be translated into arrays of Index integer
    val sentences: RDD[Array[Int]] = dataset.mapPartitions { sentenceIter =>
      // Each sentence will map to 0 or more Array[Int]
      sentenceIter.flatMap { sentence =>
        // Sentence of words, some of which map to a word index
        val wordIndexes = sentence.flatMap(bcVocabHash.value.get)
        // break wordIndexes into trunks of maxSentenceLength when has more
        wordIndexes.grouped(maxSentenceLength).map(_.toArray)
      }
    }

    val newSentences = sentences.repartition(numPartitions).cache()
    val initRandom = new XORShiftRandom(seed)

    if (vocabSize.toLong * vectorSize >= Int.MaxValue) {
      throw new RuntimeException("Please increase minCount or decrease vectorSize in Word2Vec" +
        " to avoid an OOM. You are highly recommended to make your vocabSize*vectorSize, " +
        "which is " + vocabSize + "*" + vectorSize + " for now, less than `Int.MaxValue`.")
    }

    val syn0Global =
      Array.fill[Float](vocabSize * vectorSize)((initRandom.nextFloat() - 0.5f) / vectorSize)
    val syn1Global = new Array[Float](vocabSize * vectorSize)
    val totalWordsCounts = numIterations * trainWordsCount + 1
    var alpha = learningRate

    for (k <- 1 to numIterations) {
      val bcSyn0Global = sc.broadcast(syn0Global)
      val bcSyn1Global = sc.broadcast(syn1Global)
      val numWordsProcessedInPreviousIterations = (k - 1) * trainWordsCount

      val partial = newSentences.mapPartitionsWithIndex { case (idx, iter) =>
        val random = new XORShiftRandom(seed ^ ((idx + 1) << 16) ^ ((-k - 1) << 8))
        val syn0Modify = new Array[Int](vocabSize)
        val syn1Modify = new Array[Int](vocabSize)
        val model = iter.foldLeft((bcSyn0Global.value, bcSyn1Global.value, 0L, 0L)) {
          case ((syn0, syn1, lastWordCount, wordCount), sentence) =>
            var lwc = lastWordCount
            var wc = wordCount
            if (wordCount - lastWordCount > 10000) {
              lwc = wordCount
              alpha = learningRate *
                (1 - (numPartitions * wordCount.toDouble + numWordsProcessedInPreviousIterations) /
                  totalWordsCounts)
              if (alpha < learningRate * 0.0001) alpha = learningRate * 0.0001
              logInfo(log"wordCount =" +
                log" ${MDC(COUNT, wordCount + numWordsProcessedInPreviousIterations)}," +
                log" alpha = ${MDC(ALPHA, alpha)}")
            }
            wc += sentence.length
            var pos = 0
            while (pos < sentence.length) {
              val word = sentence(pos)
              val b = random.nextInt(window)
              // Train Skip-gram
              var a = b
              while (a < window * 2 + 1 - b) {
                if (a != window) {
                  val c = pos - window + a
                  if (c >= 0 && c < sentence.length) {
                    val lastWord = sentence(c)
                    val l1 = lastWord * vectorSize
                    val neu1e = new Array[Float](vectorSize)
                    // Hierarchical softmax
                    var d = 0
                    while (d < bcVocab.value(word).codeLen) {
                      val inner = bcVocab.value(word).point(d)
                      val l2 = inner * vectorSize
                      // Propagate hidden -> output
                      var f = BLAS.nativeBLAS.sdot(vectorSize, syn0, l1, 1, syn1, l2, 1)
                      if (f > -MAX_EXP && f < MAX_EXP) {
                        val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
                        f = expTable.value(ind)
                        val g = ((1 - bcVocab.value(word).code(d) - f) * alpha).toFloat
                        BLAS.nativeBLAS.saxpy(vectorSize, g, syn1, l2, 1, neu1e, 0, 1)
                        BLAS.nativeBLAS.saxpy(vectorSize, g, syn0, l1, 1, syn1, l2, 1)
                        syn1Modify(inner) += 1
                      }
                      d += 1
                    }
                    BLAS.nativeBLAS.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
                    syn0Modify(lastWord) += 1
                  }
                }
                a += 1
              }
              pos += 1
            }
            (syn0, syn1, lwc, wc)
        }
        val syn0Local = model._1
        val syn1Local = model._2
        // Only output modified vectors.
        Iterator.tabulate(vocabSize) { index =>
          if (syn0Modify(index) > 0) {
            Some((index, syn0Local.slice(index * vectorSize, (index + 1) * vectorSize)))
          } else {
            None
          }
        }.flatten ++ Iterator.tabulate(vocabSize) { index =>
          if (syn1Modify(index) > 0) {
            Some((index + vocabSize, syn1Local.slice(index * vectorSize, (index + 1) * vectorSize)))
          } else {
            None
          }
        }.flatten
      }
      // SPARK-24666: do normalization for aggregating weights from partitions.
      // Original Word2Vec either single-thread or multi-thread which do Hogwild-style aggregation.
      // Our approach needs to do extra normalization, otherwise adding weights continuously may
      // cause overflow on float and lead to infinity/-infinity weights.
      val synAgg = partial.mapPartitions { iter =>
        iter.map { case (id, vec) =>
          (id, (vec, 1))
        }
      }.reduceByKey { (vc1, vc2) =>
        BLAS.nativeBLAS.saxpy(vectorSize, 1.0f, vc2._1, 1, vc1._1, 1)
        (vc1._1, vc1._2 + vc2._2)
      }.map { case (id, (vec, count)) =>
        BLAS.nativeBLAS.sscal(vectorSize, 1.0f / count, vec, 1)
        (id, vec)
      }.collect()
      var i = 0
      while (i < synAgg.length) {
        val index = synAgg(i)._1
        if (index < vocabSize) {
          Array.copy(synAgg(i)._2, 0, syn0Global, index * vectorSize, vectorSize)
        } else {
          Array.copy(synAgg(i)._2, 0, syn1Global, (index - vocabSize) * vectorSize, vectorSize)
        }
        i += 1
      }
      bcSyn0Global.destroy()
      bcSyn1Global.destroy()
    }
    newSentences.unpersist()

    val wordArray = vocab.map(_.word)
    new Word2VecModel(CUtils.toMapWithIndex(wordArray), syn0Global)
  }

  /**
   * Computes the vector representation of each word in vocabulary (Java version).
   * @param dataset a JavaRDD of words
   * @return a Word2VecModel
   */
  @Since("1.1.0")
  def fit[S <: JavaIterable[String]](dataset: JavaRDD[S]): Word2VecModel = {
    fit(dataset.rdd.map(_.asScala))
  }
}

/**
 * Word2Vec model
 * @param wordIndex maps each word to an index, which can retrieve the corresponding
 *                  vector from wordVectors
 * @param wordVectors array of length numWords * vectorSize, vector corresponding
 *                    to the word mapped with index i can be retrieved by the slice
 *                    (i * vectorSize, i * vectorSize + vectorSize)
 */
@Since("1.1.0")
class Word2VecModel private[spark] (
    private[spark] val wordIndex: Map[String, Int],
    private[spark] val wordVectors: Array[Float]) extends Serializable with Saveable {

  private val numWords = wordIndex.size
  // vectorSize: Dimension of each word's vector.
  private val vectorSize = wordVectors.length / numWords

  // wordList: Ordered list of words obtained from wordIndex.
  private lazy val wordList: Array[String] = {
    wordIndex.toSeq.sortBy(_._2).iterator.map(_._1).toArray
  }

  // wordVecInvNorms: Array of length numWords, each value being the inverse of
  //                  Euclidean norm of the wordVector.
  private lazy val wordVecInvNorms: Array[Float] = {
    val size = vectorSize
    Array.tabulate(numWords) { i =>
      val norm = BLAS.nativeBLAS.snrm2(size, wordVectors, i * size, 1)
      if (norm != 0) 1 / norm else 0.0F
    }
  }

  // Auxiliary constructor must begin with call to 'this'.
  // Helper constructor for `def this(model: Map[String, Array[Float]])`.
  private def this(model: (Map[String, Int], Array[Float])) = {
    this(model._1, model._2)
  }

  @Since("1.5.0")
  def this(model: Map[String, Array[Float]]) = {
    this(Word2VecModel.buildFromVecMap(model))
  }

  @Since("1.4.0")
  def save(sc: SparkContext, path: String): Unit = {
    Word2VecModel.SaveLoadV1_0.save(sc, path, getVectors)
  }

  /**
   * Transforms a word to its vector representation
   * @param word a word
   * @return vector representation of word
   */
  @Since("1.1.0")
  def transform(word: String): Vector = {
    wordIndex.get(word) match {
      case Some(index) =>
        val size = vectorSize
        val offset = index * size
        val array = Array.ofDim[Double](size)
        var i = 0
        while (i < size) { array(i) = wordVectors(offset + i); i += 1 }
        Vectors.dense(array)
      case None =>
        throw new IllegalStateException(s"$word not in vocabulary")
    }
  }

  /**
   * Find synonyms of a word; do not include the word itself in results.
   * @param word a word
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
   */
  @Since("1.1.0")
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = transform(word)
    findSynonyms(vector.toArray, num, Some(word))
  }

  /**
   * Find synonyms of the vector representation of a word, possibly
   * including any words in the model vocabulary whose vector representation
   * is the supplied vector.
   * @param vector vector representation of a word
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
   */
  @Since("1.1.0")
  def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = {
    findSynonyms(vector.toArray, num, None)
  }

  /**
   * Find synonyms of the vector representation of a word, rejecting
   * words identical to the value of wordOpt, if one is supplied.
   * @param vector vector representation of a word
   * @param num number of synonyms to find
   * @param wordOpt optionally, a word to reject from the results list
   * @return array of (word, cosineSimilarity)
   */
  private[spark] def findSynonyms(
      vector: Array[Double],
      num: Int,
      wordOpt: Option[String]): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    val localVectorSize = vectorSize

    val floatVec = vector.map(_.toFloat)
    val vecNorm = BLAS.nativeBLAS.snrm2(localVectorSize, floatVec, 1)

    val localWordList = wordList
    val localNumWords = numWords
    if (vecNorm == 0) {
      Iterator.tabulate(num + 1)(i => (localWordList(i), 0.0))
        .filterNot(t => wordOpt.contains(t._1))
        .take(num)
        .toArray
    } else {
      // Normalize input vector before BLAS.nativeBLAS.sgemv to avoid Inf value
      BLAS.nativeBLAS.sscal(localVectorSize, 1 / vecNorm, floatVec, 0, 1)

      val cosineVec = Array.ofDim[Float](localNumWords)
      BLAS.nativeBLAS.sgemv("T", localVectorSize, localNumWords, 1.0F, wordVectors, localVectorSize,
        floatVec, 1, 0.0F, cosineVec, 1)

      val localWordVecInvNorms = wordVecInvNorms
      var i = 0
      while (i < cosineVec.length) { cosineVec(i) *= localWordVecInvNorms(i); i += 1 }

      val idxOrd = new GuavaOrdering[Int] {
        override def compare(left: Int, right: Int): Int = {
          Ordering[Float].compare(cosineVec(left), cosineVec(right))
        }
      }

      idxOrd.greatestOf(Iterator.range(0, localNumWords).asJava, num + 1)
        .iterator.asScala
        .map(i => (localWordList(i), cosineVec(i).toDouble))
        .filterNot(t => wordOpt.contains(t._1))
        .take(num)
        .toArray
    }
  }

  /**
   * Returns a map of words to their vector representations.
   */
  @Since("1.2.0")
  def getVectors: Map[String, Array[Float]] = {
    wordIndex.map { case (word, ind) =>
      (word, wordVectors.slice(vectorSize * ind, vectorSize * ind + vectorSize))
    }
  }

}

@Since("1.4.0")
object Word2VecModel extends Loader[Word2VecModel] {

  private def buildFromVecMap(
      model: Map[String, Array[Float]]): (Map[String, Int], Array[Float]) = {
    require(model.nonEmpty, "Word2VecMap should be non-empty")

    val (vectorSize, numWords) = (model.head._2.length, model.size)
    val wordVectors = new Array[Float](vectorSize * numWords)

    val wordIndex = collection.immutable.Map.newBuilder[String, Int]
    wordIndex.sizeHint(numWords)

    model.iterator.zipWithIndex.foreach {
      case ((word, vector), i) =>
        wordIndex += ((word, i))
        Array.copy(vector, 0, wordVectors, i * vectorSize, vectorSize)
    }
    (wordIndex.result(), wordVectors)
  }

  private object SaveLoadV1_0 {

    val formatVersionV1_0 = "1.0"

    val classNameV1_0 = "org.apache.spark.mllib.feature.Word2VecModel"

    case class Data(word: String, vector: Array[Float])

    def load(sc: SparkContext, path: String): Word2VecModel = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val dataFrame = spark.read.parquet(Loader.dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[Data](dataFrame.schema)

      val dataArray = dataFrame.select("word", "vector").collect()
      val word2VecMap = dataArray.map(i => (i.getString(0), i.getSeq[Float](1).toArray)).toMap
      new Word2VecModel(word2VecMap)
    }

    def save(sc: SparkContext, path: String, model: Map[String, Array[Float]]): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val vectorSize = model.values.head.length
      val numWords = model.size
      val metadata = compact(render(
        ("class" -> classNameV1_0) ~ ("version" -> formatVersionV1_0) ~
        ("vectorSize" -> vectorSize) ~ ("numWords" -> numWords)))
      spark.createDataFrame(Seq(Tuple1(metadata))).write.text(Loader.metadataPath(path))

      // We want to partition the model in partitions smaller than
      // spark.kryoserializer.buffer.max
      val bufferSize = Utils.byteStringAsBytes(
        spark.conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "64m"))
      // We calculate the approximate size of the model
      // We only calculate the array size, considering an
      // average string size of 15 bytes, the formula is:
      // (floatSize * vectorSize + 15) * numWords
      val approxSize = (4L * vectorSize + 15) * numWords
      val nPartitions = ((approxSize / bufferSize) + 1).toInt
      val dataArray = model.toSeq.map { case (w, v) => Data(w, v) }
      spark.createDataFrame(dataArray).repartition(nPartitions).write.parquet(Loader.dataPath(path))
    }
  }

  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): Word2VecModel = {

    val (loadedClassName, loadedVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats: Formats = DefaultFormats
    val expectedVectorSize = (metadata \ "vectorSize").extract[Int]
    val expectedNumWords = (metadata \ "numWords").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.classNameV1_0
    (loadedClassName, loadedVersion) match {
      case (classNameV1_0, "1.0") =>
        val model = SaveLoadV1_0.load(sc, path)
        val vectorSize = model.getVectors.values.head.length
        val numWords = model.getVectors.size
        require(expectedVectorSize == vectorSize,
          s"Word2VecModel requires each word to be mapped to a vector of size " +
          s"$expectedVectorSize, got vector of size $vectorSize")
        require(expectedNumWords == numWords,
          s"Word2VecModel requires $expectedNumWords words, but got $numWords")
        model
      case _ => throw new Exception(
        s"Word2VecModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $loadedVersion).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}
