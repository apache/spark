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

package org.apache.spark.ml.feature

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils, VersionUtils}
import org.apache.spark.util.random.XORShiftRandom


/**
 * Params for [[Word2Vec]] and [[Word2VecModel]].
 */
private[feature] trait Word2VecBase extends Params
  with HasInputCol with HasOutputCol with HasMaxIter with HasStepSize with HasSeed {

  /**
   * The dimension of the code that you want to transform from words.
   * Default: 100
   * @group param
   */
  final val vectorSize = new IntParam(
    this, "vectorSize", "the dimension of codes after transforming from words (> 0)",
    ParamValidators.gt(0))
  setDefault(vectorSize -> 100)

  /** @group getParam */
  def getVectorSize: Int = $(vectorSize)

  /**
   * The window size (context words from [-window, window]).
   * Default: 5
   * @group expertParam
   */
  final val windowSize = new IntParam(
    this, "windowSize", "the window size (context words from [-window, window]) (> 0)",
    ParamValidators.gt(0))
  setDefault(windowSize -> 5)

  /** @group expertGetParam */
  def getWindowSize: Int = $(windowSize)

  /**
   * Number of partitions for sentences of words.
   * Default: 1
   * @group param
   */
  final val numPartitions = new IntParam(
    this, "numPartitions", "number of partitions for sentences of words (> 0)",
    ParamValidators.gt(0))
  setDefault(numPartitions -> 1)

  /** @group getParam */
  def getNumPartitions: Int = $(numPartitions)

  /**
   * The minimum number of times a token must appear to be included in the word2vec model's
   * vocabulary.
   * Default: 5
   * @group param
   */
  final val minCount = new IntParam(this, "minCount", "the minimum number of times a token must " +
    "appear to be included in the word2vec model's vocabulary (>= 0)", ParamValidators.gtEq(0))
  setDefault(minCount -> 5)

  /** @group getParam */
  def getMinCount: Int = $(minCount)

  /**
   * Sets the maximum length (in words) of each sentence in the input data.
   * Any sentence longer than this threshold will be divided into chunks of
   * up to `maxSentenceLength` size.
   * Default: 1000
   * @group param
   */
  final val maxSentenceLength = new IntParam(this, "maxSentenceLength", "Maximum length " +
    "(in words) of each sentence in the input data. Any sentence longer than this threshold will " +
    "be divided into chunks up to the size (> 0)", ParamValidators.gt(0))
  setDefault(maxSentenceLength -> 1000)

  /**
   * Choose the technique to use for generating word embeddings
   * Default: true (Use Skip-Gram with Hierarchical softmax)
   * @group param
   */
  final val skipGram = new BooleanParam(this, "skipGram", "Use Skip-Gram model to generate word " +
    "embeddings. When set to false, Continuous Bag of Words (CBOW) model is used instead.")
  setDefault(skipGram -> true)

  /**
   * Number of negative samples to use with CBOW based estimation
   * Default: 15
   * @group param
   */
  final val negativeSamples = new IntParam(this, "negativeSamples", "Number of negative samples " +
    "to use with CBOW estimation", ParamValidators.gt(0))
  setDefault(negativeSamples -> 15)

  /** @group getParam */
  def getMaxSentenceLength: Int = $(maxSentenceLength)

  setDefault(stepSize -> 0.025)
  setDefault(maxIter -> 1)

  /**
   * Validate and transform the input schema.
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val typeCandidates = List(new ArrayType(StringType, true), new ArrayType(StringType, false))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

/**
 * Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
 * natural language processing or machine learning process.
 */
@Since("1.4.0")
final class Word2Vec @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends Estimator[Word2VecModel] with Word2VecBase with DefaultParamsWritable {

  private val batchSize = 10000
  private val power = 0.75
  private val maxUnigramTableSize = 100*1000*1000
  private val unigramTableSizeFactor = 20

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("w2v"))

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setVectorSize(value: Int): this.type = set(vectorSize, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setWindowSize(value: Int): this.type = set(windowSize, value)

  /** @group setParam */
  @Since("1.4.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /** @group setParam */
  @Since("1.4.0")
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.4.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMinCount(value: Int): this.type = set(minCount, value)

  /** @group setParam */
  @Since("2.0.0")
  def setMaxSentenceLength(value: Int): this.type = set(maxSentenceLength, value)

  @Since("2.2.0")
  def setSkipGram(value: Boolean): this.type = set(skipGram, value)

  @Since("2.2.0")
  def setNegativeSamples(value: Int): this.type = set(negativeSamples, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): Word2VecModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Seq[String]](0))
    val wordVectors = if ($(skipGram)) {
        new feature.Word2Vec()
          .setLearningRate($(stepSize))
          .setMinCount($(minCount))
          .setNumIterations($(maxIter))
          .setNumPartitions($(numPartitions))
          .setSeed($(seed))
          .setVectorSize($(vectorSize))
          .setWindowSize($(windowSize))
          .setMaxSentenceLength($(maxSentenceLength))
          .fit(input)
      } else {
        fitCBOW(input)
      }
    copyValues(new Word2VecModel(uid, wordVectors).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Word2Vec = defaultCopy(extra)

  /**
   * Similar to InitUnigramTable in the original code. Instead of using an array of size 100 million
   * like the original, we size it to be 20 times the vocabulary size.
   * We sacrifice memory here, to get constant time lookups into this array when generating
   * negative samples.
   */
  private def generateUnigramTable(normalizedWeights: Array[Double], tableSize: Int): Array[Int] = {
    val table = Array.fill(tableSize)(0)
    var a = 0
    var i = 0
    while (a < table.length) {
      table.update(a, i)
      if (a.toFloat / table.length >= normalizedWeights(i)) {
        i = math.min(normalizedWeights.length - 1, i + 1)
      }
      a += 1
    }
    table
  }

  private def generateVocab[S <: Iterable[String]](input: RDD[S]):
      (Int, Long, Map[String, Int], Array[Int]) = {
    val sc = input.context

    val words = input.flatMap(x => x)

    val vocab = words.map(w => (w, 1L))
      .reduceByKey(_ + _)
      .filter{case (w, c) => c >= $(minCount)}
      .collect()
      .sortWith{case ((w1, c1), (w2, c2)) => c1 > c2}

    val totalWordCount = vocab.map(_._2).sum

    val vocabMap = vocab.zipWithIndex.map{case ((w, c), i) =>
      w -> i
    }.toMap

    // We create a cumulative distribution array, unlike the original implemention
    // and use binary search to get insertion points. This should replicate the same
    // behavior as the table in original implementation.
    val weights = vocab.map(x => scala.math.pow(x._2, power))
    val totalWeight = weights.sum

    val normalizedCumWeights = weights.scanLeft(0.0)(_ + _).tail.map(x => (x / totalWeight))

    val unigramTableSize =
      math.min(maxUnigramTableSize, unigramTableSizeFactor * normalizedCumWeights.length)
    val unigramTable = generateUnigramTable(normalizedCumWeights, unigramTableSize)

    (vocabMap.size, totalWordCount, vocabMap, unigramTable)
  }

  private def generateInitMatrices(vocabSize: Int, dim: Int, sc: SparkContext):
      (Broadcast[Array[Array[Float]]], Broadcast[Array[Array[Float]]]) = {
    val random = new XORShiftRandom(System.currentTimeMillis())
    // input to hidden layer weights
    val syn0 = Array.fill(vocabSize, dim)(random.nextFloat - 0.5f)
    // hidden layer to output weights
    val syn1 = Array.fill(vocabSize, dim)(0.0f)
    (sc.broadcast(syn0), sc.broadcast(syn1))
  }

  /**
   * This method implements Word2Vec Continuous Bag Of Words based implementation using
   * negative sampling optimization, using BLAS for vectorizing operations where applicable.
   * The algorithm is parallelized in the same way as the skip-gram based estimation.
   * @param input
   * @return
   */
  private def fitCBOW[S <: Iterable[String]](input: RDD[S]): feature.Word2VecModel = {
    val (vocabSize, totalWordCount, vocabMap, uniTable) = generateVocab(input)
    val negSamples = $(negativeSamples)
    assert(negSamples < uniTable.length,
      s"Need a dictionary larger than ${uniTable.length} for $negSamples negative samples")
    val seed = $(this.seed)
    val initRandom = new XORShiftRandom(seed)

    val vectorSize = $(this.vectorSize)

    val syn0Global = Array.fill(vocabSize * vectorSize)(initRandom.nextFloat - 0.5f)
    val syn1Global = Array.fill(vocabSize * vectorSize)(0.0f)

    val sc = input.context

    val vocabMapbc = sc.broadcast(vocabMap)
    val unigramTablebc = sc.broadcast(uniTable)

    val window = $(windowSize)

    val digitSentences = input.flatMap{sentence =>
      val wordIndexes = sentence.flatMap(vocabMapbc.value.get)
      wordIndexes.grouped($(maxSentenceLength)).map(_.toArray)
    }.repartition($(numPartitions)).cache()

    val learningRate = $(stepSize)

    val wordsPerPartition = totalWordCount / $(numPartitions)

    logInfo(s"VocabSize: ${vocabMap.size}, TotalWordCount: $totalWordCount")

    for {iteration <- 1 to $(maxIter)} {
      logInfo(s"Starting iteration: $iteration")
      val iterationStartTime = System.nanoTime()

      val syn0bc = sc.broadcast(syn0Global)
      val syn1bc = sc.broadcast(syn1Global)

      val partialFits = digitSentences.mapPartitionsWithIndex{ case (i_, iter) =>
        logInfo(s"Iteration: $iteration, Partition: $i_")
        logInfo(s"Numerical lib class being used : ${blas.getClass.getName}")
        val random = new XORShiftRandom(seed ^ ((i_ + 1) << 16) ^ ((-iteration - 1) << 8))
        val contextWordPairs = iter.flatMap(generateContextWordPairs(_, window, random))

        val groupedBatches = contextWordPairs.grouped(batchSize)

        val negLabels = 1.0f +: Array.fill(negSamples)(0.0f)
        val syn0 = syn0bc.value
        val syn1 = syn1bc.value
        val unigramTable = unigramTablebc.value

        // initialize intermediate arrays
        val contextVector = Array.fill(vectorSize)(0.0f)
        val l2Vectors = Array.fill(vectorSize * (negSamples + 1))(0.0f)
        val gb = Array.fill(negSamples + 1)(0.0f)
        val hiddenLayerUpdate = Array.fill(vectorSize * (negSamples + 1))(0.0f)
        val neu1e = Array.fill(vectorSize)(0.0f)
        val wordIndices = Array.fill(negSamples + 1)(0)

        val time = System.nanoTime
        var batchTime = System.nanoTime
        var idx = -1L
        for (batch <- groupedBatches) {
          idx = idx + 1

          val wordRatio =
            idx.toFloat * batchSize /
            ($(maxIter) * (wordsPerPartition.toFloat + 1)) + ((iteration - 1).toFloat / $(maxIter))
          val alpha = math.max(learningRate * 0.0001, learningRate * (1 - wordRatio)).toFloat

          if(idx % 10 == 0 && idx > 0) {
            logInfo(s"Partition: $i_, wordRatio = $wordRatio, alpha = $alpha")
            val wordCount = batchSize * idx
            val timeTaken = (System.nanoTime - time) / 1e6
            val batchWordCount = 10 * batchSize
            val currentBatchTime = (System.nanoTime - batchTime) / 1e6
            batchTime = System.nanoTime
            logInfo(s"Partition: $i_, Batch time: $currentBatchTime ms, batch speed: " +
              s"${batchWordCount / currentBatchTime * 1000} words/s")
            logInfo(s"Partition: $i_, Cumulative time: $timeTaken ms, cumulative speed: " +
              s"${wordCount / timeTaken * 1000} words/s")
          }

          val errors = for ((contextIds, word) <- batch) yield {
            // initialize vectors to 0
            initializeVector(contextVector)
            initializeVector(l2Vectors)
            initializeVector(gb)
            initializeVector(hiddenLayerUpdate)
            initializeVector(neu1e)

            val scale = 1.0f / contextIds.length

            // feed forward
            contextIds.foreach { c =>
              blas.saxpy(vectorSize, scale, syn0, c * vectorSize, 1, contextVector, 0, 1)
            }

            generateNegativeSamples(random, word, unigramTable, negSamples, wordIndices)

            wordIndices.view.zipWithIndex.foreach { case (wordId, i) =>
              blas.scopy(vectorSize, syn1, vectorSize * wordId, 1, l2Vectors, vectorSize * i, 1)
            }

            val rows = negSamples + 1
            val cols = vectorSize
            blas
              .sgemv("T", cols, rows, 1.0f, l2Vectors, 0, cols, contextVector, 0, 1, 0.0f, gb, 0, 1)

            (0 to gb.length-1).foreach {i =>
              val v = 1.0f / (1 + math.exp(-gb(i)).toFloat)
              val err = (negLabels(i) - v) * alpha
              gb.update(i, err)
            }

            // update for hidden -> output layer
            blas.sger(cols, rows, 1.0f, contextVector, 1, gb, 1, hiddenLayerUpdate, cols)

            // update hidden -> output layer, syn1
            wordIndices.view.zipWithIndex.foreach {case (w, i) =>
              blas.saxpy(vectorSize,
                1.0f,
                hiddenLayerUpdate,
                i * vectorSize,
                1,
                syn1,
                w * vectorSize,
                1)
            }

            // update for word vectors
            blas.sgemv("N", cols, rows, scale, l2Vectors, 0, cols, gb, 0, 1, 1.0f, neu1e, 0, 1)

            // update input -> hidden layer, syn0
            contextIds.foreach { i =>
              blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, i * vectorSize, 1)
            }
            gb.map(math.abs).sum / alpha
          }
          logInfo(s"Partition: $i_, Average Batch Error = ${errors.sum / batchSize}")
        }
        Iterator((0, syn0), (1, syn1))
      }

      val aggedMatrices = partialFits.reduceByKey{case (v1, v2) =>
        blas.saxpy(vocabSize, 1.0f, v2, 1, v1, 1)
        v1
      }.collect

      assert(aggedMatrices.length == 2)
      val norm = 1.0f / $(numPartitions)
      aggedMatrices.foreach {case (i, syn) =>
        blas.sscal(syn.length, norm, syn, 0, 1)
        if (i == 0) {
          // copy syn0
          blas.scopy(syn.length, syn, 0, 1, syn0Global, 0, 1)
        } else {
          // copy syn1
          blas.scopy(syn.length, syn, 0, 1, syn1Global, 0, 1)
        }
      }
      syn0bc.destroy(false)
      syn1bc.destroy(false)
      val timePerIteration = (System.nanoTime() - iterationStartTime) / 1e6
      logInfo(s"Total time taken per iteration: ${timePerIteration} ms")
    }
    digitSentences.unpersist()
    vocabMapbc.destroy()
    unigramTablebc.destroy()

    new feature.Word2VecModel(vocabMap, syn0Global)
  }

  private def initializeVector(v: Array[Float], value: Float = 0.0f): Unit = {
    var i = 0
    val length = v.length
    while(i < length) {
      v.update(i, value)
      i+= 1
    }
  }

  private def generateContextWordPairs(
      sentence: Array[Int],
      window: Int,
      random: XORShiftRandom): Iterator[(Array[Int], Int)] = {
    sentence.view.zipWithIndex.map {case (word, i) =>
      val b = window - random.nextInt(window) // (window - a) in original code
      // pick b words around the current word index
      val start = math.max(0, i - b) // c in original code, floor ar 0
      val end = math.min(sentence.length, i + b + 1) // cap at sentence length
      // make sure current word is not a part of the context
      val contextIds = sentence.view.zipWithIndex.slice(start, end)
        .filter{case (_, pos) => pos != i}.map(_._1)
      (contextIds.toArray, word)
    }.toIterator
  }

  // This essentially helps translate from uniform distribution to a distribution
  // resembling uni-gram frequency distribution.
  private def generateNegativeSamples(
      random: XORShiftRandom,
      word: Int,
      unigramTable: Array[Int],
      numSamples: Int,
      arr: Array[Int]): Unit = {
    assert(numSamples + 1 == arr.length,
      s"Input array should be large enough to hold ${numSamples} negative samples")
    arr.update(0, word)
    var i = 1
    while (i <= numSamples) {
      val negSample = unigramTable(random.nextInt(unigramTable.length))
      if(negSample != word) {
        arr.update(i, negSample)
        i += 1
      }
    }
  }
}

@Since("1.6.0")
object Word2Vec extends DefaultParamsReadable[Word2Vec] {

  @Since("1.6.0")
  override def load(path: String): Word2Vec = super.load(path)
}

/**
 * Model fitted by [[Word2Vec]].
 */
@Since("1.4.0")
class Word2VecModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @transient private val wordVectors: feature.Word2VecModel)
  extends Model[Word2VecModel] with Word2VecBase with MLWritable {

  import Word2VecModel._

  /**
   * Returns a dataframe with two fields, "word" and "vector", with "word" being a String and
   * and the vector the DenseVector that it is mapped to.
   */
  @Since("1.5.0")
  @transient lazy val getVectors: DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val wordVec = wordVectors.getVectors.mapValues(vec => Vectors.dense(vec.map(_.toDouble)))
    spark.createDataFrame(wordVec.toSeq).toDF("word", "vector")
  }

  /**
   * Find "num" number of words closest in similarity to the given word, not
   * including the word itself.
   * @return a dataframe with columns "word" and "similarity" of the word and the cosine
   * similarities between the synonyms and the given word vector.
   */
  @Since("1.5.0")
  def findSynonyms(word: String, num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(findSynonymsArray(word, num)).toDF("word", "similarity")
  }

  /**
   * Find "num" number of words whose vector representation is most similar to the supplied vector.
   * If the supplied vector is the vector representation of a word in the model's vocabulary,
   * that word will be in the results.
   * @return a dataframe with columns "word" and "similarity" of the word and the cosine
   * similarities between the synonyms and the given word vector.
   */
  @Since("2.0.0")
  def findSynonyms(vec: Vector, num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(findSynonymsArray(vec, num)).toDF("word", "similarity")
  }

  /**
   * Find "num" number of words whose vector representation is most similar to the supplied vector.
   * If the supplied vector is the vector representation of a word in the model's vocabulary,
   * that word will be in the results.
   * @return an array of the words and the cosine similarities between the synonyms given
   * word vector.
   */
  @Since("2.2.0")
  def findSynonymsArray(vec: Vector, num: Int): Array[(String, Double)] = {
    wordVectors.findSynonyms(vec, num)
  }

  /**
   * Find "num" number of words closest in similarity to the given word, not
   * including the word itself.
   * @return an array of the words and the cosine similarities between the synonyms given
   * word vector.
   */
  @Since("2.2.0")
  def findSynonymsArray(word: String, num: Int): Array[(String, Double)] = {
    wordVectors.findSynonyms(word, num)
  }

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Transform a sentence column to a vector column to represent the whole sentence. The transform
   * is performed by averaging all word vectors it contains.
   */
  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val vectors = wordVectors.getVectors
      .mapValues(vv => Vectors.dense(vv.map(_.toDouble)))
      .map(identity) // mapValues doesn't return a serializable map (SI-7005)
    val bVectors = dataset.sparkSession.sparkContext.broadcast(vectors)
    val d = $(vectorSize)
    val word2Vec = udf { sentence: Seq[String] =>
      if (sentence.isEmpty) {
        Vectors.sparse(d, Array.empty[Int], Array.empty[Double])
      } else {
        val sum = Vectors.zeros(d)
        sentence.foreach { word =>
          bVectors.value.get(word).foreach { v =>
            BLAS.axpy(1.0, v, sum)
          }
        }
        BLAS.scal(1.0 / sentence.size, sum)
        sum
      }
    }
    dataset.withColumn($(outputCol), word2Vec(col($(inputCol))))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Word2VecModel = {
    val copied = new Word2VecModel(uid, wordVectors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new Word2VecModelWriter(this)
}

@Since("1.6.0")
object Word2VecModel extends MLReadable[Word2VecModel] {

  private case class Data(word: String, vector: Array[Float])

  private[Word2VecModel]
  class Word2VecModelWriter(instance: Word2VecModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      val wordVectors = instance.wordVectors.getVectors
      val dataSeq = wordVectors.toSeq.map { case (word, vector) => Data(word, vector) }
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(dataSeq)
        .repartition(calculateNumberOfPartitions)
        .write
        .parquet(dataPath)
    }

    def calculateNumberOfPartitions(): Int = {
      val floatSize = 4
      val averageWordSize = 15
      // [SPARK-11994] - We want to partition the model in partitions smaller than
      // spark.kryoserializer.buffer.max
      val bufferSizeInBytes = Utils.byteStringAsBytes(
        sc.conf.get("spark.kryoserializer.buffer.max", "64m"))
      // Calculate the approximate size of the model.
      // Assuming an average word size of 15 bytes, the formula is:
      // (floatSize * vectorSize + 15) * numWords
      val numWords = instance.wordVectors.wordIndex.size
      val approximateSizeInBytes = (floatSize * instance.getVectorSize + averageWordSize) * numWords
      ((approximateSizeInBytes / bufferSizeInBytes) + 1).toInt
    }
  }

  private class Word2VecModelReader extends MLReader[Word2VecModel] {

    private val className = classOf[Word2VecModel].getName

    override def load(path: String): Word2VecModel = {
      val spark = sparkSession
      import spark.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val (major, minor) = VersionUtils.majorMinorVersion(metadata.sparkVersion)

      val dataPath = new Path(path, "data").toString

      val oldModel = if (major < 2 || (major == 2 && minor < 2)) {
        val data = spark.read.parquet(dataPath)
          .select("wordIndex", "wordVectors")
          .head()
        val wordIndex = data.getAs[Map[String, Int]](0)
        val wordVectors = data.getAs[Seq[Float]](1).toArray
        new feature.Word2VecModel(wordIndex, wordVectors)
      } else {
        val wordVectorsMap = spark.read.parquet(dataPath).as[Data]
          .collect()
          .map(wordVector => (wordVector.word, wordVector.vector))
          .toMap
        new feature.Word2VecModel(wordVectorsMap)
      }

      val model = new Word2VecModel(metadata.uid, oldModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[Word2VecModel] = new Word2VecModelReader

  @Since("1.6.0")
  override def load(path: String): Word2VecModel = super.load(path)
}
