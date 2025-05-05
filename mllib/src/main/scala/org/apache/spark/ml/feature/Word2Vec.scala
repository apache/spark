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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.internal.config.Kryo.KRYO_SERIALIZER_MAX_BUFFER_SIZE
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils, VersionUtils}
import org.apache.spark.util.ArrayImplicits._

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

  /** @group getParam */
  def getMaxSentenceLength: Int = $(maxSentenceLength)

  setDefault(vectorSize -> 100, windowSize -> 5, numPartitions -> 1, minCount -> 5,
    maxSentenceLength -> 1000, stepSize -> 0.025, maxIter -> 1)

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

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): Word2VecModel = {
    transformSchema(dataset.schema, logging = true)
    val input =
      dataset.select($(inputCol)).rdd.map(_.getSeq[String](0))
    val wordVectors = new feature.Word2Vec()
      .setLearningRate($(stepSize))
      .setMinCount($(minCount))
      .setNumIterations($(maxIter))
      .setNumPartitions($(numPartitions))
      .setSeed($(seed))
      .setVectorSize($(vectorSize))
      .setWindowSize($(windowSize))
      .setMaxSentenceLength($(maxSentenceLength))
      .fit(input)
    copyValues(new Word2VecModel(uid, wordVectors).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Word2Vec = defaultCopy(extra)
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

  // For ml connect only
  private[ml] def this() = this("", null)

  /**
   * Returns a dataframe with two fields, "word" and "vector", with "word" being a String and
   * and the vector the DenseVector that it is mapped to.
   */
  @Since("1.5.0")
  @transient lazy val getVectors: DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val wordVec = wordVectors.getVectors.transform((_, vec) => Vectors.dense(vec.map(_.toDouble)))
    spark.createDataFrame(wordVec.toSeq).toDF("word", "vector")
  }

  /**
   * Find "num" number of words closest in similarity to the given word, not
   * including the word itself.
   * @return a dataframe with columns "word" and "similarity" of the word and the cosine
   * similarities between the synonyms and the given word.
   */
  @Since("1.5.0")
  def findSynonyms(word: String, num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(findSynonymsArray(word, num).toImmutableArraySeq)
      .toDF("word", "similarity")
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
    spark.createDataFrame(findSynonymsArray(vec, num).toImmutableArraySeq)
      .toDF("word", "similarity")
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
    wordVectors.findSynonyms(vec.toArray, num, None)
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
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val bcModel = dataset.sparkSession.sparkContext.broadcast(this.wordVectors)
    val size = $(vectorSize)
    val emptyVec = Vectors.sparse(size, Array.emptyIntArray, Array.emptyDoubleArray)
    val transformer = udf { sentence: Seq[String] =>
      if (sentence.isEmpty) {
        emptyVec
      } else {
        val wordIndices = bcModel.value.wordIndex
        val wordVectors = bcModel.value.wordVectors
        val array = Array.ofDim[Double](size)
        var count = 0
        sentence.foreach { word =>
          wordIndices.get(word).foreach { index =>
            val offset = index * size
            var i = 0
            while (i < size) { array(i) += wordVectors(offset + i); i += 1 }
          }
          count += 1
        }
        val vec = Vectors.dense(array)
        BLAS.scal(1.0 / count, vec)
        vec
      }
    }

    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), $(vectorSize))
    }
    outputSchema
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Word2VecModel = {
    val copied = new Word2VecModel(uid, wordVectors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new Word2VecModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"Word2VecModel: uid=$uid, numWords=${wordVectors.wordIndex.size}, " +
      s"vectorSize=${$(vectorSize)}"
  }
}

@Since("1.6.0")
object Word2VecModel extends MLReadable[Word2VecModel] {

  private[Word2VecModel] case class Data(word: String, vector: Array[Float])

  private[Word2VecModel]
  class Word2VecModelWriter(instance: Word2VecModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)

      val wordVectors = instance.wordVectors.getVectors
      val dataPath = new Path(path, "data").toString
      val bufferSizeInBytes = Utils.byteStringAsBytes(
        sc.conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "64m"))
      val numPartitions = Word2VecModelWriter.calculateNumberOfPartitions(
        bufferSizeInBytes, instance.wordVectors.wordIndex.size, instance.getVectorSize)
      val datum = wordVectors.toArray.map { case (word, vector) => Data(word, vector) }
      ReadWriteUtils.saveArray[Data](dataPath, datum, sparkSession, numPartitions)
    }
  }

  private[feature]
  object Word2VecModelWriter {
    /**
     * Calculate the number of partitions to use in saving the model.
     * [SPARK-11994] - We want to partition the model in partitions smaller than
     * spark.kryoserializer.buffer.max
     * @param bufferSizeInBytes  Set to spark.kryoserializer.buffer.max
     * @param numWords  Vocab size
     * @param vectorSize  Vector length for each word
     */
    def calculateNumberOfPartitions(
        bufferSizeInBytes: Long,
        numWords: Int,
        vectorSize: Int): Int = {
      val floatSize = 4L  // Use Long to help avoid overflow
      val averageWordSize = 15
      // Calculate the approximate size of the model.
      // Assuming an average word size of 15 bytes, the formula is:
      // (floatSize * vectorSize + 15) * numWords
      val approximateSizeInBytes = (floatSize * vectorSize + averageWordSize) * numWords
      val numPartitions = (approximateSizeInBytes / bufferSizeInBytes) + 1
      require(numPartitions < 10e8, s"Word2VecModel calculated that it needs $numPartitions " +
        s"partitions to save this model, which is too large.  Try increasing " +
        s"spark.kryoserializer.buffer.max so that Word2VecModel can use fewer partitions.")
      numPartitions.toInt
    }
  }

  private class Word2VecModelReader extends MLReader[Word2VecModel] {

    private val className = classOf[Word2VecModel].getName

    override def load(path: String): Word2VecModel = {
      val spark = sparkSession

      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
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
        val datum = ReadWriteUtils.loadArray[Data](dataPath, sparkSession)
        val wordVectorsMap = datum.map(wordVector => (wordVector.word, wordVector.vector)).toMap
        new feature.Word2VecModel(wordVectorsMap)
      }

      val model = new Word2VecModel(metadata.uid, oldModel)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[Word2VecModel] = new Word2VecModelReader

  @Since("1.6.0")
  override def load(path: String): Word2VecModel = super.load(path)
}
