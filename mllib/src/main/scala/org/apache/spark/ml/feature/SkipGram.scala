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

import scala.util.Try

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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{Utils, VersionUtils}



/**
 * Params for [[SkipGram]] and [[SkipGramModel]].
 */
private[feature] trait SkipGramBase extends Params
  with HasInputCol with HasOutputCol with HasMaxIter with HasStepSize {

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
   * Number of threads.
   * Default: 1
   * @group param
   */
  final val numThread = new IntParam(
    this, "numThread", "number of threads (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getNumThread: Int = $(numThread)

  /**
   * The minimum number of times a token must appear to be included in the SkipGram model's
   * vocabulary.
   * Default: 5
   * @group param
   */
  final val minCount = new IntParam(this, "minCount", "the minimum number of times a token must " +
    "appear to be included in the SkipGram model's vocabulary (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  def getMinCount: Int = $(minCount)

  /**
   * The negative sampling word frequency power
   * Default: 0
   * @group param
   */
  final val pow = new DoubleParam(this, "pow", "the negative sampling word frequency power (0 <= pow <= 1)",
    ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getPow: Double = $(pow)

  /**
   * The frequent word subsample ratio
   * Default: 0
   * @group param
   */
  final val sample = new DoubleParam(this, "sample", "the frequent word subsample ratio (0 <= sample <= 1)",
    ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getSample: Double = $(sample)

  /**
   * The number of negative samples
   * Default: 5
   * @group param
   */
  final val negative = new IntParam(this, "negative", "the number of negative samples (>0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getNegative: Int = $(negative)

  /**
   * Param for StorageLevel for intermediate datasets. Pass in a string representation of
   * `StorageLevel`. Cannot be "NONE".
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val intermediateStorageLevel = new Param[String](this, "intermediateStorageLevel",
    "StorageLevel for intermediate datasets. Cannot be 'NONE'.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess && s != "NONE")

  /** @group expertGetParam */
  def getIntermediateStorageLevel: String = $(intermediateStorageLevel)


  setDefault(vectorSize -> 100, windowSize -> 5, numPartitions -> 1, minCount -> 5,
    stepSize -> 0.025, maxIter -> 1, numThread -> 1,
    pow -> 0.0, sample -> 0, negative -> 5,
    intermediateStorageLevel -> "MEMORY_AND_DISK")

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
 * SkipGram trains a model of `RDD[(String, Vector)]`, i.e. transforms
 * a word into a code for further natural language processing or machine learning process.
 */
@Since("3.4.0")
final class SkipGram @Since("3.4.0") (
                                       @Since("3.4.0") override val uid: String)
  extends Estimator[SkipGramModel] with SkipGramBase with DefaultParamsWritable {

  @Since("3.4.0")
  def this() = this(Identifiable.randomUID("SkipGram"))

  /** @group setParam */
  @Since("3.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("3.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.4.0")
  def setVectorSize(value: Int): this.type = set(vectorSize, value)

  /** @group expertSetParam */
  @Since("3.4.0")
  def setWindowSize(value: Int): this.type = set(windowSize, value)

  /** @group setParam */
  @Since("3.4.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /** @group setParam */
  @Since("3.4.0")
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  @Since("3.4.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("3.4.0")
  def setMinCount(value: Int): this.type = set(minCount, value)

  /** @group setParam */
  @Since("3.4.0")
  def setNegative(value: Int): this.type = set(negative, value)

  /** @group setParam */
  @Since("3.4.0")
  def setSample(value: Double): this.type = set(sample, value)

  /** @group setParam */
  @Since("3.4.0")
  def setPow(value: Double): this.type = set(pow, value)

  /** @group setParam */
  @Since("3.4.0")
  def setNumThread(value: Int): this.type = set(numThread, value)

  /** @group expertSetParam */
  @Since("3.4.0")
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  @Since("3.4.0")
  override def fit(dataset: Dataset[_]): SkipGramModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).rdd.map(_.getSeq[String](0).toArray)
    val wordVectors = new feature.SkipGram()
      .setLearningRate($(stepSize))
      .setMinCount($(minCount))
      .setNumIterations($(maxIter))
      .setNumPartitions($(numPartitions))
      .setNumThread($(numThread))
      .setNegative($(negative))
      .setPow($(pow))
      .setSample($(sample))
      .setVectorSize($(vectorSize))
      .setWindowSize($(windowSize))
      .setIntermediateRDDStorageLevel(StorageLevel.fromString($(intermediateStorageLevel)))
      .fit(input)
    copyValues(new SkipGramModel(uid, wordVectors).setParent(this))
  }

  @Since("3.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): SkipGram = defaultCopy(extra)
}

@Since("3.4.0")
object SkipGram extends DefaultParamsReadable[SkipGram] {

  @Since("3.4.0")
  override def load(path: String): SkipGram = super.load(path)
}

/**
 * Model fitted by [[SkipGram]].
 */
@Since("3.4.0")
class SkipGramModel private[ml] (
                                  @Since("3.4.0") override val uid: String,
                                  @transient private val wordVectors: feature.SkipGramModel)
  extends Model[SkipGramModel] with SkipGramBase with MLWritable {

  import SkipGramModel._

  /**
   * Returns a dataframe with two fields, "word" and "vector", with "word" being a String and
   * and the vector the DenseVector that it is mapped to.
   */
  @Since("3.4.0")
  @transient lazy val getVectors: DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val wordVec = wordVectors.getVectors
    wordVec.toDF("word", "vector")
  }

  /**
   * Find "num" number of words closest in similarity to the given word, not
   * including the word itself.
   * @return a dataframe with columns "word" and "similarity" of the word and the cosine
   * similarities between the synonyms and the given word.
   */
  @Since("3.4.0")
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
  @Since("3.4.0")
  def findSynonyms(vec: Array[Float], num: Int): DataFrame = {
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
  @Since("3.4.0")
  def findSynonymsArray(vec: Array[Float], num: Int): Array[(String, Double)] = {
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
  @Since("3.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("3.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Transform a sentence column to a vector column to represent the whole sentence. The transform
   * is performed by averaging all word vectors it contains.
   */
  @Since("3.4.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    
    val datasetWithI = dataset.withColumn("i", monotonically_increasing_id())
    
    val sentenceAvg = datasetWithI
      .select(col("i"), explode(col($(inputCol))).alias("word"))
      .join(this.getVectors, Seq("word"), "inner")
      .groupBy("i")
      .agg(array((0 until $(vectorSize))
        .map(j => avg(col("vector").getItem(j))): _*).alias($(outputCol)))
    
    datasetWithI
      .join(sentenceAvg, Seq("i"), "full_outer")
      .drop("i")
      .withColumn($(outputCol), col($(outputCol)), outputSchema($(outputCol)).metadata)
  }

  @Since("3.4.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), $(vectorSize))
    }
    outputSchema
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): SkipGramModel = {
    val copied = new SkipGramModel(uid, wordVectors)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("3.4.0")
  override def write: MLWriter = new SkipGramModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"SkipGramModel: uid=$uid, numWords=${wordVectors.getVectors.count()}, " +
      s"vectorSize=${$(vectorSize)}"
  }
}

@Since("3.4.0")
object SkipGramModel extends MLReadable[SkipGramModel] {

  private[SkipGramModel] case class Data(word: String, vector: Array[Float])

  private[SkipGramModel]
  class SkipGramModelWriter(instance: SkipGramModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      val wordVectors = instance.wordVectors.getVectors
      val dataPath = new Path(path, "data").toString
      val bufferSizeInBytes = Utils.byteStringAsBytes(
        sc.conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "64m"))
      val numPartitions = SkipGramModelWriter.calculateNumberOfPartitions(
        bufferSizeInBytes, instance.getVectors.count(), instance.getVectorSize)
      val spark = sparkSession
      import spark.implicits._
      wordVectors
        .repartition(numPartitions)
        .map { case (word, vector) => Data(word, vector) }
        .toDF()
        .write
        .parquet(dataPath)
    }
  }

  private[feature]
  object SkipGramModelWriter {
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
                                     numWords: Long,
                                     vectorSize: Int): Int = {
      val floatSize = 4L  // Use Long to help avoid overflow
      val averageWordSize = 15
      // Calculate the approximate size of the model.
      // Assuming an average word size of 15 bytes, the formula is:
      // (floatSize * vectorSize + 15) * numWords
      val approximateSizeInBytes = (floatSize * vectorSize + averageWordSize) * numWords
      val numPartitions = (approximateSizeInBytes / bufferSizeInBytes) + 1
      require(numPartitions < 10e8, s"SkipGramModel calculated that it needs $numPartitions " +
        s"partitions to save this model, which is too large.  Try increasing " +
        s"spark.kryoserializer.buffer.max so that SkipGramModel can use fewer partitions.")
      numPartitions.toInt
    }
  }

  private class SkipGramModelReader extends MLReader[SkipGramModel] {

    private val className = classOf[SkipGramModel].getName

    override def load(path: String): SkipGramModel = {
      val spark = sparkSession
      import spark.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val (major, minor) = VersionUtils.majorMinorVersion(metadata.sparkVersion)

      val dataPath = new Path(path, "data").toString

      val model = new SkipGramModel(
        metadata.uid,
        new feature.SkipGramModel(spark.read.parquet(dataPath).as[Data]
          .map(x => x.word -> x.vector).rdd)
      )
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("3.4.0")
  override def read: MLReader[SkipGramModel] = new SkipGramModelReader

  @Since("3.4.0")
  override def load(path: String): SkipGramModel = super.load(path)
}
