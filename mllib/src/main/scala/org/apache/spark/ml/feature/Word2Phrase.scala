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

import scala.util.matching.Regex
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{functions => f, Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
 * Params for [[Word2Phrase]] and [[Word2PhraseModel]].
 */
private[feature] trait Word2PhraseParams extends Params with HasInputCol with HasOutputCol {

  /**
   * delta
   * Default: 100
   * @group param
   */
  val delta: IntParam = new IntParam(this, "delta",
    "minimum word occurrence")

  /** @group getParam */
  def getDelta: Int = $(delta)

  /**
   * minimum number of occurrences before word is counted
   * Default: 5
   * @group param
   */
   val minWords: IntParam = new IntParam(this, "minWords",
    "minimum word count before it's counted")

  /**
   * threshold for score
   * Default: 0.00001
   * @group param
   */
  val threshold: DoubleParam = new DoubleParam(this, "threshold",
    "score threshold")

  /** @group getParam */
  def getThreshold: Double = $(threshold)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    validateParams()
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

/**
 * :: Experimental ::
 * Creates a training model for word2phrase
 * Uses the Word2Phrase algorithm to determine which words to turn into phrases
 */
@Experimental
class Word2Phrase(override val uid: String)
  extends Estimator[Word2PhraseModel] with Word2PhraseParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("deltathresholdScal"))

  setDefault(delta -> 100, threshold -> 0.00001, minWords -> 5)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setDelta(value: Int): this.type = set(delta, value)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  def setMinWords(value: Int): this.type = set(minWords, value)

  override def fit(dataset: DataFrame): Word2PhraseModel = {

    import dataset.sqlContext.implicits._
    val sqlContext = dataset.sqlContext

    val tokenizer = new RegexTokenizer().setInputCol($(inputCol)).setOutputCol("words")
                  .setPattern("\\W")
    val wordsData = tokenizer.transform(dataset)

    val inputColName = $(inputCol)
    val ind = wordsData.select(s"$inputColName")

    // counts the number of times each word appears
    val counts = ind.rdd.flatMap(line => line(0).asInstanceOf[String].toLowerCase.split("\\s+"))
                .map(word => (word, 1)).reduceByKey(_ + _).toDF("word", "count")
    val wordCountName = Identifiable.randomUID("wc")
    counts.registerTempTable(wordCountName)

    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordsData)

    // counts the number of bigrams (w1 w2, w2 w3, etc.)
    val biGramCount = ngramDataFrame.select("ngrams").rdd.flatMap(line => line(0).
      asInstanceOf[Seq[String]]).map(word => (word, 1)).reduceByKey(_ + _).
      toDF("biGram", "count")
    val biGramCountName = Identifiable.randomUID("bgc")
    biGramCount.registerTempTable(biGramCountName)

    // calculate the score for each bigram
    val deltaA = $(delta)
    val minWordsA = $(minWords)
    val biGramScoresName = Identifiable.randomUID("bgs")
    sqlContext.sql(s"""select biGram, (bigram_count - $deltaA)/(word1_count * word2_count)
      as score from (select biGrams.biGram as biGram, biGrams.count as bigram_count,
      wc1.word as word1, wc1.count as word1_count, wc2.word as word2,
      wc2.count as word2_count from (Select biGram, count, split(biGram,' ')[0] as word1,
      split(biGram,' ')[1] as word2 from $biGramCountName) biGrams inner join $wordCountName
      as wc1 on (wc1.word = biGrams.word1) inner join $wordCountName as wc2 on
      (wc2.word = biGrams.word2)) biGramsStats where word2_count > $minWordsA and
      word1_count > $minWordsA order by score desc""").registerTempTable(biGramScoresName)

    val thresholdA = $(threshold)
    // Scores > threshold
    val biGrams = sqlContext.sql(s"""select biGram from $biGramScoresName where
                                  score > $thresholdA""").collect()
    val bigramList = biGrams.map(row => (row(0).toString))

    copyValues(new Word2PhraseModel(uid, bigramList).setParent(this))

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Word2Phrase = defaultCopy(extra)
}

@Since("1.6.0")
object Word2Phrase extends DefaultParamsReadable[Word2Phrase] {

  @Since("1.6.0")
  override def load(path: String): Word2Phrase = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by [[Word2Phrase]].
 */
@Experimental
class Word2PhraseModel private[ml] (
    override val uid: String,
    val bigramList: Array[String])
  extends Model[Word2PhraseModel] with Word2PhraseParams with MLWritable {

  import Word2PhraseModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {

    var mapBiGrams = udf((t: String) => bigramList.foldLeft(t){case (z, r) =>
                                z.replaceAll("(?i)" + Regex.quote(r), r.split(" ").mkString("_"))})
    dataset.withColumn($(outputCol), mapBiGrams(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Word2PhraseModel = {
    val copied = new Word2PhraseModel(uid, bigramList)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new Word2PhraseModelWriter(this)
}

@Since("1.6.0")
object Word2PhraseModel extends MLReadable[Word2PhraseModel] {

  private[Word2PhraseModel]
  class Word2PhraseModelWriter(instance: Word2PhraseModel) extends MLWriter {

    private case class Data(bigramList: Seq[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.bigramList)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class Word2PhraseModelReader extends MLReader[Word2PhraseModel] {

    private val className = classOf[Word2PhraseModel].getName

    override def load(path: String): Word2PhraseModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath).select("bigramList").head()
      val bigramList = data.getAs[Seq[String]](0).toArray
      val model = new Word2PhraseModel(metadata.uid, bigramList)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[Word2PhraseModel] = new Word2PhraseModelReader

  @Since("1.6.0")
  override def load(path: String): Word2PhraseModel = super.load(path)
}
