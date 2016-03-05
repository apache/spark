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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params, IntParam}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
//import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, RegexTokenizer}
//import org.apache.spark.ml.feature.StopWordsRemover
//import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.{functions => f}
import scala.util.Try
import scala.util.matching.Regex
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

/**
 * Params for [[Word2Phrase]] and [[Word2PhraseModel]].
 * currently not being used
 */ 
private[feature] trait Word2PhraseParams extends Params with HasInputCol with HasOutputCol {

  /**
   * minimum number of occurrences before word is counted
   * Default: 100
   * @group param
   */
  val minCount: IntParam = new IntParam(this, "minCount",
    "minimum word occurrence")

  /** @group getParam */
  def getMinCount: Int = $(minCount)

  val minWords: IntParam = new IntParam(this, "minWords",
    "minimum word count before it's counted")

  /* var bigram_list = Array(("default", "val"))

  def printBigramList() : Unit = {

    println(bigram_list.deep.mkString("\n"))
  } */

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

  /* override def validateParams(): Unit = {
    require($(minCount) < $(threshold), s"The specified minCount(${$(minCount)}) is larger or equal to threshold(${$(threshold)})")
  } */
}

/**
 * :: Experimental ::
 * Creates a training model for word2phrase
 * counts the number of words and creates a mapping of words to bigrams
 */
@Experimental
class Word2Phrase(override val uid: String)
  extends Estimator[Word2PhraseModel] with Word2PhraseParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("minCountthresholdScal"))

  setDefault(minCount -> 100, threshold -> 0.00001, minWords -> 5)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMinCount(value: Int): this.type = set(minCount, value)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  def setMinWords(value: Int): this.type = set(minWords, value)

  override def fit(dataset: DataFrame): Word2PhraseModel = {

    import dataset.sqlContext.implicits._
    var sqlContext = dataset.sqlContext

    val tokenizer = new RegexTokenizer().setInputCol($(inputCol)).setOutputCol("words").setPattern("\\W")
    val inputColName = $(inputCol)
    val wordsData = tokenizer.transform(dataset)
    wordsData.registerTempTable("wordsData")

    var ind = wordsData.select(s"$inputColName")
    // counts the number of times each word appears
    var counts = ind.rdd.flatMap(line => line(0).asInstanceOf[String].toLowerCase.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _).toDF("word", "count")
    counts.registerTempTable("wordCount")

    var ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
    var ngramDataFrame = ngram.transform(wordsData)

    // counts the number of bigrams (w1 w2, w2 w3, etc.)
    var biGramCount = ngramDataFrame.select("ngrams").rdd.flatMap(line => line(0).asInstanceOf[Seq[String]]).map(word => (word, 1)).reduceByKey(_ + _).toDF("biGram", "count")
    biGramCount.registerTempTable("biGramCount")
    sqlContext.sql("select * from biGramCount").show(20)

    val minCountA = $(minCount)
    val minWordsA = $(minWords)
    // calculated the score for each bigram ***************************** minCount and threshold should probably be used in the below formula
    sqlContext.sql(s"select biGram, (bigram_count - $minCountA)/(word1_count * word2_count) as score from (select biGrams.biGram as biGram, biGrams.count as bigram_count, wc1.word as word1, wc1.count as word1_count, wc2.word as word2, wc2.count as word2_count from (Select biGram, count, split(biGram,' ')[0] as word1, split(biGram,' ')[1] as word2 from biGramCount) biGrams inner join wordCount as wc1 on (wc1.word = biGrams.word1) inner join wordCount as wc2 on (wc2.word = biGrams.word2) ) biGramsStats where word2_count > $minWordsA and word1_count > $minWordsA order by score desc").registerTempTable("bi_gram_scores")
    sqlContext.sql("select * from bi_gram_scores").show(20)

    val thresholdA = $(threshold)
    // Scores > threshold
    var biGrams = sqlContext.sql(s"select biGram from bi_gram_scores where score > $thresholdA").collect()
    var bigram_list = biGrams.map(row => (row.toString))
    println(bigram_list.deep.mkString("\n"))

    copyValues(new Word2PhraseModel(uid, bigram_list).setParent(this))
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
    val bigram_list: Array[String])
  extends Model[Word2PhraseModel] with Word2PhraseParams with MLWritable {

  import Word2PhraseModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {

    var mapBiGrams = udf((t: String) => bigram_list.foldLeft(t){case (z, r) => z.replaceAll("(?i)"+r, r.split(" ").mkString("_"))})
    dataset.withColumn($(outputCol), mapBiGrams(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Word2PhraseModel = {
    val copied = new Word2PhraseModel(uid, bigram_list)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new Word2PhraseModelWriter(this)
}

@Since("1.6.0")
object Word2PhraseModel extends MLReadable[Word2PhraseModel] {

  private[Word2PhraseModel]
  class Word2PhraseModelWriter(instance: Word2PhraseModel) extends MLWriter {

    //private case class Data(bigram_list: Array[(String, String)])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      //val data = new Data(instance.bigram_list)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame((instance.bigram_list).toSeq).write.parquet(dataPath)
    }
  }

  private class Word2PhraseModelReader extends MLReader[Word2PhraseModel] {

    private val className = classOf[Word2PhraseModel].getName

    override def load(path: String): Word2PhraseModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
      //var holder = Array(("ok", "ok"), ("ok", "ok"), ("o", "oO"))
           // val model = new Word2PhraseModel(metadata.uid, data.map((bigram:String, bigram_broken:String) => (bigram, bigram_broken)).collect())

      val model = new Word2PhraseModel(metadata.uid, data.map(row => (row(0).toString)).collect())
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[Word2PhraseModel] = new Word2PhraseModelReader

  @Since("1.6.0")
  override def load(path: String): Word2PhraseModel = super.load(path)
}
