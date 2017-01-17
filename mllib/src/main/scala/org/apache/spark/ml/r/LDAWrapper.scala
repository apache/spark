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

package org.apache.spark.ml.r

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


private[r] class LDAWrapper private (
    val pipeline: PipelineModel,
    val logLikelihood: Double,
    val logPerplexity: Double,
    val vocabulary: Array[String]) extends MLWritable {

  import LDAWrapper._

  private val lda: LDAModel = pipeline.stages.last.asInstanceOf[LDAModel]
  private val preprocessor: PipelineModel =
    new PipelineModel(s"${Identifiable.randomUID(pipeline.uid)}", pipeline.stages.dropRight(1))

  def transform(data: Dataset[_]): DataFrame = {
    val vec2ary = udf { vec: Vector => vec.toArray }
    val outputCol = lda.getTopicDistributionCol
    val tempCol = s"${Identifiable.randomUID(outputCol)}"
    val preprocessed = preprocessor.transform(data)
    lda.transform(preprocessed, ParamPair(lda.topicDistributionCol, tempCol))
      .withColumn(outputCol, vec2ary(col(tempCol)))
      .drop(TOKENIZER_COL, STOPWORDS_REMOVER_COL, COUNT_VECTOR_COL, tempCol)
  }

  def computeLogPerplexity(data: Dataset[_]): Double = {
    lda.logPerplexity(preprocessor.transform(data))
  }

  def topics(maxTermsPerTopic: Int): DataFrame = {
    val topicIndices: DataFrame = lda.describeTopics(maxTermsPerTopic)
    if (vocabulary.isEmpty || vocabulary.length < vocabSize) {
      topicIndices
    } else {
      val index2term = udf { indices: mutable.WrappedArray[Int] => indices.map(i => vocabulary(i)) }
      topicIndices
        .select(col("topic"), index2term(col("termIndices")).as("term"), col("termWeights"))
    }
  }

  lazy val isDistributed: Boolean = lda.isDistributed
  lazy val vocabSize: Int = lda.vocabSize
  lazy val docConcentration: Array[Double] = lda.getEffectiveDocConcentration
  lazy val topicConcentration: Double = lda.getEffectiveTopicConcentration

  override def write: MLWriter = new LDAWrapper.LDAWrapperWriter(this)
}

private[r] object LDAWrapper extends MLReadable[LDAWrapper] {

  val TOKENIZER_COL = s"${Identifiable.randomUID("rawTokens")}"
  val STOPWORDS_REMOVER_COL = s"${Identifiable.randomUID("tokens")}"
  val COUNT_VECTOR_COL = s"${Identifiable.randomUID("features")}"

  private def getPreStages(
      features: String,
      customizedStopWords: Array[String],
      maxVocabSize: Int): Array[PipelineStage] = {
    val tokenizer = new RegexTokenizer()
      .setInputCol(features)
      .setOutputCol(TOKENIZER_COL)
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(TOKENIZER_COL)
      .setOutputCol(STOPWORDS_REMOVER_COL)
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)
    val countVectorizer = new CountVectorizer()
      .setVocabSize(maxVocabSize)
      .setInputCol(STOPWORDS_REMOVER_COL)
      .setOutputCol(COUNT_VECTOR_COL)

    Array(tokenizer, stopWordsRemover, countVectorizer)
  }

  def fit(
      data: DataFrame,
      features: String,
      k: Int,
      maxIter: Int,
      optimizer: String,
      subsamplingRate: Double,
      topicConcentration: Double,
      docConcentration: Array[Double],
      customizedStopWords: Array[String],
      maxVocabSize: Int): LDAWrapper = {

    val lda = new LDA()
      .setK(k)
      .setMaxIter(maxIter)
      .setSubsamplingRate(subsamplingRate)
      .setOptimizer(optimizer)

    val featureSchema = data.schema(features)
    val stages = featureSchema.dataType match {
      case d: StringType =>
        getPreStages(features, customizedStopWords, maxVocabSize) ++
          Array(lda.setFeaturesCol(COUNT_VECTOR_COL))
      case d: VectorUDT =>
        Array(lda.setFeaturesCol(features))
      case _ =>
        throw new SparkException(
          s"Unsupported input features type of ${featureSchema.dataType.typeName}," +
            s" only String type and Vector type are supported now.")
    }

    if (topicConcentration != -1) {
      lda.setTopicConcentration(topicConcentration)
    } else {
      // Auto-set topicConcentration
    }

    if (docConcentration.length == 1) {
      if (docConcentration.head != -1) {
        lda.setDocConcentration(docConcentration.head)
      } else {
        // Auto-set docConcentration
      }
    } else {
      lda.setDocConcentration(docConcentration)
    }

    val pipeline = new Pipeline().setStages(stages)
    val model = pipeline.fit(data)

    val vocabulary: Array[String] = featureSchema.dataType match {
      case d: StringType =>
        val countVectorModel = model.stages(2).asInstanceOf[CountVectorizerModel]
        countVectorModel.vocabulary
      case _ => Array.empty[String]
    }

    val ldaModel: LDAModel = model.stages.last.asInstanceOf[LDAModel]
    val preprocessor: PipelineModel =
      new PipelineModel(s"${Identifiable.randomUID(pipeline.uid)}", model.stages.dropRight(1))

    val preprocessedData = preprocessor.transform(data)

    new LDAWrapper(
      model,
      ldaModel.logLikelihood(preprocessedData),
      ldaModel.logPerplexity(preprocessedData),
      vocabulary)
  }

  override def read: MLReader[LDAWrapper] = new LDAWrapperReader

  override def load(path: String): LDAWrapper = super.load(path)

  class LDAWrapperWriter(instance: LDAWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("logLikelihood" -> instance.logLikelihood) ~
        ("logPerplexity" -> instance.logPerplexity) ~
        ("vocabulary" -> instance.vocabulary.toList)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class LDAWrapperReader extends MLReader[LDAWrapper] {

    override def load(path: String): LDAWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val logLikelihood = (rMetadata \ "logLikelihood").extract[Double]
      val logPerplexity = (rMetadata \ "logPerplexity").extract[Double]
      val vocabulary = (rMetadata \ "vocabulary").extract[List[String]].toArray

      val pipeline = PipelineModel.load(pipelinePath)
      new LDAWrapper(pipeline, logLikelihood, logPerplexity, vocabulary)
    }
  }
}
