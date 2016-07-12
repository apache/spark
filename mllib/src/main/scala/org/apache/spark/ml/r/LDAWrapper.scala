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

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class LDAWrapper private (
    val pipeline: PipelineModel,
    val likelihood: Double,
    val perplexity: Double) extends MLWritable {

  private val lda: LDAModel = pipeline.stages(0).asInstanceOf[LDAModel]
  private val preprocessor: PipelineModel =
    new PipelineModel(s"${pipeline.uid}-preprocessor", pipeline.stages.dropRight(1))

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(lda.getFeaturesCol)
  }

  def perplexityFor(data: Dataset[_]): Double = {
    math.exp(lda.logPerplexity(preprocessor.transform(data)))
  }

  lazy val isDistributed: Boolean = lda.isDistributed
  lazy val described: DataFrame = lda.describeTopics()
  lazy val vocabSize: Int = lda.vocabSize
  lazy val docConcentration: Array[Double] = lda.getEffectiveDocConcentration
  lazy val topicConcentration: Double = lda.getEffectiveTopicConcentration

  override def write: MLWriter = new LDAWrapper.LDAWrapperWriter(this)
}

private[r] object LDAWrapper extends MLReadable[LDAWrapper] {

  def fit(
      data: DataFrame,
      features: String,
      k: Int,
      maxIter: Int,
      optimizer: String,
      subsamplingRate: Double,
      topicConcentration: Double,
      docConcentration: Array[Double]): LDAWrapper = {

    val lda = new LDA()
      .setFeaturesCol(features)
      .setK(k)
      .setMaxIter(maxIter)
      .setSubsamplingRate(subsamplingRate)

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

    val pipeline = new Pipeline().setStages(Array(lda))
    val model = pipeline.fit(data)
    val ldaModel = model.stages(0).asInstanceOf[LDAModel]

    new LDAWrapper(
      model, math.exp(ldaModel.logLikelihood(data)), math.exp(ldaModel.logPerplexity(data)))
  }

  override def read: MLReader[LDAWrapper] = new LDAWrapperReader

  override def load(path: String): LDAWrapper = super.load(path)

  class LDAWrapperWriter(instance: LDAWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("likelihood" -> instance.likelihood) ~
        ("perplexity" -> instance.perplexity)
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
      val logLikelihood = (rMetadata \ "likelihood").extract[Double]
      val logPerplexity = (rMetadata \ "perplexity").extract[Double]

      val pipeline = PipelineModel.load(pipelinePath)
      new LDAWrapper(pipeline, logLikelihood, logPerplexity)
    }
  }
}
