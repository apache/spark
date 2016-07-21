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

import org.apache.spark.SparkException
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class ALSWrapper private (
    val pipeline: PipelineModel,
    val rFeatures: Array[String]) extends MLWritable {

  private val alsModel: ALSModel =
    pipeline.stages(1).asInstanceOf[ALSModel]

  lazy val rUserFactors: DataFrame = alsModel.userFactors

  lazy val rItemFactors: DataFrame = alsModel.itemFactors

  override def write: MLWriter =
    new ALSWrapper.ALSWrapperWriter(this)
}

private[r] object ALSWrapper extends MLReadable[ALSWrapper] {

  def fit(formula: String, data: DataFrame, rank: Int, maxIter: Int): ALSWrapper = {
    val rFormula = new RFormula()
      .setFormula(formula)
    val rFormulaModel = rFormula.fit(data)
    // get labels and feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormula.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    // assemble and fit the pipeline
    val als = new ALS()
      .setRank(rank)
      .setMaxIter(maxIter)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, als))
      .fit(data)

    val alsm: ALSModel =
      pipeline.stages(1).asInstanceOf[ALSModel]

    val rFeatures: Array[String] = features

    new ALSWrapper(pipeline, rFeatures)
  }

  class ALSWrapperWriter(instance: ALSWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("rFeatures" -> instance.rFeatures.toSeq)

      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  override def read: MLReader[ALSWrapper] =
    new ALSWrapperReader

  override def load(path: String): ALSWrapper = super.load(path)

  class ALSWrapperReader
    extends MLReader[ALSWrapper] {

    override def load(path: String): ALSWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val rFeatures = (rMetadata \ "rFeatures").extract[Array[String]]
//      val rCoefficients = (rMetadata \ "rCoefficients").extract[Array[Double]]
//      val rDispersion = (rMetadata \ "rDispersion").extract[Double]
//      val rNullDeviance = (rMetadata \ "rNullDeviance").extract[Double]
//      val rDeviance = (rMetadata \ "rDeviance").extract[Double]
//      val rResidualDegreeOfFreedomNull =
      // (rMetadata \ "rResidualDegreeOfFreedomNull").extract[Long]
//      val rResidualDegreeOfFreedom = (rMetadata \ "rResidualDegreeOfFreedom").extract[Long]
//      val rAic = (rMetadata \ "rAic").extract[Double]
//      val rNumIterations = (rMetadata \ "rNumIterations").extract[Int]

      val pipeline = PipelineModel.load(pipelinePath)

//      new GeneralizedLinearRegressionWrapper(pipeline, rFeatures, rCoefficients, rDispersion,
      //        rNullDeviance, rDeviance, rResidualDegreeOfFreedomNull, rResidualDegreeOfFreedom,
      //        rAic, rNumIterations, isLoaded = true)
      new ALSWrapper(pipeline, rFeatures)
    }
  }
}
