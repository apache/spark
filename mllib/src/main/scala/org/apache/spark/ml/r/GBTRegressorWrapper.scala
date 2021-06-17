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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class GBTRegressorWrapper private (
  val pipeline: PipelineModel,
  val formula: String,
  val features: Array[String]) extends MLWritable {

  private val gbtrModel: GBTRegressionModel =
    pipeline.stages(1).asInstanceOf[GBTRegressionModel]

  lazy val numFeatures: Int = gbtrModel.numFeatures
  lazy val featureImportances: Vector = gbtrModel.featureImportances
  lazy val numTrees: Int = gbtrModel.getNumTrees
  lazy val treeWeights: Array[Double] = gbtrModel.treeWeights
  lazy val maxDepth: Int = gbtrModel.getMaxDepth

  def summary: String = gbtrModel.toDebugString

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(gbtrModel.getFeaturesCol)
  }

  override def write: MLWriter = new
      GBTRegressorWrapper.GBTRegressorWrapperWriter(this)
}

private[r] object GBTRegressorWrapper extends MLReadable[GBTRegressorWrapper] {
  def fit(  // scalastyle:ignore
      data: DataFrame,
      formula: String,
      maxDepth: Int,
      maxBins: Int,
      maxIter: Int,
      stepSize: Double,
      minInstancesPerNode: Int,
      minInfoGain: Double,
      checkpointInterval: Int,
      lossType: String,
      seed: String,
      subsamplingRate: Double,
      maxMemoryInMB: Int,
      cacheNodeIds: Boolean): GBTRegressorWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    // assemble and fit the pipeline
    val rfr = new GBTRegressor()
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setMaxIter(maxIter)
      .setStepSize(stepSize)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinInfoGain(minInfoGain)
      .setCheckpointInterval(checkpointInterval)
      .setLossType(lossType)
      .setSubsamplingRate(subsamplingRate)
      .setMaxMemoryInMB(maxMemoryInMB)
      .setCacheNodeIds(cacheNodeIds)
      .setFeaturesCol(rFormula.getFeaturesCol)
    if (seed != null && seed.length > 0) rfr.setSeed(seed.toLong)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, rfr))
      .fit(data)

    new GBTRegressorWrapper(pipeline, formula, features)
  }

  override def read: MLReader[GBTRegressorWrapper] = new GBTRegressorWrapperReader

  override def load(path: String): GBTRegressorWrapper = super.load(path)

  class GBTRegressorWrapperWriter(instance: GBTRegressorWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("formula" -> instance.formula) ~
        ("features" -> instance.features.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))

      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)
      instance.pipeline.save(pipelinePath)
    }
  }

  class GBTRegressorWrapperReader extends MLReader[GBTRegressorWrapper] {

    override def load(path: String): GBTRegressorWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val formula = (rMetadata \ "formula").extract[String]
      val features = (rMetadata \ "features").extract[Array[String]]

      new GBTRegressorWrapper(pipeline, formula, features)
    }
  }
}
