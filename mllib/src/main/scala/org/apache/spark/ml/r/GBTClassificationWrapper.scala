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
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class GBTClassifierWrapper private (
  val pipeline: PipelineModel,
  val formula: String,
  val features: Array[String]) extends MLWritable {

  import GBTClassifierWrapper._

  private val gbtcModel: GBTClassificationModel =
    pipeline.stages(1).asInstanceOf[GBTClassificationModel]

  lazy val numFeatures: Int = gbtcModel.numFeatures
  lazy val featureImportances: Vector = gbtcModel.featureImportances
  lazy val numTrees: Int = gbtcModel.getNumTrees
  lazy val treeWeights: Array[Double] = gbtcModel.treeWeights
  lazy val maxDepth: Int = gbtcModel.getMaxDepth

  def summary: String = gbtcModel.toDebugString

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(gbtcModel.getFeaturesCol)
      .drop(gbtcModel.getLabelCol)
  }

  override def write: MLWriter = new
      GBTClassifierWrapper.GBTClassifierWrapperWriter(this)
}

private[r] object GBTClassifierWrapper extends MLReadable[GBTClassifierWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

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
      cacheNodeIds: Boolean): GBTClassifierWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setForceIndexLabel(true)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get labels and feature names from output schema
    val (features, labels) = getFeaturesAndLabels(rFormulaModel, data)

    // assemble and fit the pipeline
    val rfc = new GBTClassifier()
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
      .setLabelCol(rFormula.getLabelCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)
    if (seed != null && seed.length > 0) rfc.setSeed(seed.toLong)

    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, rfc, idxToStr))
      .fit(data)

    new GBTClassifierWrapper(pipeline, formula, features)
  }

  override def read: MLReader[GBTClassifierWrapper] = new GBTClassifierWrapperReader

  override def load(path: String): GBTClassifierWrapper = super.load(path)

  class GBTClassifierWrapperWriter(instance: GBTClassifierWrapper)
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

  class GBTClassifierWrapperReader extends MLReader[GBTClassifierWrapper] {

    override def load(path: String): GBTClassifierWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val formula = (rMetadata \ "formula").extract[String]
      val features = (rMetadata \ "features").extract[Array[String]]

      new GBTClassifierWrapper(pipeline, formula, features)
    }
  }
}
