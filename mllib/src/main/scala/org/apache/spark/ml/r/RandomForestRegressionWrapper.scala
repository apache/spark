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
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class RandomForestRegressorWrapper private (
  val pipeline: PipelineModel,
  val formula: String,
  val features: Array[String]) extends MLWritable {

  private val rfrModel: RandomForestRegressionModel =
    pipeline.stages(1).asInstanceOf[RandomForestRegressionModel]

  lazy val numFeatures: Int = rfrModel.numFeatures
  lazy val featureImportances: Vector = rfrModel.featureImportances
  lazy val numTrees: Int = rfrModel.getNumTrees
  lazy val treeWeights: Array[Double] = rfrModel.treeWeights
  lazy val maxDepth: Int = rfrModel.getMaxDepth

  def summary: String = rfrModel.toDebugString

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(rfrModel.getFeaturesCol)
  }

  override def write: MLWriter = new
      RandomForestRegressorWrapper.RandomForestRegressorWrapperWriter(this)
}

private[r] object RandomForestRegressorWrapper extends MLReadable[RandomForestRegressorWrapper] {
  def fit(  // scalastyle:ignore
      data: DataFrame,
      formula: String,
      maxDepth: Int,
      maxBins: Int,
      numTrees: Int,
      impurity: String,
      minInstancesPerNode: Int,
      minInfoGain: Double,
      checkpointInterval: Int,
      featureSubsetStrategy: String,
      seed: String,
      subsamplingRate: Double,
      maxMemoryInMB: Int,
      cacheNodeIds: Boolean,
      bootstrap: Boolean): RandomForestRegressorWrapper = {

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
    val rfr = new RandomForestRegressor()
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setNumTrees(numTrees)
      .setImpurity(impurity)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinInfoGain(minInfoGain)
      .setCheckpointInterval(checkpointInterval)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setSubsamplingRate(subsamplingRate)
      .setMaxMemoryInMB(maxMemoryInMB)
      .setCacheNodeIds(cacheNodeIds)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setBootstrap(bootstrap)
    if (seed != null && seed.length > 0) rfr.setSeed(seed.toLong)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, rfr))
      .fit(data)

    new RandomForestRegressorWrapper(pipeline, formula, features)
  }

  override def read: MLReader[RandomForestRegressorWrapper] = new RandomForestRegressorWrapperReader

  override def load(path: String): RandomForestRegressorWrapper = super.load(path)

  class RandomForestRegressorWrapperWriter(instance: RandomForestRegressorWrapper)
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

  class RandomForestRegressorWrapperReader extends MLReader[RandomForestRegressorWrapper] {

    override def load(path: String): RandomForestRegressorWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val formula = (rMetadata \ "formula").extract[String]
      val features = (rMetadata \ "features").extract[Array[String]]

      new RandomForestRegressorWrapper(pipeline, formula, features)
    }
  }
}
