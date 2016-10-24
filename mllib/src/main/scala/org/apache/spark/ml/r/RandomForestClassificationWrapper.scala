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
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class RandomForestClassifierWrapper private (
  val pipeline: PipelineModel,
  val formula: String,
  val features: Array[String]) extends MLWritable {

  private val DTModel: RandomForestClassificationModel =
    pipeline.stages(1).asInstanceOf[RandomForestClassificationModel]

  lazy val numFeatures: Int = DTModel.numFeatures
  lazy val featureImportances: Vector = DTModel.featureImportances
  lazy val numTrees: Int = DTModel.getNumTrees
  lazy val treeWeights: Array[Double] = DTModel.treeWeights

  def summary: String = DTModel.toDebugString

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(DTModel.getFeaturesCol)
  }

  override def write: MLWriter = new
      RandomForestClassifierWrapper.RandomForestClassifierWrapperWriter(this)
}

private[r] object RandomForestClassifierWrapper extends MLReadable[RandomForestClassifierWrapper] {
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
      probabilityCol: String,
      maxMemoryInMB: Int,
      cacheNodeIds: Boolean): RandomForestClassifierWrapper = {

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
    val rfc = new RandomForestClassifier()
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
      .setProbabilityCol(probabilityCol)
      .setFeaturesCol(rFormula.getFeaturesCol)
    if (seed != null && seed.length > 0) rfc.setSeed(seed.toLong)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, rfc))
      .fit(data)

    new RandomForestClassifierWrapper(pipeline, formula, features)
  }

  override def read: MLReader[RandomForestClassifierWrapper] =
    new RandomForestClassifierWrapperReader

  override def load(path: String): RandomForestClassifierWrapper = super.load(path)

  class RandomForestClassifierWrapperWriter(instance: RandomForestClassifierWrapper)
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

  class RandomForestClassifierWrapperReader extends MLReader[RandomForestClassifierWrapper] {

    override def load(path: String): RandomForestClassifierWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val formula = (rMetadata \ "formula").extract[String]
      val features = (rMetadata \ "features").extract[Array[String]]

      new RandomForestClassifierWrapper(pipeline, formula, features)
    }
  }
}
