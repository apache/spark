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
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class BisectingKMeansWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val size: Array[Long],
    val isLoaded: Boolean = false) extends MLWritable {
  private val bisectingKmeansModel: BisectingKMeansModel =
    pipeline.stages.last.asInstanceOf[BisectingKMeansModel]

  lazy val coefficients: Array[Double] = bisectingKmeansModel.clusterCenters.flatMap(_.toArray)

  lazy val k: Int = bisectingKmeansModel.getK

  // If the model is loaded from a saved model, cluster is NULL. It is checked on R side
  lazy val cluster: DataFrame = bisectingKmeansModel.summary.cluster

  def fitted(method: String): DataFrame = {
    if (method == "centers") {
      bisectingKmeansModel.summary.predictions.drop(bisectingKmeansModel.getFeaturesCol)
    } else if (method == "classes") {
      bisectingKmeansModel.summary.cluster
    } else {
      throw new UnsupportedOperationException(
        s"Method (centers or classes) required but $method found.")
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(bisectingKmeansModel.getFeaturesCol)
  }

  override def write: MLWriter = new BisectingKMeansWrapper.BisectingKMeansWrapperWriter(this)
}

private[r] object BisectingKMeansWrapper extends MLReadable[BisectingKMeansWrapper] {

  def fit(
      data: DataFrame,
      formula: String,
      k: Int,
      maxIter: Int,
      seed: String,
      minDivisibleClusterSize: Double
      ): BisectingKMeansWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setFeaturesCol("features")
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    val bisectingKmeans = new BisectingKMeans()
      .setK(k)
      .setMaxIter(maxIter)
      .setMinDivisibleClusterSize(minDivisibleClusterSize)
      .setFeaturesCol(rFormula.getFeaturesCol)

    if (seed != null && seed.length > 0) bisectingKmeans.setSeed(seed.toInt)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, bisectingKmeans))
      .fit(data)

    val bisectingKmeansModel: BisectingKMeansModel =
      pipeline.stages.last.asInstanceOf[BisectingKMeansModel]
    val size: Array[Long] = bisectingKmeansModel.summary.clusterSizes

    new BisectingKMeansWrapper(pipeline, features, size)
  }

  override def read: MLReader[BisectingKMeansWrapper] = new BisectingKMeansWrapperReader

  override def load(path: String): BisectingKMeansWrapper = super.load(path)

  class BisectingKMeansWrapperWriter(instance: BisectingKMeansWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toSeq) ~
        ("size" -> instance.size.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))

      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)
      instance.pipeline.save(pipelinePath)
    }
  }

  class BisectingKMeansWrapperReader extends MLReader[BisectingKMeansWrapper] {

    override def load(path: String): BisectingKMeansWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val size = (rMetadata \ "size").extract[Array[Long]]
      new BisectingKMeansWrapper(pipeline, features, size, isLoaded = true)
    }
  }

}
