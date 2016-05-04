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
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class KMeansWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val size: Array[Long],
    val isLoaded: Boolean = false) extends MLWritable {

  private val kMeansModel: KMeansModel = pipeline.stages(1).asInstanceOf[KMeansModel]

  lazy val coefficients: Array[Double] = kMeansModel.clusterCenters.flatMap(_.toArray)

  lazy val k: Int = kMeansModel.getK

  lazy val cluster: DataFrame = kMeansModel.summary.cluster

  def fitted(method: String): DataFrame = {
    if (method == "centers") {
      kMeansModel.summary.predictions.drop(kMeansModel.getFeaturesCol)
    } else if (method == "classes") {
      kMeansModel.summary.cluster
    } else {
      throw new UnsupportedOperationException(
        s"Method (centers or classes) required but $method found.")
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(kMeansModel.getFeaturesCol)
  }

  override def write: MLWriter = new KMeansWrapper.KMeansWrapperWriter(this)
}

private[r] object KMeansWrapper extends MLReadable[KMeansWrapper] {

  def fit(
      data: DataFrame,
      formula: String,
      k: Int,
      maxIter: Int,
      initMode: String): KMeansWrapper = {

    val rFormulaModel = new RFormula()
      .setFormula(formula)
      .setFeaturesCol("features")
      .fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    val kMeans = new KMeans()
      .setK(k)
      .setMaxIter(maxIter)
      .setInitMode(initMode)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, kMeans))
      .fit(data)

    val kMeansModel: KMeansModel = pipeline.stages(1).asInstanceOf[KMeansModel]
    val size: Array[Long] = kMeansModel.summary.clusterSizes

    new KMeansWrapper(pipeline, features, size)
  }

  override def read: MLReader[KMeansWrapper] = new KMeansWrapperReader

  override def load(path: String): KMeansWrapper = super.load(path)

  class KMeansWrapperWriter(instance: KMeansWrapper) extends MLWriter {

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

  class KMeansWrapperReader extends MLReader[KMeansWrapper] {

    override def load(path: String): KMeansWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val size = (rMetadata \ "size").extract[Array[Long]]
      new KMeansWrapper(pipeline, features, size, isLoaded = true)
    }
  }
}
