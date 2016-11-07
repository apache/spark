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
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

private[r] class GaussianMixtureWrapper private (
    val pipeline: PipelineModel,
    val dim: Int,
    val isLoaded: Boolean = false) extends MLWritable {

  private val gmm: GaussianMixtureModel = pipeline.stages(1).asInstanceOf[GaussianMixtureModel]

  lazy val k: Int = gmm.getK

  lazy val lambda: Array[Double] = gmm.weights

  lazy val mu: Array[Double] = gmm.gaussians.flatMap(_.mean.toArray)

  lazy val sigma: Array[Double] = gmm.gaussians.flatMap(_.cov.toArray)

  lazy val vectorToArray = udf { probability: Vector => probability.toArray }
  lazy val posterior: DataFrame = gmm.summary.probability
    .withColumn("posterior", vectorToArray(col(gmm.summary.probabilityCol)))
    .drop(gmm.summary.probabilityCol)

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(gmm.getFeaturesCol)
  }

  override def write: MLWriter = new GaussianMixtureWrapper.GaussianMixtureWrapperWriter(this)

}

private[r] object GaussianMixtureWrapper extends MLReadable[GaussianMixtureWrapper] {

  def fit(
      data: DataFrame,
      formula: String,
      k: Int,
      maxIter: Int,
      tol: Double): GaussianMixtureWrapper = {

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
    val dim = features.length

    val gm = new GaussianMixture()
      .setK(k)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFeaturesCol(rFormula.getFeaturesCol)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, gm))
      .fit(data)

    new GaussianMixtureWrapper(pipeline, dim)
  }

  override def read: MLReader[GaussianMixtureWrapper] = new GaussianMixtureWrapperReader

  override def load(path: String): GaussianMixtureWrapper = super.load(path)

  class GaussianMixtureWrapperWriter(instance: GaussianMixtureWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("dim" -> instance.dim)
      val rMetadataJson: String = compact(render(rMetadata))

      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)
      instance.pipeline.save(pipelinePath)
    }
  }

  class GaussianMixtureWrapperReader extends MLReader[GaussianMixtureWrapper] {

    override def load(path: String): GaussianMixtureWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val dim = (rMetadata \ "dim").extract[Int]
      new GaussianMixtureWrapper(pipeline, dim, isLoaded = true)
    }
  }
}
