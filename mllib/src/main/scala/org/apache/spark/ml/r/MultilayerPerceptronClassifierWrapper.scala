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
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class MultilayerPerceptronClassifierWrapper private (
    val pipeline: PipelineModel,
    val labelCount: Long,
    val layers: Array[Int],
    val weights: Array[Double]
  ) extends MLWritable {

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
  }

  /**
   * Returns an [[MLWriter]] instance for this ML instance.
   */
  override def write: MLWriter =
    new MultilayerPerceptronClassifierWrapper.MultilayerPerceptronClassifierWrapperWriter(this)
}

private[r] object MultilayerPerceptronClassifierWrapper
  extends MLReadable[MultilayerPerceptronClassifierWrapper] {

  val PREDICTED_LABEL_COL = "prediction"

  def fit(
      data: DataFrame,
      blockSize: Int,
      layers: Array[Int],
      solver: String,
      maxIter: Int,
      tol: Double,
      stepSize: Double,
      seed: String,
      initialWeights: Array[Double]
     ): MultilayerPerceptronClassifierWrapper = {
    // get labels and feature names from output schema
    val schema = data.schema

    // assemble and fit the pipeline
    val mlp = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(blockSize)
      .setSolver(solver)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setStepSize(stepSize)
      .setPredictionCol(PREDICTED_LABEL_COL)
    if (seed != null && seed.length > 0) mlp.setSeed(seed.toInt)
    if (initialWeights != null) mlp.setInitialWeights(Vectors.dense(initialWeights))

    val pipeline = new Pipeline()
      .setStages(Array(mlp))
      .fit(data)

    val multilayerPerceptronClassificationModel: MultilayerPerceptronClassificationModel =
    pipeline.stages.head.asInstanceOf[MultilayerPerceptronClassificationModel]

    val weights = multilayerPerceptronClassificationModel.weights.toArray
    val layersFromPipeline = multilayerPerceptronClassificationModel.layers
    val labelCount = data.select("label").distinct().count()

    new MultilayerPerceptronClassifierWrapper(pipeline, labelCount, layersFromPipeline, weights)
  }

  /**
   * Returns an [[MLReader]] instance for this class.
   */
  override def read: MLReader[MultilayerPerceptronClassifierWrapper] =
    new MultilayerPerceptronClassifierWrapperReader

  override def load(path: String): MultilayerPerceptronClassifierWrapper = super.load(path)

  class MultilayerPerceptronClassifierWrapperReader
    extends MLReader[MultilayerPerceptronClassifierWrapper]{

    override def load(path: String): MultilayerPerceptronClassifierWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val labelCount = (rMetadata \ "labelCount").extract[Long]
      val layers = (rMetadata \ "layers").extract[Array[Int]]
      val weights = (rMetadata \ "weights").extract[Array[Double]]

      val pipeline = PipelineModel.load(pipelinePath)
      new MultilayerPerceptronClassifierWrapper(pipeline, labelCount, layers, weights)
    }
  }

  class MultilayerPerceptronClassifierWrapperWriter(instance: MultilayerPerceptronClassifierWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("labelCount" -> instance.labelCount) ~
        ("layers" -> instance.layers.toSeq) ~
        ("weights" -> instance.weights.toArray.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }
}
