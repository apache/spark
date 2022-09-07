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
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class MultilayerPerceptronClassifierWrapper private (
    val pipeline: PipelineModel
  ) extends MLWritable {

  import MultilayerPerceptronClassifierWrapper._

  private val mlpModel: MultilayerPerceptronClassificationModel =
    pipeline.stages(1).asInstanceOf[MultilayerPerceptronClassificationModel]

  lazy val weights: Array[Double] = mlpModel.weights.toArray
  lazy val layers: Array[Int] = mlpModel.getLayers

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(mlpModel.getFeaturesCol)
      .drop(mlpModel.getLabelCol)
      .drop(PREDICTED_LABEL_INDEX_COL)
  }

  /**
   * Returns an [[MLWriter]] instance for this ML instance.
   */
  override def write: MLWriter =
    new MultilayerPerceptronClassifierWrapper.MultilayerPerceptronClassifierWrapperWriter(this)
}

private[r] object MultilayerPerceptronClassifierWrapper
  extends MLReadable[MultilayerPerceptronClassifierWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit(  // scalastyle:ignore
      data: DataFrame,
      formula: String,
      blockSize: Int,
      layers: Array[Int],
      solver: String,
      maxIter: Int,
      tol: Double,
      stepSize: Double,
      seed: String,
      initialWeights: Array[Double],
      handleInvalid: String
     ): MultilayerPerceptronClassifierWrapper = {
    val rFormula = new RFormula()
      .setFormula(formula)
      .setForceIndexLabel(true)
      .setHandleInvalid(handleInvalid)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)
    // get labels and feature names from output schema
    val (_, labels) = getFeaturesAndLabels(rFormulaModel, data)

    // assemble and fit the pipeline
    val mlp = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(blockSize)
      .setSolver(solver)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setStepSize(stepSize)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setLabelCol(rFormula.getLabelCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)
    if (seed != null && seed.length > 0) mlp.setSeed(seed.toInt)
    if (initialWeights != null) {
      require(initialWeights.length > 0)
      mlp.setInitialWeights(Vectors.dense(initialWeights))
    }

    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, mlp, idxToStr))
      .fit(data)

    new MultilayerPerceptronClassifierWrapper(pipeline)
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
      val pipelinePath = new Path(path, "pipeline").toString

      val pipeline = PipelineModel.load(pipelinePath)
      new MultilayerPerceptronClassifierWrapper(pipeline)
    }
  }

  class MultilayerPerceptronClassifierWrapperWriter(instance: MultilayerPerceptronClassifierWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = "class" -> instance.getClass.getName
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }
}
