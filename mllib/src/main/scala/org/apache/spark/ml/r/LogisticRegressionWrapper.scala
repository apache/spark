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
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.util.ArrayImplicits._

private[r] class LogisticRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val labels: Array[String]) extends MLWritable {

  import LogisticRegressionWrapper._

  private val lrModel: LogisticRegressionModel =
    pipeline.stages(1).asInstanceOf[LogisticRegressionModel]

  lazy val rFeatures: Array[String] = if (lrModel.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  lazy val rCoefficients: Array[Double] = {
    val numRows = lrModel.coefficientMatrix.numRows
    val numCols = lrModel.coefficientMatrix.numCols
    val numColsWithIntercept = if (lrModel.getFitIntercept) numCols + 1 else numCols
    val coefficients: Array[Double] = new Array[Double](numRows * numColsWithIntercept)
    val coefficientVectors: Seq[Vector] = lrModel.coefficientMatrix.rowIter.toSeq
    var i = 0
    if (lrModel.getFitIntercept) {
      while (i < numRows) {
        coefficients(i * numColsWithIntercept) = lrModel.interceptVector(i)
        System.arraycopy(coefficientVectors(i).toArray, 0,
          coefficients, i * numColsWithIntercept + 1, numCols)
        i += 1
      }
    } else {
      while (i < numRows) {
        System.arraycopy(coefficientVectors(i).toArray, 0,
          coefficients, i * numColsWithIntercept, numCols)
        i += 1
      }
    }
    coefficients
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(lrModel.getFeaturesCol)
      .drop(lrModel.getLabelCol)
  }

  override def write: MLWriter = new LogisticRegressionWrapper.LogisticRegressionWrapperWriter(this)
}

private[r] object LogisticRegressionWrapper
    extends MLReadable[LogisticRegressionWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit( // scalastyle:ignore
      data: DataFrame,
      formula: String,
      regParam: Double,
      elasticNetParam: Double,
      maxIter: Int,
      tol: Double,
      family: String,
      standardization: Boolean,
      thresholds: Array[Double],
      weightCol: String,
      aggregationDepth: Int,
      numRowsOfBoundsOnCoefficients: Int,
      numColsOfBoundsOnCoefficients: Int,
      lowerBoundsOnCoefficients: Array[Double],
      upperBoundsOnCoefficients: Array[Double],
      lowerBoundsOnIntercepts: Array[Double],
      upperBoundsOnIntercepts: Array[Double],
      handleInvalid: String
      ): LogisticRegressionWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setForceIndexLabel(true)
      .setHandleInvalid(handleInvalid)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    val fitIntercept = rFormula.hasIntercept

    // get labels and feature names from output schema
    val (features, labels) = getFeaturesAndLabels(rFormulaModel, data)

    // assemble and fit the pipeline
    val lr = new LogisticRegression()
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFitIntercept(fitIntercept)
      .setFamily(family)
      .setStandardization(standardization)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setLabelCol(rFormula.getLabelCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)
      .setAggregationDepth(aggregationDepth)

    if (thresholds.length > 1) {
      lr.setThresholds(thresholds)
    } else {
      lr.setThreshold(thresholds(0))
    }

    if (weightCol != null) lr.setWeightCol(weightCol)

    if (numRowsOfBoundsOnCoefficients != 0 &&
      numColsOfBoundsOnCoefficients != 0 && lowerBoundsOnCoefficients != null) {
      val coef = Matrices.dense(numRowsOfBoundsOnCoefficients,
        numColsOfBoundsOnCoefficients, lowerBoundsOnCoefficients)
      lr.setLowerBoundsOnCoefficients(coef)
    }

    if (numRowsOfBoundsOnCoefficients != 0 &&
      numColsOfBoundsOnCoefficients != 0 && upperBoundsOnCoefficients != null) {
      val coef = Matrices.dense(numRowsOfBoundsOnCoefficients,
        numColsOfBoundsOnCoefficients, upperBoundsOnCoefficients)
      lr.setUpperBoundsOnCoefficients(coef)
    }

    if (lowerBoundsOnIntercepts != null) {
      val intercept = Vectors.dense(lowerBoundsOnIntercepts)
      lr.setLowerBoundsOnIntercepts(intercept)
    }

    if (upperBoundsOnIntercepts != null) {
      val intercept = Vectors.dense(upperBoundsOnIntercepts)
      lr.setUpperBoundsOnIntercepts(intercept)
    }

    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, lr, idxToStr))
      .fit(data)

    new LogisticRegressionWrapper(pipeline, features, labels)
  }

  override def read: MLReader[LogisticRegressionWrapper] = new LogisticRegressionWrapperReader

  override def load(path: String): LogisticRegressionWrapper = super.load(path)

  class LogisticRegressionWrapperWriter(instance: LogisticRegressionWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toImmutableArraySeq) ~
        ("labels" -> instance.labels.toImmutableArraySeq)
      val rMetadataJson: String = compact(render(rMetadata))
      // Note that we should write single file. If there are more than one row
      // it produces more partitions.
      sparkSession.createDataFrame(Seq(Tuple1(rMetadataJson))).write.text(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class LogisticRegressionWrapperReader extends MLReader[LogisticRegressionWrapper] {

    override def load(path: String): LogisticRegressionWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sparkSession.read.text(rMetadataPath)
        .first().getString(0)
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val labels = (rMetadata \ "labels").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new LogisticRegressionWrapper(pipeline, features, labels)
    }
  }
}
