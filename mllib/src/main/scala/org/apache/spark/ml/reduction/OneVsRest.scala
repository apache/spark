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

package org.apache.spark.ml.reduction

import scala.language.existentials

import org.apache.spark.annotation.{AlphaComponent, Experimental}
import org.apache.spark.ml.{PredictorParams, PredictionModel, Predictor}
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MetadataUtils, SchemaUtils}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Model produced by [[OneVsRest]].
 *
 * @param parent
 * @param models the binary classification models for reduction.
 */
@AlphaComponent
class OneVsRestModel[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]](
      override val parent: OneVsRest[FeaturesType, E, M],
      val models: Array[_ <: ClassificationModel[FeaturesType, M]])
  extends PredictionModel[FeaturesType, OneVsRestModel[FeaturesType, E, M]]
  with PredictorParams {

  override protected def featuresDataType: DataType = parent.featuresDataType

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, featuresDataType)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // Check schema
    val parentSchema = dataset.schema
    transformSchema(parentSchema, logging = true)
    val sqlCtx = dataset.sqlContext
    val rawPredictionCol = parent.classifier.getRawPredictionCol
    // score each model on every data point and pick the model with highest score
    // TODO: Use DataFrame expressions to leverage performance here
    val predictions = models.zipWithIndex.par.map { case (model, index) =>
      val output = model.transform(dataset)
      output.select(rawPredictionCol).map { case Row(p: Vector) => List((index, p(1))) }
    }.reduce[RDD[List[(Int, Double)]]] { case (x, y) =>
      x.zip(y).map { case ((a, b)) =>
        a ++ b
      }
    }.map(_.maxBy(_._2))

    // ensure that we pass through columns that are part of the original dataset.
    val results = dataset.select(col("*")).rdd.zip(predictions).map { case (row, (label, _)) =>
      Row.fromSeq(row.toSeq ++ List(label.toDouble))
    }

    // the output schema should retain all input fields and add prediction column.
    val outputSchema = SchemaUtils.appendColumn(parentSchema, $(predictionCol), DoubleType)
    sqlCtx.createDataFrame(results, outputSchema)
  }

  override protected def predict(features: FeaturesType): Double = {
    throw new UnsupportedOperationException("Ensemble Classifier does not support predict," +
      " use transform instead")
  }
}

/**
 * :: Experimental ::
 *
 * Reduction of Multiclass Classification to Binary Classification.
 * Performs reduction using one against all strategy.
 * For a multiclass classification with k classes, train k models (one per class).
 * Each example is scored against all k models and the model with highest score
 * is picked to label the example.
 *
 */
@Experimental
class OneVsRest[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]]
  (val classifier: Classifier[FeaturesType, E, M])
  extends Predictor[FeaturesType, OneVsRest[FeaturesType, E, M],
    OneVsRestModel[FeaturesType, E, M]]
  with PredictorParams {

  override private[ml] def featuresDataType: DataType = classifier.featuresDataType

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, featuresDataType)
  }

  override protected def train(dataset: DataFrame): OneVsRestModel[FeaturesType, E, M] = {
    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)

    val multiclassLabeled = dataset.select($(labelCol), $(featuresCol))

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // create k columns, one for each binary classifier.
    val models = Range(0, numClasses).par.map { index =>
      val labelColName = "mc2b$" + index
      val label: Double => Double = (label: Double) => {
        if (label.toInt == index) 1.0 else 0.0
      }

      // generate new label metadata for the binary problem.
      // TODO: use when ... otherwise after SPARK-7321 is merged
      val labelUDF = callUDF(label, DoubleType, col($(labelCol)))
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelUDFWithNewMeta = labelUDF.as(labelColName, newLabelMeta)
      val trainingDataset = multiclassLabeled.withColumn(labelColName, labelUDFWithNewMeta)
      val map = new ParamMap()
      map.put(classifier.labelCol -> labelColName)
      classifier.fit(trainingDataset, map)
    }.toArray[ClassificationModel[FeaturesType, M]]

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    new OneVsRestModel[FeaturesType, E, M](this, models)
  }
}
