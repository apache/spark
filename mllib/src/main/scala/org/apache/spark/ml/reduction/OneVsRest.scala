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

import java.util.UUID

import scala.language.existentials

import org.apache.spark.annotation.{AlphaComponent, Experimental}
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Params for [[OneVsRest]].
 */
private[ml] trait OneVsRestParams extends PredictorParams {

  type ClassifierType = Classifier[F, E, M] forSome {
    type F ;
    type M <: ClassificationModel[F,M];
    type E <:  Classifier[F, E,M]
  }

  /**
   * param for the base classifier that we reduce multiclass classification into.
   * @group param
   */
  val classifier: Param[ClassifierType]  =
    new Param(this, "classifier", "base binary classifier ")

  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)

}

/**
 * Model produced by [[OneVsRest]].
 *
 * @param parent
 * @param models the binary classification models for reduction.
 */
@AlphaComponent
class OneVsRestModel(
      override val parent: OneVsRest,
      val models: Array[_ <: ClassificationModel[_,_]])
  extends Model[OneVsRestModel] with OneVsRestParams {

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, getClassifier.featuresDataType)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // Check schema
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val init: () => Map[Int, Double] = () => {Map()}
    val mapType = MapType(IntegerType, DoubleType, false)
    val newDataset = dataset.withColumn(accColName,callUDF(init, mapType))

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.zipWithIndex.foldLeft[DataFrame](newDataset) {
      case (df, (model, index)) => {
        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(col(rawPredictionCol), col(accColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val update: (Map[Int, Double], Vector) => Map[Int, Double]  =
          (predictions: Map[Int, Double], prediction: Vector) => {
            predictions + ((index, prediction(1)))
        }
        val updateUdf = callUDF(update, mapType, col(accColName), col(rawPredictionCol))
        val transformedDataset = model.transform(df).select(columns:_*)
        val updatedDataset = transformedDataset.withColumn(tmpColName, updateUdf)
        val newColumns = origCols ++ List(col(tmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(newColumns:_*).withColumnRenamed(tmpColName, accColName)
      }
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    // output the index of the classifier with highest confidence as prediction
    val label: Map[Int, Double] => Double = (predictions: Map[Int, Double]) => {
      predictions.maxBy(_._2)._1.toDouble
    }
    aggregatedDataset.withColumn($(predictionCol), callUDF(label, DoubleType, col(accColName)))
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
class OneVsRest extends Estimator[OneVsRestModel] with OneVsRestParams {

  /** @group setParam */
  // TODO: Find a better way to do this. Existential Types don't work with Java API so cast needed.
  def setClassifier(value: Classifier[_,_,_]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
  }

  override def fit(dataset: DataFrame): OneVsRestModel = {
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

      val label: Double => Double = (label: Double) => {
        if (label.toInt == index) 1.0 else 0.0
      }

      // generate new label metadata for the binary problem.
      // TODO: use when ... otherwise after SPARK-7321 is merged
      val labelUDF = callUDF(label, DoubleType, col($(labelCol)))
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val skipFeatures: Any => Boolean = (name: Any) => name.toString.equals(featuresCol.name)
      val labelColName = "mc2b$" + index
      val labelUDFWithNewMeta = labelUDF.as(labelColName, newLabelMeta)
      val trainingDataset = multiclassLabeled.withColumn(labelColName, labelUDFWithNewMeta)
      val classifier = getClassifier
      classifier.fit(trainingDataset, classifier.labelCol -> labelColName)
    }.toArray[ClassificationModel[_,_]]

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    copyValues(new OneVsRestModel(this, models))
  }
}
