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

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.classification.{ClassificationModel, Classifier, ClassifierParams}
import org.apache.spark.ml.param.{ParamMap, Param}
import org.apache.spark.ml.util.{MetadataUtils, SchemaUtils}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Params for [[OneVsRest]].
 */
private[ml] trait OneVsRestParams extends ClassifierParams {

  type ClassifierType = Classifier[F, E, M] forSome {
    type F;
    type M <: ClassificationModel[F, M];
    type E <: Classifier[F, E, M]
  }

  /**
   * param for the classifier that we reduce multiclass classification into.
   * @group param
   */
  val classifier: Param[ClassifierType]  =
    new Param(this, "classifier", "base binary classifier")

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
    val models: Array[Model[_]])
  extends Model[OneVsRestModel] with OneVsRestParams {

  override def transform(dataset: DataFrame): DataFrame = {
    // Check schema
    val parentSchema = dataset.schema
    transformSchema(parentSchema, logging = true)
    val sqlCtx = dataset.sqlContext

    // score each model on every data point and pick the model with highest score
    // TODO: Use DataFrame expressions to leverage performance here
    val predictions = models.zipWithIndex.par.map { case (model, index) =>
      val output = model.transform(dataset)
      output.select($(rawPredictionCol)).map { case Row(p: Vector) => List((index, p(1))) }
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

  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, featuresDataType)
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
 * Classifier parameters like featuresCol, PredictionCol and rawPredictionCol are
 * set directly on the OneVsRest and are ignored on the underlying classifier.
 *
 */
class OneVsRest extends Estimator[OneVsRestModel]
  with OneVsRestParams {

  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  def setClassifier(value: ClassifierType): this.type = set(classifier, value)

  override def fit(dataset: DataFrame): OneVsRestModel = {

    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      dataset.select($(labelCol)).distinct.count().toInt
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

      // generate new label for each binary classifier.
      // generate new label metadata for the binary problem.
      // TODO use when ... otherwise after SPARK-7321 is merged
      val labelUDF = callUDF(label, DoubleType, col($(labelCol)))
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelUDFWithNewMeta = labelUDF.as(labelColName, newLabelMeta)
      val trainingDataset = multiclassLabeled.withColumn(labelColName, labelUDFWithNewMeta)
      val classifier = getClassifier
      val map = new ParamMap()
      map.put(classifier.labelCol -> labelColName)
      map.put(classifier.featuresCol -> $(featuresCol))
      map.put(classifier.predictionCol -> $(predictionCol))
      map.put(classifier.rawPredictionCol -> $(rawPredictionCol))
      classifier.fit(trainingDataset, map)
    }.toArray[Model[_]]

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    copyValues(new OneVsRestModel(this, models))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, featuresDataType)
  }

}
