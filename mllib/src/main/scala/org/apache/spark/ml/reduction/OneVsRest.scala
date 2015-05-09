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
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{MetadataUtils, SchemaUtils}
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
    val parentSchema = dataset.schema
    transformSchema(parentSchema, logging = true)
    val sqlCtx = dataset.sqlContext

    // determine the output fields added by the model
    val origColNames = dataset.schema.map(_.name).toSet
    val newColNames = models.head.transformSchema(dataset.schema).map(_.name).toSet
    val additionalColNames = newColNames.diff(origColNames)

    // each classifier is a transformer of
    // the input dataset. Apply the sequence of transformations
    // and collect the resulting dataset.
    // since each classifier adds columns to the dataset we need
    // to ensure the column names are unique
    var scoredDataset = dataset
    val predictionCols = models.zipWithIndex.map { case (model, index) =>
      val map = copyWithOffset(model, index.toString, additionalColNames)
      scoredDataset = model.transform(scoredDataset, map)
      map(model.rawPredictionCol)
    }.toSeq

    // TODO: Is there a way to refactor this code to use a UDF + withColumn instead?
    val numClasses = models.size
    val results = scoredDataset.select("*", predictionCols: _*).map { row =>
      val s = row.toSeq
      val star = s.dropRight(numClasses)
      val scores = s.takeRight(numClasses).map(_.asInstanceOf[Vector])
      val prediction = scores.zipWithIndex.maxBy(_._1(1))._2.toDouble
      Row.fromSeq(star ++ List(prediction))
    }
    val scoredDatasetSchema = scoredDataset.schema
    val outputSchema = SchemaUtils.appendColumn(scoredDatasetSchema, $(predictionCol), DoubleType)
    sqlCtx.createDataFrame(results, outputSchema)
  }

  private def copyWithOffset[T <: Params](from: T, offset: String, names: Set[String]) = {
    val map = from.extractParamMap()
    val to = new ParamMap()
    map.toSeq.foreach { paramPair =>
      val param = paramPair.param
      val v = paramPair.value
      if (names.contains(v.toString)) {
        to.put(param.asInstanceOf[Param[Any]], v.toString + offset)
      }
    }
    to
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
  // TODO: Find a better way to do this. Existential Types dont work with Java API so cast needed.
  def setClassifier(value: Classifier[_,_,_]): this.type = set(classifier, value.asInstanceOf[ClassifierType])

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
