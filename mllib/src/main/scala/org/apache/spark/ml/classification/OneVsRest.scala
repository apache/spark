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

package org.apache.spark.ml.classification

import java.util.UUID

import scala.language.existentials

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Params for [[OneVsRest]].
 */
private[ml] trait OneVsRestParams extends PredictorParams {

  // scalastyle:off structural.type
  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }
  // scalastyle:on structural.type

  /**
   * param for the base binary classifier that we reduce multiclass classification into.
   * The base classifier input and output columns are ignored in favor of
   * the ones specified in [[OneVsRest]].
   * @group param
   */
  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")

  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)
}

/**
 * :: Experimental ::
 * Model produced by [[OneVsRest]].
 * This stores the models resulting from training k binary classifiers: one for each class.
 * Each example is scored against all k models, and the model with the highest score
 * is picked to label the example.
 *
 * @param labelMetadata Metadata of label column if it exists, or Nominal attribute
 *                      representing the number of classes in training dataset otherwise.
 * @param models The binary classification models for the reduction.
 *               The i-th model is produced by testing the i-th class (taking label 1) vs the rest
 *               (taking label 0).
 */
@Experimental
final class OneVsRestModel private[ml] (
    override val uid: String,
    labelMetadata: Metadata,
    val models: Array[_ <: ClassificationModel[_, _]])
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
    val initUDF = udf { () => Map[Int, Double]() }
    val mapType = MapType(IntegerType, DoubleType, valueContainsNull = false)
    val newDataset = dataset.withColumn(accColName, initUDF())

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.zipWithIndex.foldLeft[DataFrame](newDataset) {
      case (df, (model, index)) =>
        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(col(rawPredictionCol), col(accColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
          predictions + ((index, prediction(1)))
        }
        val transformedDataset = model.transform(df).select(columns : _*)
        val updatedDataset = transformedDataset
          .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))
        val newColumns = origCols ++ List(col(tmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(newColumns : _*).withColumnRenamed(tmpColName, accColName)
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    // output the index of the classifier with highest confidence as prediction
    val labelUDF = udf { (predictions: Map[Int, Double]) =>
      predictions.maxBy(_._2)._1.toDouble
    }

    // output label and label metadata as prediction
    aggregatedDataset
      .withColumn($(predictionCol), labelUDF(col(accColName)).as($(predictionCol), labelMetadata))
      .drop(accColName)
  }

  override def copy(extra: ParamMap): OneVsRestModel = {
    val copied = new OneVsRestModel(
      uid, labelMetadata, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
    copyValues(copied, extra).setParent(parent)
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
 */
@Experimental
final class OneVsRest(override val uid: String)
  extends Estimator[OneVsRestModel] with OneVsRestParams {

  def this() = this(Identifiable.randomUID("oneVsRest"))

  /** @group setParam */
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

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
      val labelUDF = udf { (label: Double) =>
        if (label.toInt == index) 1.0 else 0.0
      }

      // generate new label metadata for the binary problem.
      // TODO: use when ... otherwise after SPARK-7321 is merged
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelColName = "mc2b$" + index
      val labelUDFWithNewMeta = labelUDF(col($(labelCol))).as(labelColName, newLabelMeta)
      val trainingDataset = multiclassLabeled.withColumn(labelColName, labelUDFWithNewMeta)
      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      paramMap.put(classifier.predictionCol -> getPredictionCol)
      classifier.fit(trainingDataset, paramMap)
    }.toArray[ClassificationModel[_, _]]

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    // extract label metadata from label column if present, or create a nominal attribute
    // to output the number of labels
    val labelAttribute = Attribute.fromStructField(labelSchema) match {
      case _: NumericAttribute | UnresolvedAttribute =>
        NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
      case attr: Attribute => attr
    }
    val model = new OneVsRestModel(uid, labelAttribute.toMetadata(), models).setParent(this)
    copyValues(model)
  }

  override def copy(extra: ParamMap): OneVsRest = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsRest]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }
}
