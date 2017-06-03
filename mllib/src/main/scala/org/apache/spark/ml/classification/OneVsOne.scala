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

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.annotation.{Experimental, Since}
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
 * Params for [[OneVsOne]].
 */
private[ml] trait OneVsOneParams extends PredictorParams {

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
   * the ones specified in [[OneVsOne]].
   *
   * @group param
   */
  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")

  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)
}

/**
 * :: Experimental ::
 * Model produced by [[OneVsOne]].
 * This stores the models resulting from training k * (k - 1) / 2 binary classifiers: one for
 * each class-pair. Each example is scored against all those models, and the model with the
 * highest score is picked to label the example.
 *
 * @param labelMetadata Metadata of label column if it exists, or Nominal attribute
 *                      representing the number of classes in training dataset otherwise.
 * @param models The binary classification models for the reduction.
 * @param pairs The corresponding pair indices for model in models.
 */
@Since("2.0.0")
@Experimental
final class OneVsOneModel private[ml] (
    @Since("2.0.0")  override val uid: String,
    @Since("2.0.0") labelMetadata: Metadata,
    @Since("2.0.0") val models: Array[_ <: ClassificationModel[_, _]],
    @Since("2.0.0") val pairs: Array[(Int, Int)])
  extends Model[OneVsOneModel] with OneVsOneParams {

  require(models.length == pairs.length)

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, getClassifier.featuresDataType)
  }

  @Since("2.0.0")
  override def transform(dataset: DataFrame): DataFrame = {
    // Check schema
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val initUDF = udf { () => Map[Int, Double]() }
    val newDataset = dataset.withColumn(accColName, initUDF())

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.zip(pairs).foldLeft[DataFrame](newDataset) {
      case (df, (model, (negIndex, posIndex))) =>
        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(col(rawPredictionCol), col(accColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
          predictions + ((negIndex, prediction(0)), (posIndex, prediction(1)))
        }
        val transformedDataset = model.transform(df).select(columns: _*)
        val updatedDataset = transformedDataset
          .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))
        val newColumns = origCols ++ List(col(tmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(newColumns: _*).withColumnRenamed(tmpColName, accColName)
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
      .withColumn($(predictionCol), labelUDF(col(accColName)), labelMetadata)
      .drop(accColName)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): OneVsOneModel = {
    val copied = new OneVsOneModel(
      uid, labelMetadata,
      models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]), pairs)
    copyValues(copied, extra).setParent(parent)
  }
}

/**
 * :: Experimental ::
 *
 * Reduction of Multiclass Classification to Binary Classification.
 * Performs reduction using one against all strategy.
 * For a multiclass classification with k classes, train k * (k - 1) / 2 models
 * (one per class-pair).
 * Each example is scored against all k * (k - 1) / 2 models and the model with
 * highest score is picked to label the example.
 */
@Since("2.0.0")
@Experimental
final class OneVsOne @Since("2.0.0") (
    @Since("2.0.0") override val uid: String)
  extends Estimator[OneVsOneModel] with OneVsOneParams {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("oneVsOne"))

  /** @group setParam */
  @Since("2.0.0")
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  /** @group setParam */
  @Since("2.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
  }

  @Since("2.0.0")
  override def fit(dataset: DataFrame): OneVsOneModel = {
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

    val pairs = ArrayBuffer[(Int, Int)]()
    for (negIndex <- 0 until numClasses; posIndex <- negIndex + 1 until numClasses) {
      pairs.append((negIndex, posIndex))
    }

    // create k * (k - 1) / 2 columns, one for each binary classifier.
    val models = pairs.map {
      case (negIndex, posIndex) =>
        // generate new label metadata for the binary problem.
        val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
        val labelColName = "mc2b$" + negIndex + "vs" + posIndex

        val trainingDataset = multiclassLabeled
          .filter(s"${$(labelCol)} = ${negIndex} or ${$(labelCol)} = ${posIndex}")
          .withColumn(labelColName, when(col($(labelCol)) === posIndex.toDouble, 1.0)
            .otherwise(0.0), newLabelMeta)

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
    val model = new OneVsOneModel(uid, labelAttribute.toMetadata(),
      models, pairs.toArray).setParent(this)
    copyValues(model)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): OneVsOne = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsOne]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }
}
