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
import org.apache.spark.ml.classification.{ClassificationModel, Classifier, ClassifierParams}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Params for [[Multiclass2Binary]].
 */
private[ml] trait Multiclass2BinaryParams extends ClassifierParams {

  type ClassifierType = Classifier[F, E, M] forSome {
    type F ;
    type M <: ClassificationModel[F,M];
    type E <:  Classifier[F, E,M]
  }

  /**
   * param for prediction column name
   * @group param
   */
  val idCol: Param[String] =
    new Param(this, "idCol", "id column name")

  setDefault(idCol, "id")

  /**
   * param for base classifier index column name
   * @group param
   */
  val indexCol: Param[String] =
    new Param(this, "indexCol", "classifier index column name")

  setDefault(indexCol, "index")

  /**
   * param for the base classifier that we reduce multiclass classification into.
   * @group param
   */
  val baseClassifier: Param[ClassifierType]  =
    new Param(this, "baseClassifier", "base binary classifier/regressor ")

  /** @group getParam */
  def getBaseClassifier: ClassifierType = getOrDefault(baseClassifier)

  /**
   * param for number of classes.
   * @group param
   */
  val k: IntParam = new IntParam(this, "k", "number of classes")

  /** @group getParam */
  def getK(): Int = getOrDefault(k)

}

/**
 *
 * @param parent
 * @param baseClassificationModels the binary classification models for reduction.
 */
@AlphaComponent
private[ml] class Multiclass2BinaryModel(
    override val parent: Multiclass2Binary,
    val baseClassificationModels: Seq[Model[_]])
  extends Model[Multiclass2BinaryModel] with Multiclass2BinaryParams {

  /**
   * Transforms the dataset with provided parameter map as additional parameters.
   * @param dataset input dataset
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame): DataFrame = {
    // Check schema
    val parentSchema = dataset.schema
    transformSchema(parentSchema, logging = true)
    val sqlCtx = dataset.sqlContext
    // score each model on every data point and pick the model with highest score
    // TODO: Add randomization when there are ties.
    val predictions = baseClassificationModels.zipWithIndex.par.map { case (model, index) =>
      val output = model.transform(dataset)
      output.select($(rawPredictionCol)).map { case Row(p: Vector) => List((index, p(1))) }
    }.reduce[RDD[List[(Int, Double)]]] { case (x, y) =>
      x.zip(y).map { case ((a, b)) =>
        a ++ b
      }
    }.
      map(_.maxBy(_._2))
    // ensure that we pass through columns that are part of the original dataset.
    val results = dataset.select(col("*")).rdd.zip(predictions).map { case ((row, (label, _))) =>
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
 */
class Multiclass2Binary extends Estimator[Multiclass2BinaryModel]
  with Multiclass2BinaryParams {

  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  /** @group setParam */
  def setBaseClassifier(value: ClassifierType): this.type = set(baseClassifier, value)

  /** @group setParam */
  def setNumClasses(value: Int): this.type = set(k, value)

  override def fit(dataset: DataFrame): Multiclass2BinaryModel = {
    val sqlCtx = dataset.sqlContext
    val numClasses = $(k)
    // create k columns, one for each binary classifier.
    val multiclassLabeled = dataset.select($(labelCol), $(featuresCol)).cache()
    val models = Range(0, numClasses).par.map { index =>
      val labelColName = "mc2b$" + index
      val label: Double => Double = (label: Double) => {
        if (label.toInt == index) 1.0 else 0.0
      }
      val labelUDF = callUDF(label, DoubleType, col($(labelCol)))
      val trainingDataset = multiclassLabeled.withColumn(labelColName, labelUDF)
      newClassifier(sqlCtx, labelColName).fit(trainingDataset)
    }.toList
    multiclassLabeled.unpersist()
    new Multiclass2BinaryModel(this, models)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, featuresDataType)
  }

  /**
   * Create an initialized classifier with label column overridden
   * to point to the right column.
   *
   * @param sqlCtx
   * @param labelColName
   * @return
   */
  private def newClassifier(sqlCtx: SQLContext, labelColName: String) = {

    val serializer = sqlCtx.sparkContext.env.serializer
    val serializerInstance = serializer.newInstance()
    // create a copy of the classifier
    // TODO: Should there be a copy method on Estimator?
    val origClassifier = getBaseClassifier
    val newClassifier = Utils.clone[ClassifierType](origClassifier, serializerInstance)

    // set the label column to use the appropriate column as label.
    newClassifier.setLabelCol(labelColName)
    newClassifier
  }

}
