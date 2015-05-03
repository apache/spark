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

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.classification.ClassifierParams
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.util.Utils

/**
 * Params for [[Multiclass2Binary]].
 */
private[ml] trait Multiclass2BinaryParams extends ClassifierParams {

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
  val baseClassifier: Param[Estimator[_ <: Model[_]]] =
    new Param(this, "baseClassifier", "base binary classifier/regressor ")

  /** @group getParam */
  def getBaseClassifier: Estimator[_ <: Model[_]] = getOrDefault(baseClassifier)

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
 * @param fittingParamMap
 * @param baseClassificationModels the binary classification models for reduction.
 */
@AlphaComponent
private[ml] class Multiclass2BinaryModel(
    override val parent: Multiclass2Binary,
    override val fittingParamMap: ParamMap,
    val baseClassificationModels: Seq[Model[_]])
  extends Model[Multiclass2BinaryModel] with Multiclass2BinaryParams {

  /**
   * Transforms the dataset with provided parameter map as additional parameters.
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    // Check schema
    val parentSchema = dataset.schema
    transformSchema(parentSchema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val sqlCtx = dataset.sqlContext
    // score each model on every data point and pick the model with highest score
    // TODO: Add randomization when there are ties.
    val predictions = baseClassificationModels.zipWithIndex.par.map { case (model, index) =>
      val output = model.transform(dataset, paramMap)
      output.select(map(rawPredictionCol)).map { case Row(p: Vector) => List((index, p(1))) }
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
    val outputSchema = SchemaUtils.appendColumn(parentSchema, map(predictionCol), DoubleType)
    sqlCtx.createDataFrame(results, outputSchema)
  }

  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = false, featuresDataType)
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
  def setBaseClassifier(value: Estimator[_ <: Model[_]]): this.type = set(baseClassifier, value)

  /** @group setParam */
  def setNumClasses(value: Int): this.type = set(k, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): Multiclass2BinaryModel = {
    val map = extractParamMap(paramMap)
    val sqlCtx = dataset.sqlContext
    val schema = dataset.schema
    val numClasses = map(k)
    // create k columns, one for each binary classifier.
    val multiclassLabeled = dataset.select(map(labelCol), map(featuresCol))
    val binaryDataset = multiclassLabeled.map { case (Row(label: Double, features: Vector)) =>
      val labels = Range(0, numClasses).map { index =>
        if (label.toInt == index) 1.0 else 0.0
      }
      Row.fromSeq(List(label, features) ++ labels)
    }
    var newSchema = multiclassLabeled.schema
    Range(0, numClasses).foreach{ index =>
      val labelColName = Multiclass2Binary.labelColName(index)
      newSchema = SchemaUtils.appendColumn(newSchema, labelColName, DoubleType)
    }
    val trainingDataset = sqlCtx.createDataFrame(binaryDataset, newSchema)
    // learn k models, one for each label value.
    val models = Range(0, numClasses).par.map{ index =>
      val newClassifier = Multiclass2Binary.newClassifier(sqlCtx, getBaseClassifier, index)
      newClassifier.fit(trainingDataset)
    }.toList
    new Multiclass2BinaryModel(this, paramMap, models)
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = true, featuresDataType)
  }

}

object Multiclass2Binary {

  /**
   * Create a new classifier instance for multiclass classification.
   * The new classifier is configured to recognize the appropriate label
   * column.
   *
   * @param sqlCtx
   * @param origClassifier
   * @param index
   * @return
   */
  private def newClassifier(sqlCtx: SQLContext,
      origClassifier: Estimator[_ <: Model[_]],
      index: Int) = {

    val serializer = sqlCtx.sparkContext.env.serializer
    val serializerInstance = serializer.newInstance()
    // create a copy of the classifier
    // TODO: Should there be a copy method on Estimator?
    val classifier = Utils.clone[Estimator[_ <: Model[_]]](origClassifier, serializerInstance)
    val baseClassifierClass = classifier.getClass()
    val labelColName = Multiclass2Binary.labelColName(index)
    // set the label column to use the appropriate column as label.
    val setMthd = baseClassifierClass.getMethod("setLabelCol", classOf[String])
    setMthd.setAccessible(true)
    setMthd.invoke(classifier, labelColName)
    setMthd.setAccessible(false)
    classifier
  }

  /**
   * Generate a label column name for a given label index.
   *
   * @param index
   * @return
   */
  private def labelColName(index: Int) = {"mc2b$" + index}

}
