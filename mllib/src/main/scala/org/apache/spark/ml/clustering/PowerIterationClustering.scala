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

package org.apache.spark.ml.clustering

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}

/*
 * Common params for PowerIterationClustering and PowerIterationClusteringModel
 */
private[clustering] trait PowerIterationClusteringParams extends Params with HasMaxIter
  with HasFeaturesCol with HasPredictionCol {

  /*
   * The number of clusters to create (k). Must be > 1. Default: 2.
   * @group param
   */
  @Since("2.0.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  @Since("2.0.0")
  final val initMode = new Param[String](this, "initMode", "The initialization algorithm. " +
    "Supported options: 'random' and 'degree'.",
    (value: String) => validateInitMode(value))

  private[spark] def validateInitMode(initMode: String): Boolean = {
    initMode match {
      case "random" => true
      case "degree" => true
      case _ => false
    }
  }

  /** @group expertGetParam */
  @Since("2.0.0")
  def getInitMode: String = $(initMode)

  /**
    * Validates and transforms the input schema.
    * @param schema input schema
    * @return output schema
    */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}


@Since("2.0.0")
@Experimental
class PowerIterationClusteringModel private[ml] (
    @Since("2.0.0") override val uid: String)
  extends Model[PowerIterationClusteringModel] with PowerIterationClusteringParams with MLWritable {
  @Since("2.0.0")
  override def copy(extra: ParamMap): PowerIterationClusteringModel = {
    val copied = new PowerIterationClusteringModel(uid)
    copyValues(copied, extra).setParent(this.parent)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = ???

  @Since("2.0.0")
  override def write: MLWriter =
    new PowerIterationClusteringModel.PowerIterationClusteringModelWriter(this)

  private var trainingSummary: Option[PowerIterationClusteringSummary] = None

  private[clustering] def setSummary(summary: PowerIterationClusteringSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /**
    * Return true if there exists summary of model.
    */
  @Since("2.0.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
    * Gets summary of model on training set. An exception is
    * thrown if `trainingSummary == None`.
    */
  @Since("2.0.0")
  def summary: PowerIterationClusteringSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

@Since("2.0.0")
object PowerIterationClusteringModel extends MLReadable[PowerIterationClusteringModel] {

  @Since("2.0.0")
  override def read: MLReader[PowerIterationClusteringModel] =
    new PowerIterationClusteringModelReader()

  @Since("2.0.0")
  override def load(path: String): PowerIterationClusteringModel = ???

  /** [[MLWriter]] instance for [[PowerIterationClusteringModel]] */
  private[PowerIterationClusteringModel] class PowerIterationClusteringModelWriter
  (instance: PowerIterationClusteringModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = ???
  }

  private class PowerIterationClusteringModelReader
    extends MLReader[PowerIterationClusteringModel] {

    override def load(path: String): PowerIterationClusteringModel = ???
  }
}

@Since("2.0.0")
@Experimental
class PowerIterationClustering @Since("2.0.0") (
                               @Since("2.0.0") override val uid: String)
  extends Estimator[PowerIterationClusteringModel] with PowerIterationClusteringParams
    with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> "random")

  @Since("2.0.0")
  override def copy(extra: ParamMap): PowerIterationClustering = defaultCopy(extra)

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("PowerIterationClustering"))

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group setParam */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): PowerIterationClusteringModel = ???

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("2.0.0")
object PowerIterationClustering extends DefaultParamsReadable[PowerIterationClustering] {

  @Since("2.0.0")
  override def load(path: String): PowerIterationClustering = super.load(path)
}

@Since("2.0.0")
@Experimental
class PowerIterationClusteringSummary private[clustering] () extends Serializable {

}
