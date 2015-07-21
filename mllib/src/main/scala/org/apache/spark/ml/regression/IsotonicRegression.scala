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

package org.apache.spark.ml.regression

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param.{Param, ParamMap, BooleanParam}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.mllib.regression.{IsotonicRegression => MLlibIsotonicRegression}
import org.apache.spark.mllib.regression.{IsotonicRegressionModel => MLlibIsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, DataType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
 * Params for isotonic regression.
 */
private[regression] trait IsotonicRegressionParams extends PredictorParams {

  /**
   * Param for weight column name.
   * TODO: Move weightCol to sharedParams.
   *
   * @group param
   */
  final val weightCol: Param[String] =
    new Param[String](this, "weightCol", "weight column name")

  /** @group getParam */
  final def getWeightCol: String = $(weightCol)

  /**
   * Param for isotonic parameter.
   * Isotonic (increasing) or antitonic (decreasing) sequence.
   * @group param
   */
  final val isotonic: BooleanParam =
    new BooleanParam(this, "isotonic", "isotonic (increasing) or antitonic (decreasing) sequence")

  /** @group getParam */
  final def getIsotonicParam: Boolean = $(isotonic)
}

/**
 * :: Experimental ::
 * Isotonic regression.
 *
 * Currently implemented using parallelized pool adjacent violators algorithm.
 * Only univariate (single feature) algorithm supported.
 *
 * Uses [[org.apache.spark.mllib.regression.IsotonicRegression]].
 */
@Experimental
class IsotonicRegression(override val uid: String)
  extends Regressor[Double, IsotonicRegression, IsotonicRegressionModel]
  with IsotonicRegressionParams {

  def this() = this(Identifiable.randomUID("isoReg"))

  /**
   * Set the isotonic parameter.
   * Default is true.
   * @group setParam
   */
  def setIsotonicParam(value: Boolean): this.type = set(isotonic, value)
  setDefault(isotonic -> true)

  /**
   * Set weight column param.
   * Default is weight.
   * @group setParam
   */
  def setWeightParam(value: String): this.type = set(weightCol, value)
  setDefault(weightCol -> "weight")

  override private[ml] def featuresDataType: DataType = DoubleType

  override def copy(extra: ParamMap): IsotonicRegression = defaultCopy(extra)

  private[this] def extractWeightedLabeledPoints(
      dataset: DataFrame): RDD[(Double, Double, Double)] = {

    dataset.select($(labelCol), $(featuresCol), $(weightCol))
      .map { case Row(label: Double, features: Double, weights: Double) =>
        (label, features, weights)
      }
  }

  override protected def train(dataset: DataFrame): IsotonicRegressionModel = {
    SchemaUtils.checkColumnType(dataset.schema, $(weightCol), DoubleType)
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val instances = extractWeightedLabeledPoints(dataset)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val isotonicRegression = new MLlibIsotonicRegression().setIsotonic($(isotonic))
    val parentModel = isotonicRegression.run(instances)

    new IsotonicRegressionModel(uid, parentModel)
  }
}

/**
 * :: Experimental ::
 * Model fitted by IsotonicRegression.
 * Predicts using a piecewise linear function.
 *
 * For detailed rules see [[org.apache.spark.mllib.regression.IsotonicRegressionModel.predict()]].
 *
 * @param parentModel A [[org.apache.spark.mllib.regression.IsotonicRegressionModel]]
 *                    model trained by [[org.apache.spark.mllib.regression.IsotonicRegression]].
 */
class IsotonicRegressionModel private[ml] (
    override val uid: String,
    private[ml] val parentModel: MLlibIsotonicRegressionModel)
  extends RegressionModel[Double, IsotonicRegressionModel]
  with IsotonicRegressionParams {

  override def featuresDataType: DataType = DoubleType

  override protected def predict(features: Double): Double = {
    parentModel.predict(features)
  }

  override def copy(extra: ParamMap): IsotonicRegressionModel = {
    copyValues(new IsotonicRegressionModel(uid, parentModel), extra)
  }
}
