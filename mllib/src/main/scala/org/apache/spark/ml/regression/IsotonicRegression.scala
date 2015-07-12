package org.apache.spark.ml.regression

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param.{Param, ParamMap, BooleanParam}
import org.apache.spark.ml.util.Identifiable
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
   * @group param
   */
  final val weightCol: Param[String] = new Param[String](this, "weightCol", "weight column name")

  /** @group getParam */
  final def getWeightCol: String = $(weightCol)

  /**
   * Param for isotonic parameter.
   * @group param
   */
  final val isotonicParam: BooleanParam = new BooleanParam(this, "isotonicParam", "isotonic parameter")

  /** @group getParam */
  final def getIsotonicParam: Boolean = $(isotonicParam)
}

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
  def setIsotonicParam(value: Boolean): this.type = set(isotonicParam, value)
  setDefault(isotonicParam -> true)

  /**
   * Set the isotonic parameter.
   * Default is true.
   * @group setParam
   */
  def setWeightParam(value: String): this.type = set(weightCol, value)
  setDefault(weightCol -> "weight")

  override def featuresDataType: DataType = DoubleType

  override def copy(extra: ParamMap): IsotonicRegression = defaultCopy(extra)

  private def extractWeightedLabeledPoints(dataset: DataFrame): RDD[(Double, Double, Double)] = {
    dataset.select($(labelCol), $(featuresCol), $(weightCol))
      .map { case Row(label: Double, features: Double, weights: Double) => (label, features, weights) }
  }

  override protected def train(dataset: DataFrame): IsotonicRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val instances = extractWeightedLabeledPoints(dataset)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val isotonicRegression = new org.apache.spark.mllib.regression.IsotonicRegression().setIsotonic($(isotonicParam))
    val model = isotonicRegression.run(instances)

    new IsotonicRegressionModel(uid, model)
  }
}

class IsotonicRegressionModel private[ml] (
    override val uid: String,
    val model: org.apache.spark.mllib.regression.IsotonicRegressionModel)
  extends RegressionModel[Double, IsotonicRegressionModel]
  with IsotonicRegressionParams {

  override def featuresDataType: DataType = DoubleType

  override protected def predict(features: Double): Double = {
    model.predict(features)
  }

  override def copy(extra: ParamMap): IsotonicRegressionModel = {
    copyValues(new IsotonicRegressionModel(uid, model), extra)
  }
}

