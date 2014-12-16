package org.apache.spark.ml.impl.estimator

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.{Estimator, LabeledPoint, Model}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Star

@AlphaComponent
private[ml] trait PredictorParams extends Params
  with HasLabelCol with HasFeaturesCol with HasPredictionCol {

  /**
   * Validates and transforms the input schema with the provided param map.
   * @param schema input schema
   * @param paramMap additional parameters
   * @param fitting whether this is in fitting
   * @return output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      paramMap: ParamMap,
      fitting: Boolean): StructType = {
    val map = this.paramMap ++ paramMap
    val featuresType = schema(map(featuresCol)).dataType
    // TODO: Support casting Array[Double] and Array[Float] to Vector.
    require(featuresType.isInstanceOf[VectorUDT],
      s"Features column ${map(featuresCol)} must be Vector types" +
        s" but was actually $featuresType.")
    if (fitting) {
      val labelType = schema(map(labelCol)).dataType
      require(labelType == DoubleType || labelType == IntegerType,
        s"Cannot convert label column ${map(labelCol)} of type $labelType to a Double column.")
    }
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(map(predictionCol)),
      s"Prediction column ${map(predictionCol)} already exists.")
    val outputFields = schema.fields ++ Seq(
      StructField(map(predictionCol), DoubleType, nullable = false))
    StructType(outputFields)
  }
}

private[ml] abstract class Predictor[Learner <: Predictor[Learner, M], M <: PredictionModel[M]]
  extends Estimator[M] with PredictorParams {

  // TODO: Eliminate asInstanceOf and see if that works.
  def setLabelCol(value: String): Learner = set(labelCol, value).asInstanceOf[Learner]
  def setFeaturesCol(value: String): Learner = set(featuresCol, value).asInstanceOf[Learner]
  def setPredictionCol(value: String): Learner = set(predictionCol, value).asInstanceOf[Learner]

  protected def selectLabelColumn(dataset: SchemaRDD, paramMap: ParamMap): RDD[Double] = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    dataset.select(map(labelCol).attr).map {
      case Row(label: Double) => label
      case Row(label: Int) => label.toDouble
    }
  }

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = true)
  }

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): M = {
    transformSchema(dataset.schema, paramMap, logging = true)
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val instances = dataset.select(map(labelCol).attr, map(featuresCol).attr)
      .map { case Row(label: Double, features: Vector) =>
      LabeledPoint(label, features)
    }
    val model = train(instances, map)
    // copy model params
    Params.inheritValues(map, this, model)
    model
  }

  /**
   * Notes to developers:
   *  - Unlike [[fit()]], this method takes [[paramMap]] which has already been
   *    combined with the internal paramMap.
   *  - This should handle caching the dataset if needed.
   * @param dataset  Training data
   * @param paramMap  Parameters for training.
   */
  def train(dataset: RDD[LabeledPoint], paramMap: ParamMap): M
}

private[ml] abstract class PredictionModel[M <: PredictionModel[M]]
  extends Model[M] with PredictorParams {

  def setFeaturesCol(value: String): M = set(featuresCol, value).asInstanceOf[M]

  def setPredictionCol(value: String): M = set(predictionCol, value).asInstanceOf[M]

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = false)
  }

  /**
   * Transforms dataset by reading from [[featuresCol]], calling [[predict( )]], and storing
   * the predictions as a new column [[predictionCol]].
   * This default implementation should be overridden as needed.
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset with [[predictionCol]] of type [[Double]]
   */
  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import org.apache.spark.sql.catalyst.dsl._
    import dataset.sqlContext._

    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val pred: Vector => Double = (features) => {
      predict(features)
    }
    dataset.select(Star(None), pred.call(map(featuresCol).attr) as map(predictionCol))
  }

  /**
   * Default implementation.
   * Override for efficiency; e.g., this does not broadcast the model.
   */
  def predict(dataset: RDD[Vector]): RDD[Double] = {
    dataset.map(predict)
  }

  /**
   * Predict label for the given features.
   */
  def predict(features: Vector): Double
}
