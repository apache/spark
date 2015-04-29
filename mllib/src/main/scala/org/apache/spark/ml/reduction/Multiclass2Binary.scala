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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

/**
 * Params for [[Multiclass2Binary]].
 */
private[ml] trait Multiclass2BinaryParams
  extends ClassifierParams {

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
 * :: AlphaComponent ::
 *
 * Model produced by [[Multiclass2Binary]].
 */
@AlphaComponent
private[ml] class Multiclass2BinaryModel(
                                          override val parent: Multiclass2Binary,
                                          override val fittingParamMap: ParamMap,
                                          val baseClassificationModels: Seq[Model[_]])
  extends Model[Multiclass2BinaryModel]
  with Multiclass2BinaryParams {

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
    val predictions = baseClassificationModels.zipWithIndex.par.map { case (model, index) =>
      val output = model.transform(dataset, paramMap)
      output.select(map(rawPredictionCol)).map { case Row(p: Vector) => List((index, p(1))) }
    }.reduce[RDD[List[(Int, Double)]]] { case (x, y) =>
      x.zip(y).map { case ((a, b)) =>
        a ++ b
      }
    }.
      map(_.maxBy(_._2))

    val results = dataset.select(col("*")).rdd.zip(predictions).map { case ((row, (label, _))) =>
      Row.fromSeq(row.toSeq ++ List(label.toDouble))
    }
    // determine the output schema
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
 * Performs a reduction from Multiclass Classification to Binary Classification.
 * Currently implements One vs All reduction.
 */
class Multiclass2Binary extends Estimator[Multiclass2BinaryModel]
with Multiclass2BinaryParams {

  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  /** @group setParam */
  def setBaseClassifier(value: Estimator[_ <: Model[_]]): this.type = set(baseClassifier, value)

  /** @group setParam */
  def setNumClasses(value: Int): this.type = set(k, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap) = {
    val map = extractParamMap(paramMap)
    val sqlCtx = dataset.sqlContext
    val schema = dataset.schema
    val numClasses = map(k)

    def getConfiguredClassifier() = {
      import scala.language.existentials
      val baseClassifierClass = getBaseClassifier.getClass()
      val baseClassifier = baseClassifierClass.newInstance()
      baseClassifier
    }
    val models = Range(0, numClasses).par.map { index =>
      val multiclassLabeled = dataset.select(map(labelCol), map(featuresCol))
      val binaryLabeled = multiclassLabeled.map { case Row(label: Double, features: Vector) =>
        val newLabel = if (label.toInt == index) 1.0 else 0.0
        Row(newLabel, features)
      }
      val trainingDataset = sqlCtx.createDataFrame(binaryLabeled, schema)
      val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
      if (handlePersistence) {
        trainingDataset.persist(StorageLevel.MEMORY_AND_DISK)
      }

      getConfiguredClassifier().fit(trainingDataset, paramMap)
    }.toList
    new Multiclass2BinaryModel(this, paramMap, models)
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = true, featuresDataType)
  }

}
