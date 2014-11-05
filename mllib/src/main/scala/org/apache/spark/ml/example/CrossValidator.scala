package org.apache.spark.ml.example

import com.github.fommil.netlib.F2jBLAS

import org.apache.spark.ml._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SchemaRDD

class CrossValidator extends Estimator[CrossValidatorModel] with Params with OwnParamMap {

  private val f2jBLAS = new F2jBLAS

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  val evaluator: Param[Evaluator] = new Param(this, "evaluator", "evaluator for selection")

  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  val numFolds: Param[Int] = new Param(this, "numFolds", "number of folds for cross validation", 3)

  /**
   * Fits a single model to the input data with provided parameter map.
   *
   * @param dataset input dataset
   * @param paramMap parameters
   * @return fitted model
   */
  override def fit(dataset: SchemaRDD, paramMap: ParamMap): CrossValidatorModel = {
    val sqlCtx = dataset.sqlContext
    val map = this.paramMap ++ paramMap
    val schema = dataset.schema
    val est = map(estimator)
    val eval = map(evaluator)
    val epm = map(estimatorParamMaps)
    val numModels = epm.size
    val metrics = new Array[Double](epm.size)
    val splits = MLUtils.kFold(dataset, map(numFolds), 0)
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sqlCtx.applySchema(training, schema).cache()
      val validationDataset = sqlCtx.applySchema(validation, schema).cache()
      epm.zipWithIndex.foreach { case (m, idx) =>
        println(s"Training split $splitIndex with parameters:\n$m")
        val model = est.fit(trainingDataset, m).asInstanceOf[Model]
        val metric = eval.evaluate(model.transform(validationDataset, m), map)
        println(s"Got metric $metric.")
        metrics(idx) += metric
      }
    }
    f2jBLAS.dscal(numModels, 1.0 / map(numFolds), metrics, 1)
    println(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) = metrics.zipWithIndex.maxBy(_._1)
    println("Best set of parameters:\n" + epm(bestIndex))
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model]
    new CrossValidatorModel(bestModel, bestMetric / map(numFolds))
  }
}

class CrossValidatorModel(bestModel: Model, metric: Double) extends Model {
  def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    bestModel.transform(dataset, paramMap)
  }
}

