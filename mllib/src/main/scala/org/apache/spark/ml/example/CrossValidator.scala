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

package org.apache.spark.ml.example

import com.github.fommil.netlib.F2jBLAS

import org.apache.spark.ml._
import org.apache.spark.ml.param.{ParamMap, Params, Param}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SchemaRDD

trait HasEstimator extends Params {

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  def setEstimator(estimator: Estimator[_]): this.type = {
    set(this.estimator, estimator)
    this
  }

  def getEstimator: Estimator[_] = {
    get(this.estimator)
  }
}

trait HasEvaluator extends Params {

  val evaluator: Param[Evaluator] = new Param(this, "evaluator", "evaluator for selection")

  def setEvaluator(evaluator: Evaluator): this.type = {
    set(this.evaluator, evaluator)
    this
  }

  def getEvaluator: Evaluator = {
    get(evaluator)
  }
}

trait HasEstimatorParamMaps extends Params {

  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  def setEstimatorParamMaps(estimatorParamMaps: Array[ParamMap]): this.type = {
    set(this.estimatorParamMaps, estimatorParamMaps)
    this
  }

  def getEstimatorParamMaps: Array[ParamMap] = {
    get(estimatorParamMaps)
  }
}


class CrossValidator extends Estimator[CrossValidatorModel] with Params
    with HasEstimator with HasEstimatorParamMaps with HasEvaluator {

  private val f2jBLAS = new F2jBLAS

  // Overwrite return type for Java users.
  override def setEstimator(estimator: Estimator[_]): this.type = super.setEstimator(estimator)
  override def setEstimatorParamMaps(estimatorParamMaps: Array[ParamMap]): this.type =
    super.setEstimatorParamMaps(estimatorParamMaps)
  override def setEvaluator(evaluator: Evaluator): this.type = super.setEvaluator(evaluator)

  val numFolds: Param[Int] = new Param(this, "numFolds", "number of folds for cross validation", 3)

  def setNumFolds(numFolds: Int): this.type = {
    set(this.numFolds, numFolds)
    this
  }

  def getNumFolds: Int = {
    get(numFolds)
  }

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

