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
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SchemaRDD

class CrossValidator extends Estimator[CrossValidatorModel] with Params {

  private val f2jBLAS = new F2jBLAS

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")
  def setEstimator(value: Estimator[_]): this.type = { set(estimator, value); this }
  def getEstimator: Estimator[_] = get(estimator)

  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")
  def getEstimatorParamMaps: Array[ParamMap] = get(estimatorParamMaps)
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = {
    set(estimatorParamMaps, value)
    this
  }

  val evaluator: Param[Evaluator] = new Param(this, "evaluator", "evaluator for selection")
  def setEvaluator(value: Evaluator): this.type = { set(evaluator, value); this }
  def getEvaluator: Evaluator = get(evaluator)

  val numFolds: Param[Int] =
    new IntParam(this, "numFolds", "number of folds for cross validation", Some(3))
  def setNumFolds(value: Int): this.type = { set(numFolds, value); this }
  def getNumFolds: Int = get(numFolds)

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
      // multi-model training
      println(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model]]
      var i = 0
      while(i < numModels) {
        val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)), map)
        println(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
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
