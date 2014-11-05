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

import org.apache.spark.ml._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * Logistic regression (example).
 */
class LogisticRegression extends Estimator[LogisticRegressionModel] with OwnParamMap {

  final val maxIter: Param[Int] = new Param(this, "maxIter", "max number of iterations", 100)

  final val regParam: Param[Double] = new Param(this, "regParam", "regularization constant", 0.1)

  final val labelCol: Param[String] = new Param(this, "labelCol", "label column name", "label")

  final val featuresCol: Param[String] =
    new Param(this, "featuresCol", "features column name", "features")

  override final val model: LogisticRegressionModelParams = new LogisticRegressionModelParams {}

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): LogisticRegressionModel = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    import map.implicitMapping
    val instances = dataset.select((labelCol: String).attr, (featuresCol: String).attr)
      .map { case Row(label: Double, features: Vector) =>
        LabeledPoint(label, features)
      }.cache()
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(regParam)
      .setNumIterations(maxIter)
    val lrm = new LogisticRegressionModel(lr.run(instances).weights)
    instances.unpersist()
    this.model.params.foreach { param =>
      if (map.contains(param)) {
        lrm.set(lrm.getParam(param.name), map(param))
      }
    }
    if (!lrm.paramMap.contains(lrm.featuresCol) && map.contains(lrm.featuresCol)) {
      lrm.set(lrm.featuresCol, featuresCol: String)
    }
    lrm
  }
}

trait LogisticRegressionModelParams extends Params with Identifiable {

  final val threshold: Param[Double] =
    new Param(this, "threshold", "threshold for prediction", 0.5)

  final val featuresCol: Param[String] =
    new Param(this, "featuresCol", "features column name", "features")
}

class LogisticRegressionModel(
    val weights: Vector)
    extends Model with LogisticRegressionModelParams with OwnParamMap {

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    import map.implicitMapping
    val score: Vector => Double = (v) => {
      val margin = BLAS.dot(v, weights)
      1.0 / (1.0 + math.exp(-margin))
    }
    val thres: Double = threshold
    val predict: Vector => Double = (v) => {
      if (score(v) > thres) 1.0 else 0.0
    }
    dataset.select(
      Star(None),
      score.call((featuresCol: String).attr) as 'score,
      predict.call((featuresCol: String).attr) as 'prediction)
  }
}
