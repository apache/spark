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

package org.apache.spark.ml.api.r

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

private[r] object SparkRWrappers {
  def fitRModelFormula(
      value: String,
      df: DataFrame,
      family: String,
      lambda: Double,
      alpha: Double): PipelineModel = {
    val formula = new RFormula().setFormula(value)
    val estimator = family match {
      case "gaussian" => new LinearRegression()
        .setRegParam(lambda)
        .setElasticNetParam(alpha)
        .setFitIntercept(formula.hasIntercept)
      case "binomial" => new LogisticRegression()
        .setRegParam(lambda)
        .setElasticNetParam(alpha)
        .setFitIntercept(formula.hasIntercept)
    }
    val pipeline = new Pipeline().setStages(Array(formula, estimator))
    pipeline.fit(df)
  }
}
