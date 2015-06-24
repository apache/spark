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

package org.apache.spark.ml.tuning

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object ValidationSuite {

  abstract class MyModel extends Model[MyModel]

  class MyEstimator(override val uid: String) extends Estimator[MyModel] with HasInputCol {

    override def validateParams(): Unit = require($(inputCol).nonEmpty)

    override def fit(dataset: DataFrame): MyModel = {
      throw new UnsupportedOperationException
    }

    override def transformSchema(schema: StructType): StructType = {
      throw new UnsupportedOperationException
    }

    override def copy(extra: ParamMap): MyEstimator = defaultCopy(extra)
  }

  class MyEvaluator extends Evaluator {

    override def evaluate(dataset: DataFrame): Double = {
      throw new UnsupportedOperationException
    }

    override val uid: String = "eval"

    override def copy(extra: ParamMap): MyEvaluator = defaultCopy(extra)
  }
}