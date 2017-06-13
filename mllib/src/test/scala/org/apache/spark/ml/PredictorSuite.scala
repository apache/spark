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

package org.apache.spark.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class PredictorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import PredictorSuite._

  test("should support all NumericType labels and weights, and not support other types") {
    val df = spark.createDataFrame(Seq(
      (0, 1, Vectors.dense(0, 2, 3)),
      (1, 2, Vectors.dense(0, 3, 9)),
      (0, 3, Vectors.dense(0, 2, 6))
    )).toDF("label", "weight", "features")

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))

    val predictor = new MockPredictor().setWeightCol("weight")

    types.foreach { t =>
      predictor.fit(df.select(col("label").cast(t), col("weight").cast(t), col("features")))
    }

    intercept[IllegalArgumentException] {
      predictor.fit(df.select(col("label").cast(StringType), col("weight"), col("features")))
    }

    intercept[IllegalArgumentException] {
      predictor.fit(df.select(col("label"), col("weight").cast(StringType), col("features")))
    }
  }
}

object PredictorSuite {

  class MockPredictor(override val uid: String)
    extends Predictor[Vector, MockPredictor, MockPredictionModel] with HasWeightCol {

    def this() = this(Identifiable.randomUID("mockpredictor"))

    def setWeightCol(value: String): this.type = set(weightCol, value)

    override def train(dataset: Dataset[_]): MockPredictionModel = {
      require(dataset.schema("label").dataType == DoubleType)
      require(dataset.schema("weight").dataType == DoubleType)
      new MockPredictionModel(uid)
    }

    override def copy(extra: ParamMap): MockPredictor =
      throw new NotImplementedError()
  }

  class MockPredictionModel(override val uid: String)
    extends PredictionModel[Vector, MockPredictionModel] {

    def this() = this(Identifiable.randomUID("mockpredictormodel"))

    override def predict(features: Vector): Double =
      throw new NotImplementedError()

    override def copy(extra: ParamMap): MockPredictionModel =
      throw new NotImplementedError()
  }
}
