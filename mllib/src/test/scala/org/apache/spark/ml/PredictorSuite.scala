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

  test("multiple columns for features should work well without side effect") {
    // Should fail due to not supporting multiple columns
    intercept[IllegalArgumentException] {
      new MockPredictor(false).setFeaturesCol(Array("feature1", "feature2", "feature3"))
    }

    // Only use multiple columns for features
    val df = spark.createDataFrame(Seq(
      (0, 1, 0, 2, 3),
      (1, 2, 0, 3, 9),
      (0, 3, 0, 2, 6)
    )).toDF("label", "weight", "feature1", "feature2", "feature3")

    val predictor = new MockPredictor().setWeightCol("weight")
      .setFeaturesCol(Array("feature1", "feature2", "feature3"))
    predictor.fit(df)

    // Should fail due to wrong type for column "feature1" in schema
    intercept[IllegalArgumentException] {
      predictor.fit(df.select(col("label"), col("weight"),
        col("feature1").cast(StringType), col("feature2"), col("feature3")))
    }

    val df2 = df.toDF("label", "weight", "features", "feature2", "feature3")
    // Should fail due to missing "feature1" in schema
    intercept[IllegalArgumentException] {
      predictor.setFeaturesCol(Array("feature1", "feature2", "feature3")).fit(df2)
    }

    // Should fail due to wrong type in schema for single column of features.
    // "features", being equal to the single column name, are provided in "df2" schema
    // but not specified as multi-columns, so treat it as single column and should be
    // "Vector", but actually "Int".
    intercept[IllegalArgumentException] {
      predictor.setFeaturesCol(Array("feature2", "feature3")).fit(df2)
    }
  }
}

object PredictorSuite {

  class MockPredictor(override val uid: String, val supportMultiColumns: Boolean)
    extends Predictor[Vector, MockPredictor, MockPredictionModel] with HasWeightCol {

    def this(supportMulti: Boolean) = this(Identifiable.randomUID("mockpredictor"), supportMulti)
    def this() = this(true)

    def setWeightCol(value: String): this.type = set(weightCol, value)

    override def train(dataset: Dataset[_]): MockPredictionModel = {
      require(dataset.schema("label").dataType == DoubleType)
      require(dataset.schema("weight").dataType == DoubleType)
      new MockPredictionModel(uid)
    }

    override def copy(extra: ParamMap): MockPredictor =
      throw new UnsupportedOperationException()

    override protected def isSupportMultiColumnsForFeatures: Boolean = supportMultiColumns
  }

  class MockPredictionModel(override val uid: String)
    extends PredictionModel[Vector, MockPredictionModel] {

    def this() = this(Identifiable.randomUID("mockpredictormodel"))

    override def predict(features: Vector): Double =
      throw new UnsupportedOperationException()

    override def copy(extra: ParamMap): MockPredictionModel =
      throw new UnsupportedOperationException()
  }
}
