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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class MinMaxScalerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
  import testImplicits._

  test("MinMaxScaler fit basic case") {
    val data = Array(
      Vectors.dense(1, 0, Long.MinValue),
      Vectors.dense(2, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(3, Long.MaxValue)),
      Vectors.sparse(3, Array(0), Array(1.5)))

    val expected: Array[Vector] = Array(
      Vectors.dense(-5, 0, -5),
      Vectors.dense(0, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(5, 5)),
      Vectors.sparse(3, Array(0), Array(-2.5)))

    val df = spark.createDataFrame(data.zip(expected)).toDF("features", "expected")
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
      .setMin(-5)
      .setMax(5)

    val model = scaler.fit(df)
    model.transform(df).select("expected", "scaled").collect()
      .foreach { case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1.equals(vector2), "Transformed vector is different with expected.")
    }

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }

  test("MinMaxScaler arguments max must be larger than min") {
    withClue("arguments max must be larger than min") {
      val dummyDF = Seq((1, Vectors.dense(1.0, 2.0))).toDF("id", "feature")
      intercept[IllegalArgumentException] {
        val scaler = new MinMaxScaler().setMin(10).setMax(0).setInputCol("feature")
        scaler.transformSchema(dummyDF.schema)
      }
      intercept[IllegalArgumentException] {
        val scaler = new MinMaxScaler().setMin(0).setMax(0).setInputCol("feature")
        scaler.transformSchema(dummyDF.schema)
      }
    }
  }

  test("MinMaxScaler read/write") {
    val t = new MinMaxScaler()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMax(1.0)
      .setMin(-1.0)
    testDefaultReadWrite(t)
  }

  test("MinMaxScalerModel read/write") {
    val instance = new MinMaxScalerModel(
        "myMinMaxScalerModel", Vectors.dense(-1.0, 0.0), Vectors.dense(1.0, 10.0))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMin(-1.0)
      .setMax(1.0)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.originalMin === instance.originalMin)
    assert(newInstance.originalMax === instance.originalMax)
  }

  test("MinMaxScaler should remain NaN value") {
    val data = Array(
      Vectors.dense(1, Double.NaN, 2.0, 2.0),
      Vectors.dense(2, 2.0, 0.0, 3.0),
      Vectors.dense(3, Double.NaN, 0.0, 1.0),
      Vectors.dense(6, 2.0, 2.0, Double.NaN))

    val expected: Array[Vector] = Array(
      Vectors.dense(-5.0, Double.NaN, 5.0, 0.0),
      Vectors.dense(-3.0, 0.0, -5.0, 5.0),
      Vectors.dense(-1.0, Double.NaN, -5.0, -5.0),
      Vectors.dense(5.0, 0.0, 5.0, Double.NaN))

    val df = spark.createDataFrame(data.zip(expected)).toDF("features", "expected")
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
      .setMin(-5)
      .setMax(5)

    val model = scaler.fit(df)
    model.transform(df).select("expected", "scaled").collect()
      .foreach { case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1.equals(vector2), "Transformed vector is different with expected.")
      }
  }
}
