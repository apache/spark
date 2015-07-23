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

package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.mllib.util.TestingUtils._

class MultilayerPerceptronRegressorSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("XOR function learning") {
    val inputs = Array[Array[Double]](
      Array[Double](0, 0),
      Array[Double](0, 1),
      Array[Double](1, 0),
      Array[Double](1, 1)
    )
    val outputs = Array[Double](0, 1, 1, 0)
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(Array(label)))
    }
    val rddData = sc.parallelize(data, 1)
    val dataFrame = sqlContext.createDataFrame(rddData).toDF("inputCol","outputCol")
    val hiddenLayersTopology = Array[Int](5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val trainer = new MultilayerPerceptronRegressor("mlpr")
    .setInputCol("inputCol")
    .setOutputCol("outputCol")
    .setBlockSize(1)
    .setLayers(layerSizes)
    .setMaxIter(100)
    .setTol(1e-4)
    .setSeed(11L)
    val model = trainer.fit(dataFrame)
    .setInputCol("inputCol")
    model.transform(dataFrame)
    .select("rawPrediction", "outputCol").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x ~== y absTol 1e-3, "Transformed vector is different with expected vector.")
    }  }
}
