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

package org.apache.spark.ml.r

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

class MultilayerPerceptronClassifierWrapperSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = spark.createDataFrame(Seq(
      (Vectors.dense(0.0, 0.0), 0.0),
      (Vectors.dense(0.0, 1.0), 1.0),
      (Vectors.dense(1.0, 0.0), 1.0),
      (Vectors.dense(1.0, 1.0), 0.0))
    ).toDF("features", "label")
  }

  test("fit() method should work") {
    val layers = Array[Int](2, 5, 2)

    val trainer = new MultilayerPerceptronClassifierWrapper()

    val model = trainer.fit(dataset)
    val result = model.transform(dataset)
    val predictionAndLabels = result.select("prediction", "label").collect()
    predictionAndLabels.foreach { case Row(p: Double, l: Double) =>
      assert(p == l)
    }
  }
}

