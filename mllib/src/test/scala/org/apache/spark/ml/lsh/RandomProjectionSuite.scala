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

package org.apache.spark.ml.lsh

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RandomProjectionSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("RandomProjection") {
    val data = {
      for (i <- -20 until 20; j <- -20 until 20) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    // Project from 2 dimensional Euclidean Space to 10 dimensions
    val rp = new RandomProjection()
      .setOutputDim(10)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(3.0)

    val model = rp.fit(df)

    model.getModelDataset.show()
    model.approxNearestNeighbors(Vectors.dense(1.2, 3.4), k = 20).show()
  }
}
