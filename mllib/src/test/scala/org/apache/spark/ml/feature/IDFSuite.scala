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
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.Row

class IDFSuite extends SparkFunSuite with MLlibTestSparkContext {

  def scaleDataWithIDF(dataSet: Array[Vector], model: Vector): Array[Vector] = {
    dataSet.map {
      case data: DenseVector =>
        val res = data.toArray.zip(model.toArray).map { case (x, y) => x * y }
        Vectors.dense(res)
      case data: SparseVector =>
        val res = data.indices.zip(data.values).map { case (id, value) =>
          (id, value * model(id))
        }
        Vectors.sparse(data.size, res)
    }
  }

  test("compute IDF with default parameter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((numOfData + 1.0) / (x + 1.0))
    })
    val expected = scaleDataWithIDF(data, idf)

    val df = sqlContext.createDataFrame(data.zip(expected)).toDF("features", "expected")

    val idfModel = new IDF()
      .setInputCol("features")
      .setOutputCol("idfValue")
      .fit(df)

    idfModel.transform(df).select("idfValue", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x ~== y absTol 1e-5, "Transformed vector is different with expected vector.")
    }
  }

  test("compute IDF with setter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) math.log((numOfData + 1.0) / (x + 1.0)) else 0
    })
    val expected = scaleDataWithIDF(data, idf)

    val df = sqlContext.createDataFrame(data.zip(expected)).toDF("features", "expected")

    val idfModel = new IDF()
      .setInputCol("features")
      .setOutputCol("idfValue")
      .setMinDocFreq(1)
      .fit(df)

    idfModel.transform(df).select("idfValue", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x ~== y absTol 1e-5, "Transformed vector is different with expected vector.")
    }
  }
}
