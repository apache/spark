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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class IDFSuite extends FunSuite with MLlibTestSparkContext {

  @transient var dataFrame: DataFrame = _
  @transient var idf: IDF = _
  @transient var resultWithDefaultParam: Array[Vector] = _
  @transient var resultWithSetParam: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val n = 4
    val data = Array(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
    val m = data.size

    val expectedDefaultModel = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((m + 1.0) / (x + 1.0))
    })

    val expectedSetModel = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) {
        math.log((m + 1.0) / (x + 1.0))
      } else {
        0
      }
    })

    val result: (Vector) => Array[Vector] = { model: Vector =>
      Array(
        Vectors.sparse(n, Array(1, 3), Array(1.0 * model(1), 2.0 * model(3))),
        Vectors.dense(0.0, 1.0 * model(1), 2.0 * model(2), 3.0 * model(3)),
        Vectors.sparse(n, Array(1), Array(1.0 * model(1)))
      )
    }

    resultWithDefaultParam = result(expectedDefaultModel)

    resultWithSetParam = result(expectedSetModel)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    dataFrame = sc.parallelize(data, 2).map(Tuple1.apply).toDF("features")
    idf = new IDF()
      .setInputCol("features")
      .setOutputCol("idf_value")
  }

  def collectResult(result: DataFrame): Array[Vector] = {
    result.select("idf_value").collect().map {
      case Row(features: Vector) => features
    }
  }

  def assertValues(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall { (vector1, vector2) =>
      vector1 ~== vector2 absTol 1E-5
    }, "The vector value is not correct after IDF.")
  }

  test("Normalization with default parameter") {
    val idfModel = idf.fit(dataFrame)
    val tfIdf = collectResult(idfModel.transform(dataFrame))

    assertValues(tfIdf, resultWithDefaultParam)
  }

  test("Normalization with setter") {
    val idfModel = idf.setMinDocFreq(1).fit(dataFrame)
    val tfIdf = collectResult(idfModel.transform(dataFrame))

    assertValues(tfIdf, resultWithSetParam)
  }
}
