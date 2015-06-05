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

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class StandardScalerSuite extends FunSuite with MLlibTestSparkContext{

  @transient var data: Array[Vector] = _
  @transient var dataFrame: DataFrame = _
  @transient var resWithStd: Array[Vector] = _
  @transient var resWithMean: Array[Vector] = _
  @transient var resWithBoth: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.dense(-2.0, 2.3, 0.0),
      Vectors.dense(0.0, -1.0, -3.0),
      Vectors.dense(0.0, -5.1, 0.0),
      Vectors.dense(3.8, 0.0, 1.9),
      Vectors.dense(1.7, -0.6, 0.0),
      Vectors.dense(0.0, 1.9, 0.0)
    )
    resWithMean  = Array(
      Vectors.dense(-2.583333333333, 2.716666666667, 0.183333333333),
      Vectors.dense(-0.583333333333, -0.583333333333, -2.816666666667),
      Vectors.dense(-0.583333333333, -4.683333333333, 0.183333333333),
      Vectors.dense(3.216666666667, 0.416666666667, 2.083333333333),
      Vectors.dense(1.116666666667, -0.183333333333, 0.183333333333),
      Vectors.dense(-0.583333333333, 2.316666666667, 0.183333333333)
    )
    resWithStd = Array(
      Vectors.dense(-1.018281014244, 0.866496453808, 0.0),
      Vectors.dense(0.0, -0.376737588612, -1.90436210586),
      Vectors.dense(0, -1.921361701921, 0),
      Vectors.dense(1.934733927063, 0, 1.20609600038),
      Vectors.dense(0.865538862107, -0.226042553167, 0.0),
      Vectors.dense(0, 0.715801418363, 0)
    )
    resWithBoth = Array(
      Vectors.dense(-1.315279643398, 1.0234704490628, 0.116377684247),
      Vectors.dense(-0.296998629154, -0.2197635933570, -1.787984421610),
      Vectors.dense(-0.296998629154, -1.7643877066665, 0.116377684247),
      Vectors.dense(1.637735297909, 0.1569739952550, 1.322473684622),
      Vectors.dense(0.568540232953, -0.0690685579122, 0.116377684247),
      Vectors.dense(-0.296998629154, 0.8727754136179, 0.116377684247)
    )

    dataFrame = sqlContext.createDataFrame(data.map(Tuple1(_))).toDF("features")
  }

  def collectResult(result: DataFrame): Array[Vector] = {
    result.select("standarded_features").collect().map {
      case Row(features: Vector) => features
    }
  }

  def assertValues(lhs: Array[Vector], rhs: Array[Vector]): Unit = {
    assert((lhs, rhs).zipped.forall { (vector1, vector2) =>
      vector1 ~== vector2 absTol 1E-5
    }, "The vector value is not correct after standardization.")
  }

  test("Standardization with default parameter") {
    val standardscaler0 =  new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standarded_features")
      .fit(dataFrame)

    val res = collectResult(standardscaler0.transform(dataFrame))
    assertValues(res, resWithStd)
  }

  test("Standardization with setter") {
    val standardscaler1 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standarded_features")
      .setWithMean(true)
      .setWithStd(true)
      .fit(dataFrame)

    val standardscaler2 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standarded_features")
      .setWithMean(true)
      .setWithStd(false)
      .fit(dataFrame)

    val standardscaler3 = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standarded_features")
      .setWithMean(false)
      .setWithStd(false)
      .fit(dataFrame)

    val res1 = collectResult(standardscaler1.transform(dataFrame))
    val res2 = collectResult(standardscaler2.transform(dataFrame))
    val res3 = collectResult(standardscaler3.transform(dataFrame))

    assertValues(res1, resWithBoth)
    assertValues(res2, resWithMean)
    assertValues(res3, data)
  }
}
