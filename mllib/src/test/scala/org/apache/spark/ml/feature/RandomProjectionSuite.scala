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


import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame


case class TestRow(label: Double, vector: Vector)

class RandomProjectionSuite extends SparkFunSuite with MLlibTestSparkContext {
  var data: DataFrame = _
  var normal: MultivariateGaussian = _
  var estimator: RandomProjection = _
  val randomOutputCol = s"testOutputColumn_${Math.random()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    estimator = new RandomProjection()

    val mean = DenseVector[Double](1, 1, 2, 2)
    val covar = DenseMatrix(
      (1.0, 0.1, -0.1, 0.5),
      (0.1, 1.0, -0.4, 0.3),
      (-0.1, -0.4, 1.0, -0.4),
      (0.5, 0.3, -0.4, 1.0)
    )

    normal = new MultivariateGaussian(mean, covar)

    data = sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(Seq(
      TestRow(1.0, Vectors.dense(normal.draw.toArray)),
      TestRow(1.0, Vectors.dense(normal.draw.toArray)),
      TestRow(1.0, Vectors.dense(normal.draw.toArray)),
      TestRow(1.0, Vectors.dense(normal.draw.toArray)),
      TestRow(1.0, Vectors.dense(normal.draw.toArray)),
      TestRow(1.0, Vectors.dense(normal.draw.toArray)),
      TestRow(1.0, Vectors.dense(normal.draw.toArray))
    )))
  }


  private def getBaseModel(outCol: String, intrinsicDimension: Int = 20) = {
    estimator.setIntrinsicDimension(intrinsicDimension)
    // generic schema will call the values "vector"
    estimator.setInputCol("vector")
    estimator.setOutputCol(outCol)
    estimator.fit(data)
  }

  test("transform output column correctly") {
    val model = getBaseModel(randomOutputCol)
    val transformed = model.transformSchema(data.schema)
    assert(transformed.fieldNames.contains(randomOutputCol))
  }

  test("reduce the dimension correctly and preserve amount of instances") {
    val newDimension = 3
    val output = "features"
    val model = getBaseModel(outCol = output, intrinsicDimension = newDimension)
    val reducedDF = model.transform(data)

    val reducedData = reducedDF.select(output).collect()
    val sample = reducedData(0).getAs[Vector](0)
    assert(sample.size == newDimension)
    assert(reducedData.length == data.rdd.collect().length)
  }

  test("required params are set") {
    val estimator = new RandomProjection()
    intercept[Exception] {
      estimator.fit(data)
    }

    estimator.setIntrinsicDimension(20)
    intercept[Exception] {
      estimator.fit(data)
    }

    estimator.setInputCol("vector") // required in this test case
    intercept[Exception] {
      estimator.fit(data)
    }

    estimator.setOutputCol("output")
    val model = estimator.fit(data)

    assert(model.isInstanceOf[RPModel])
  }

  test("ensure the input column exists while fitting") {
    val estimator = new RandomProjection()
    estimator.setInputCol("unknown")
    estimator.setIntrinsicDimension(20)
    estimator.setOutputCol("output")

    intercept[IllegalArgumentException] {
      estimator.fit(data)
    }

    // now the correct one is set
    estimator.setInputCol("vector")
    val model = estimator.fit(data)

    assert(model.isInstanceOf[RPModel])
  }


  test("transform the outputColumn correctly") {
    estimator.setIntrinsicDimension(20)
    // generic schema will call the values "vector"
    estimator.setInputCol("vector")
    val testOutputColumn = s"testOutputColumn_${Math.random()}"
    estimator.setOutputCol(testOutputColumn)
    val transformed = estimator.transformSchema(data.schema)

    assert(transformed.fieldNames.contains(testOutputColumn))
  }

  test("extract the original dimension correctly") {
    val estimator = new RandomProjection()
    estimator.setInputCol("vector")
    estimator.setIntrinsicDimension(20)
    estimator.setOutputCol("output")
    assert(estimator.getOriginalDimension(data).equals(4))
  }

}
