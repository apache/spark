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

package org.apache.spark.mllib.feature

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, MultivariateOnlineSummarizer}
import org.apache.spark.rdd.RDD

class StandardScalerSuite extends FunSuite with MLlibTestSparkContext {

  private def computeSummary(data: RDD[Vector]): MultivariateStatisticalSummary = {
    data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
  }

  test("Standardization with dense input") {
    val data = Array(
      Vectors.dense(-2.0, 2.3, 0),
      Vectors.dense(0.0, -1.0, -3.0),
      Vectors.dense(0.0, -5.1, 0.0),
      Vectors.dense(3.8, 0.0, 1.9),
      Vectors.dense(1.7, -0.6, 0.0),
      Vectors.dense(0.0, 1.9, 0.0)
    )

    val dataRDD = sc.parallelize(data, 3)

    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler()
    val standardizer3 = new StandardScaler(withMean = true, withStd = false)

    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val data1 = data.map(model1.transform)
    val data2 = data.map(model2.transform)
    val data3 = data.map(model3.transform)

    val data1RDD = model1.transform(dataRDD)
    val data2RDD = model2.transform(dataRDD)
    val data3RDD = model3.transform(dataRDD)

    val summary = computeSummary(dataRDD)
    val summary1 = computeSummary(data1RDD)
    val summary2 = computeSummary(data2RDD)
    val summary3 = computeSummary(data3RDD)

    assert((data, data1, data1RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((data, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((data, data3, data3RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((data1, data1RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert((data3, data3RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(summary1.mean ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary1.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(summary2.mean !~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary2.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(summary3.mean ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary3.variance ~== summary.variance absTol 1E-5)

    assert(data1(0) ~== Vectors.dense(-1.31527964, 1.023470449, 0.11637768424) absTol 1E-5)
    assert(data1(3) ~== Vectors.dense(1.637735298, 0.156973995, 1.32247368462) absTol 1E-5)
    assert(data2(4) ~== Vectors.dense(0.865538862, -0.22604255, 0.0) absTol 1E-5)
    assert(data2(5) ~== Vectors.dense(0.0, 0.71580142, 0.0) absTol 1E-5)
    assert(data3(1) ~== Vectors.dense(-0.58333333, -0.58333333, -2.8166666666) absTol 1E-5)
    assert(data3(5) ~== Vectors.dense(-0.58333333, 2.316666666, 0.18333333333) absTol 1E-5)
  }


  test("Standardization with sparse input") {
    val data = Array(
      Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
      Vectors.sparse(3, Seq((1, -1.0), (2, -3.0))),
      Vectors.sparse(3, Seq((1, -5.1))),
      Vectors.sparse(3, Seq((0, 3.8), (2, 1.9))),
      Vectors.sparse(3, Seq((0, 1.7), (1, -0.6))),
      Vectors.sparse(3, Seq((1, 1.9)))
    )

    val dataRDD = sc.parallelize(data, 3)

    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler()
    val standardizer3 = new StandardScaler(withMean = true, withStd = false)

    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val data2 = data.map(model2.transform)

    withClue("Standardization with mean can not be applied on sparse input.") {
      intercept[IllegalArgumentException] {
        data.map(model1.transform)
      }
    }

    withClue("Standardization with mean can not be applied on sparse input.") {
      intercept[IllegalArgumentException] {
        data.map(model3.transform)
      }
    }

    val data2RDD = model2.transform(dataRDD)

    val summary2 = computeSummary(data2RDD)

    assert((data, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after standardization.")

    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(summary2.mean !~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(summary2.variance ~== Vectors.dense(1.0, 1.0, 1.0) absTol 1E-5)

    assert(data2(4) ~== Vectors.sparse(3, Seq((0, 0.865538862), (1, -0.22604255))) absTol 1E-5)
    assert(data2(5) ~== Vectors.sparse(3, Seq((1, 0.71580142))) absTol 1E-5)
  }

  test("Standardization with constant input") {
    // When the input data is all constant, the variance is zero. The standardization against
    // zero variance is not well-defined, but we decide to just set it into zero here.
    val data = Array(
      Vectors.dense(2.0),
      Vectors.dense(2.0),
      Vectors.dense(2.0)
    )

    val dataRDD = sc.parallelize(data, 2)

    val standardizer1 = new StandardScaler(withMean = true, withStd = true)
    val standardizer2 = new StandardScaler(withMean = true, withStd = false)
    val standardizer3 = new StandardScaler(withMean = false, withStd = true)

    val model1 = standardizer1.fit(dataRDD)
    val model2 = standardizer2.fit(dataRDD)
    val model3 = standardizer3.fit(dataRDD)

    val data1 = data.map(model1.transform)
    val data2 = data.map(model2.transform)
    val data3 = data.map(model3.transform)

    assert(data1.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
    assert(data2.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
    assert(data3.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
  }

}
