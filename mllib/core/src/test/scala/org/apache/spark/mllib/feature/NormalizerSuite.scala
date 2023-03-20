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

import breeze.linalg.{norm => brzNorm}

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class NormalizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  val data = Array(
    Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
    Vectors.dense(0.0, 0.0, 0.0),
    Vectors.dense(0.6, -1.1, -3.0),
    Vectors.sparse(3, Seq((1, 0.91), (2, 3.2))),
    Vectors.sparse(3, Seq((0, 5.7), (1, 0.72), (2, 2.7))),
    Vectors.sparse(3, Seq())
  )

  lazy val dataRDD = sc.parallelize(data, 3)

  test("Normalization using L1 distance") {
    val l1Normalizer = new Normalizer(1)

    val data1 = data.map(l1Normalizer.transform)
    val data1RDD = l1Normalizer.transform(dataRDD)

    assert((data, data1, data1RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after normalization.")

    assert((data1, data1RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(brzNorm(data1(0).asBreeze, 1) ~== 1.0 absTol 1E-5)
    assert(brzNorm(data1(2).asBreeze, 1) ~== 1.0 absTol 1E-5)
    assert(brzNorm(data1(3).asBreeze, 1) ~== 1.0 absTol 1E-5)
    assert(brzNorm(data1(4).asBreeze, 1) ~== 1.0 absTol 1E-5)

    assert(data1(0) ~== Vectors.sparse(3, Seq((0, -0.465116279), (1, 0.53488372))) absTol 1E-5)
    assert(data1(1) ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(data1(2) ~== Vectors.dense(0.12765957, -0.23404255, -0.63829787) absTol 1E-5)
    assert(data1(3) ~== Vectors.sparse(3, Seq((1, 0.22141119), (2, 0.7785888))) absTol 1E-5)
    assert(data1(4) ~== Vectors.dense(0.625, 0.07894737, 0.29605263) absTol 1E-5)
    assert(data1(5) ~== Vectors.sparse(3, Seq()) absTol 1E-5)
  }

  test("Normalization using L2 distance") {
    val l2Normalizer = new Normalizer()

    val data2 = data.map(l2Normalizer.transform)
    val data2RDD = l2Normalizer.transform(dataRDD)

    assert((data, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after normalization.")

    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(brzNorm(data2(0).asBreeze, 2) ~== 1.0 absTol 1E-5)
    assert(brzNorm(data2(2).asBreeze, 2) ~== 1.0 absTol 1E-5)
    assert(brzNorm(data2(3).asBreeze, 2) ~== 1.0 absTol 1E-5)
    assert(brzNorm(data2(4).asBreeze, 2) ~== 1.0 absTol 1E-5)

    assert(data2(0) ~== Vectors.sparse(3, Seq((0, -0.65617871), (1, 0.75460552))) absTol 1E-5)
    assert(data2(1) ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(data2(2) ~== Vectors.dense(0.184549876, -0.3383414, -0.922749378) absTol 1E-5)
    assert(data2(3) ~== Vectors.sparse(3, Seq((1, 0.27352993), (2, 0.96186349))) absTol 1E-5)
    assert(data2(4) ~== Vectors.dense(0.897906166, 0.113419726, 0.42532397) absTol 1E-5)
    assert(data2(5) ~== Vectors.sparse(3, Seq()) absTol 1E-5)
  }

  test("Normalization using L^Inf distance.") {
    val lInfNormalizer = new Normalizer(Double.PositiveInfinity)

    val dataInf = data.map(lInfNormalizer.transform)
    val dataInfRDD = lInfNormalizer.transform(dataRDD)

    assert((data, dataInf, dataInfRDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after normalization.")

    assert((dataInf, dataInfRDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(dataInf(0).toArray.map(math.abs).max ~== 1.0 absTol 1E-5)
    assert(dataInf(2).toArray.map(math.abs).max ~== 1.0 absTol 1E-5)
    assert(dataInf(3).toArray.map(math.abs).max ~== 1.0 absTol 1E-5)
    assert(dataInf(4).toArray.map(math.abs).max ~== 1.0 absTol 1E-5)

    assert(dataInf(0) ~== Vectors.sparse(3, Seq((0, -0.86956522), (1, 1.0))) absTol 1E-5)
    assert(dataInf(1) ~== Vectors.dense(0.0, 0.0, 0.0) absTol 1E-5)
    assert(dataInf(2) ~== Vectors.dense(0.2, -0.36666667, -1.0) absTol 1E-5)
    assert(dataInf(3) ~== Vectors.sparse(3, Seq((1, 0.284375), (2, 1.0))) absTol 1E-5)
    assert(dataInf(4) ~== Vectors.dense(1.0, 0.12631579, 0.473684211) absTol 1E-5)
    assert(dataInf(5) ~== Vectors.sparse(3, Seq()) absTol 1E-5)
  }

}
