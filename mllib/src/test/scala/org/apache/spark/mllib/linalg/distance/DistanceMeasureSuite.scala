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

package org.apache.spark.mllib.linalg.distance

import breeze.linalg.{DenseVector => DBV, Vector => BV}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.TestingUtils._
import org.scalatest.FunSuite

class DistanceMeasureSuite extends FunSuite {

  test("check the implicit method with (Vector, Vector) => Double") {
    val vector1 = Vectors.dense(1, 1, 1, 1, 1).toBreeze
    val vector2 = Vectors.dense(2, 2, 2, 2, 2).toBreeze

    val fun = (v1: Vector, v2: Vector) => (v1.toBreeze - v2.toBreeze).norm(1)
    val measure: DistanceMeasure = fun
    val distance = measure(vector1, vector2)
    assert(distance == 5)
  }

  test("check the implicit method with (BV[Double], BV[Double]) => Double") {
    val vector1 = Vectors.dense(1, 1, 1, 1, 1).toBreeze
    val vector2 = Vectors.dense(2, 2, 2, 2, 2).toBreeze

    val fun = (v1: BV[Double], v2: BV[Double]) => (v1 - v2).norm(1)
    val measure: DistanceMeasure = fun
    val distance = measure(vector1, vector2)
    assert(distance == 5)
  }
}

class SquaredEuclideanDistanceMeasureSuite extends GeneralDistanceMeasureSuite {
  override def distanceFactory = new SquaredEuclideanDistanceMeasure

  test("the distance should be 45.0") {
    val v1 = Vectors.dense(2, 3).toBreeze
    val v2 = Vectors.dense(5, 9).toBreeze

    val distance = distanceFactory(v1, v2)
    val expected = 45.0
    assert(distance == 45.0, s"the distance should be nearly equal to 45.0, but ${distance}")
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)

    val distance = SquaredEuclideanDistanceMeasure(vector1, vector2)
    assert(distance ~== 8.0 absTol 1.0E-10)
  }
}

class CosineDistanceMeasureSuite extends GeneralDistanceMeasureSuite {
  override def distanceFactory = new CosineDistanceMeasure

  test("concreate distance check") {
    val vector1 = Vectors.dense(1.0, 2.0).toBreeze
    val vector2 = Vectors.dense(3.0, 4.0).toBreeze
    val distance = distanceFactory(vector1, vector2)
    assert(distance ~== 0.01613008990009257 absTol 1.0E-10)
  }

  test("two vectors have the same magnitude") {
    val vector1 = Vectors.dense(1.0, 1.0).toBreeze
    val vector2 = Vectors.dense(2.0, 2.0).toBreeze
    val distance = distanceFactory(vector1, vector2)
    assert(distance ~== 0.0 absTol 1.0E-10)
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = CosineDistanceMeasure(vector1, vector2)
    assert(distance ~== 0.01613008990009257 absTol 1.0E-10)
  }
}

class TanimotoDistanceMeasureSuite extends GeneralDistanceMeasureSuite {
  override def distanceFactory = new TanimotoDistanceMeasure

  test("calculate tanimoto distance for 2-dimension") {
    val vector1 = Vectors.dense(1.0, 2.0).toBreeze
    val vector2 = Vectors.dense(3.0, 4.0).toBreeze
    val distance = distanceFactory(vector1, vector2)
    assert(distance ~== 0.42105263157894735 absTol 1.0E-10)
  }

  test("calculate tanimoto distance for 3-dimension") {
    val vector1 = Vectors.dense(1.0, 2.0, 3.0).toBreeze
    val vector2 = Vectors.dense(4.0, 5.0, 6.0).toBreeze

    val distance = distanceFactory(vector1, vector2)
    assert(distance ~== 0.4576271186440678 absTol 1.0E-10)
  }

  test("calculate tanimoto distance for 6-dimension") {
    val vector1 = Vectors.dense(1.0, 1.0, 1.0, 1.0, 1.0, 1.0).toBreeze
    val vector2 = Vectors.dense(-1.0, -1.0, -1.0, -1.0, -1.0, -1.0).toBreeze
    val distance = distanceFactory(vector1, vector2)
    assert(distance ~== 1.3333333333 absTol 1.0E-10)
  }

  test("called by the companion object") {
    val vector1 = Vectors.dense(1.0, 2.0)
    val vector2 = Vectors.dense(3.0, 4.0)
    val distance = TanimotoDistanceMeasure(vector1, vector2)
    assert(distance ~== 0.42105263157894735 absTol 1.0E-10)
  }
}
