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

import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite
import org.apache.spark.mllib.util.TestingUtils._

class ElementwiseProductSuite extends FunSuite with MLlibTestSparkContext{

  val denseData =  Array(
    Vectors.dense(1.0, 1.0, 0.0, 0.0),
    Vectors.dense(1.0, 2.0, -3.0, 0.0),
    Vectors.dense(1.0, 3.0, 0.0, 0.0),
    Vectors.dense(1.0, 4.0, 1.9, -9.0),
    Vectors.dense(1.0, 5.0, 0.0, 0.0)
  )

  val sparseData = Array(
    Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
    Vectors.sparse(3, Seq((1, -1.0), (2, -3.0))),
    Vectors.sparse(3, Seq((1, -5.1))),
    Vectors.sparse(3, Seq((0, 3.8), (2, 1.9))),
    Vectors.sparse(3, Seq((0, 1.7), (1, -0.6))),
    Vectors.sparse(3, Seq((1, 1.9)))
  )

  val scalingVector = Vectors.dense(2.0, 0.5, 0.0, 0.25)

  test("elementwise (hadamard) product should properly apply vector to dense data set") {

    val transformer = new ElementwiseProduct(scalingVector)
    val transformedData = transformer.transform(sc.makeRDD(denseData))

    val transformedVecs = transformedData.collect()

    val fourthVec = transformedVecs.apply(3).toArray

    assert(fourthVec.apply(0) === 2.0, "product by 2.0 should have been applied")
    assert(fourthVec.apply(1) === 2.0, "product by 0.5 should have been applied")
    assert(fourthVec.apply(2) === 0.0, "product by 0.0 should have been applied")
    assert(fourthVec.apply(3) === -2.25, "product by 0.25 should have been applied")
  }

  test("elementwise (hadamard) product should properly apply vector to sparse data set") {

    val dataRDD = sc.parallelize(sparseData, 3)

    val scalingVec = Vectors.dense(1.0, 0.0, 0.5)

    val transformer = new ElementwiseProduct(scalingVec)

    val data2 = sparseData.map(transformer.transform)
    val data2RDD = transformer.transform(dataRDD)

    assert((sparseData, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after hadamard product")

    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))

    assert(data2(0) ~== Vectors.sparse(3, Seq((0, -2.0), (1, 0.0))) absTol 1E-5)
    assert(data2(1) ~== Vectors.sparse(3, Seq((1, 0.0), (2, -1.5))) absTol 1E-5)
  }
}
