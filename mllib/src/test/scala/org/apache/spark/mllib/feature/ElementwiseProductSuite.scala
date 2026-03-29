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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.ArrayImplicits._

class ElementwiseProductSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("elementwise (hadamard) product should properly apply vector to dense data set") {
    val denseData = Array(
      Vectors.dense(1.0, 4.0, 1.9, -9.0)
    )
    val scalingVec = Vectors.dense(2.0, 0.5, 0.0, 0.25)
    val transformer = new ElementwiseProduct(scalingVec)
    val transformedData = transformer.transform(sc.makeRDD(denseData.toImmutableArraySeq))
    val transformedVecs = transformedData.collect()
    val transformedVec = transformedVecs(0)
    val expectedVec = Vectors.dense(2.0, 2.0, 0.0, -2.25)
    assert(transformedVec ~== expectedVec absTol 1E-5,
      s"Expected transformed vector $expectedVec but found $transformedVec")
  }

  test("elementwise (hadamard) product should properly apply vector to sparse data set") {
    val sparseData = Array(
      Vectors.sparse(3, Seq((1, -1.0), (2, -3.0)))
    )
    val dataRDD = sc.parallelize(sparseData.toImmutableArraySeq, 3)
    val scalingVec = Vectors.dense(1.0, 0.0, 0.5)
    val transformer = new ElementwiseProduct(scalingVec)
    val data2 = sparseData.map(transformer.transform)
    val data2RDD = transformer.transform(dataRDD)

    assert(sparseData.lazyZip(data2).lazyZip(data2RDD.collect()).forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after hadamard product")

    assert(data2.lazyZip(data2RDD.collect()).forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert(data2(0) ~== Vectors.sparse(3, Seq((1, 0.0), (2, -1.5))) absTol 1E-5)
  }
}
