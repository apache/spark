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

package org.apache.spark.ml.rdd

import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.rdd.RDDFunctions._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.SparkFunSuite


class RDDFunctionsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("sliceReduce") {
    val data = Seq(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
    val rdd = sc.parallelize(data, data.length)

    val sumArray = rdd.sliceReduce ({ case (arr1, arr2) =>
      arr1.indices.map(i => arr1(i) + arr2(i)).toArray
    }, 2)

    assert(sumArray.canEqual(Array(12, 15, 18)))
  }

  test("sliceAggregate") {
    val data = Seq(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
    val rdd = sc.parallelize(data, data.length)

    def seqOp: (Vector, Array[Int]) => Vector = (c: Vector, x: Array[Int]) => {
      val result = c.copy
      BLAS.axpy(1.0, Vectors.dense(x.map(_.toDouble)), result)
      result
    }
    def combOp: (Vector, Vector) => Vector = (c1: Vector, c2: Vector) => {
      val result = c1.copy
      BLAS.axpy(1.0, c2, result)
      result
    }

    val sumArray = rdd.sliceAggregate(Vectors.zeros(3))(seqOp, combOp, 2)
    assert(sumArray.toArray.canEqual(Array(12, 15, 18)))
  }
}
