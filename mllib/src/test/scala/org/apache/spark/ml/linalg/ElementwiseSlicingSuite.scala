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

package org.apache.spark.ml.linalg

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.rdd.RDDFunctions._
import org.apache.spark.mllib.util.MLlibTestSparkContext


class ElementwiseSlicingSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("array slicing") {
    val data = Seq(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
    val rdd = sc.parallelize(data, data.length)

    val sumArray = rdd.sliceReduce ((arr1, arr2) =>
      arr1.indices.map(i => arr1(i) + arr2(i)).toArray,
      2)

    assert(sumArray.canEqual(Array(12, 15, 18)))
  }

  test("iterable slicing") {
    val data = Seq(Iterable(1, 2, 3), Iterable(4, 5, 6), Iterable(7, 8, 9))
    val rdd = sc.parallelize(data, data.length)
    val sumIterable = rdd.sliceReduce ((arr1, arr2) =>
      arr1.zip(arr2).map { case (elem1, elem2) => elem1 + elem2 },
      2)
    val sumArray = sumIterable.toArray
    assert(sumArray.canEqual(Array(12, 15, 18)))
  }

  test("option slicing") {
    val rowNum = 100
    val dim = 10000
    val optRdd = sc.parallelize(0 until rowNum, rowNum)
      .map { rowId =>
        val arr = (0 until dim).toArray.map(i => 1.0)
        Some(arr)
      }

    val aggSum = optRdd.sliceAggregate(Option.empty[Array[Double]])(
      (op1, op2) => op2, (op1, op2) => op1, 2)
    assert(aggSum.get.sum == dim)
  }

  test("tuple2 slicing") {
    val first = Seq(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
    val second = first.map(arr => arr.toIterable)
    val tuple2Rdd = sc.parallelize(first.zip(second), first.length)

    val sumTuple2 = tuple2Rdd.sliceReduce({ case ((a1, b1), (a2, b2)) =>
      val a = a1.indices.map(i => a1(i) + a2(i)).toArray
      val b = b1.zip(b2).map { case (elem1, elem2) => elem1 + elem2 }
      (a, b)
    }, 2)

    val sumArray1 = sumTuple2._1
    val sumArray2 = sumTuple2._2.toArray

    assert(sumArray1.canEqual(Array(12, 15, 18)))
    assert(sumArray2.canEqual(Array(12, 15, 18)))
  }

  test("tuple3 slicing") {
    val first = Seq(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
    val second = first.map(arr => arr.toIterable)
    val third = first.map(arr => arr ++ arr)

    val firstRdd = sc.parallelize(first, first.length)
    val secondRdd = sc.parallelize(second, second.length)
    val thirdRdd = sc.parallelize(third, third.length)

    val sumTuple3 = firstRdd.zip(secondRdd).zip(thirdRdd).map(x => (x._1._1, x._1._2, x._2))
      .sliceReduce ({ case ((a1, b1, c1), (a2, b2, c2)) =>
        val a = a1.indices.map(i => a1(i) + a2(i)).toArray
        val b = b1.zip(b2).map { case (elem1, elem2) => elem1 + elem2 }
        val c = c1.indices.map(i => c1(i) + c2(i)).toArray
        (a, b, c)
      }, 2)


    val sumArray1 = sumTuple3._1
    val sumArray2 = sumTuple3._2.toArray
    val sumArray3 = sumTuple3._3

    assert(sumArray1.canEqual(Array(12, 15, 18)))
    assert(sumArray2.canEqual(Array(12, 15, 18)))
    assert(sumArray3.canEqual(Array(12, 15, 18)))
  }

  test("DenseVector slice reduce") {
    val dvSeq = Seq(Vectors.dense(1.0, 2.0, 3.0).toDense,
      Vectors.dense(4.0, 5.0, 6.0).toDense, Vectors.dense(7.0, 8.0, 9.0).toDense)

    val rdd = sc.parallelize(dvSeq, dvSeq.length)
    val sumDv = rdd.sliceReduce ((dv1, dv2) => {
      val dv = dv1.copy
      BLAS.axpy(1.0, dv2, dv)
      dv.toDense
    }, 2)
    assert(sumDv.toArray.canEqual(Array(12.0, 15.0, 18.0)))
  }

  test("SparseVector slice reduce") {
    val svSeq = Seq(Vectors.dense(1.0, 2.0, 3.0).toSparse,
      Vectors.dense(4.0, 5.0, 6.0).toSparse, Vectors.dense(7.0, 8.0, 9.0).toSparse)

    val rdd = sc.parallelize(svSeq, svSeq.length)
    val sumSv = rdd.sliceReduce ((sv1, sv2) => {
      val sv = sv1.copy.toDense
      BLAS.axpy(1.0, sv2.toDense, sv)
      sv.toSparse
    }, 2)
    assert(sumSv.toArray.canEqual(Array(12.0, 15.0, 18.0)))
  }

  test("Vector slice reduce") {
    val dvSeq = Seq(Vectors.dense(1.0, 2.0, 3.0),
      Vectors.dense(4.0, 5.0, 6.0), Vectors.dense(7.0, 8.0, 9.0))

    val rdd = sc.parallelize(dvSeq, dvSeq.length)
    val sumDv = rdd.sliceReduce ((dv1, dv2) => {
      val dv = dv1.copy
      BLAS.axpy(1.0, dv2, dv)
      dv.toDense
    }, 2)
    assert(sumDv.toArray.canEqual(Array(12.0, 15.0, 18.0)))
  }

  test("num of slice bigger than array size") {
    val data = Seq(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
    val rdd = sc.parallelize(data, data.length * 2)

    val sumArray = rdd.sliceReduce ((arr1, arr2) =>
      arr1.indices.map(i => arr1(i) + arr2(i)).toArray,
      2)

    assert(sumArray.canEqual(Array(12, 15, 18)))
  }
}