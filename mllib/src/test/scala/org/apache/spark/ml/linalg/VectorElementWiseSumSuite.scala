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
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}


class VectorElementWiseSumSuite extends SparkFunSuite with MLlibTestSparkContext {

  val vStruct = StructType(Seq(StructField("v", new VectorUDT, false)))

  test("Sum with DenseVectors") {
    val dv1 = Vectors.dense(0.5, 1.2)
    val dv2 = Vectors.dense(1.0, 2.0)
    val result = Vectors.dense(1.5, 3.2)
    val vectorsRDD = spark.sparkContext.parallelize(Seq(Row(dv1), Row(dv2)))

    val df = spark.createDataFrame(vectorsRDD, vStruct)
    val vSum = new VectorElementWiseSum()
    val actualRes = df.agg(vSum(df("v"))).first.getAs[Vector](0)

    assert(result.equals(actualRes))

  }

  test("Sum with SparseVectors") {
    val sv1 = Vectors.sparse(3, Array(1, 2), Array(3.6, 1.0))
    val sv2 = Vectors.sparse(3, Array(1), Array(2.0))
    val result = Vectors.sparse(3, Array(1, 2), Array(5.6, 1.0))
    val vectorsRDD = spark.sparkContext.parallelize(Seq(Row(sv1), Row(sv2)))

    val df = spark.createDataFrame(vectorsRDD, vStruct)
    val vSum = new VectorElementWiseSum()
    val actualRes = df.agg(vSum(df("v"))).first.getAs[Vector](0)

    assert(result.equals(actualRes))

  }

  test("Sum with negative numbers") {
    val dv1 = Vectors.dense(-0.5, 1.2)
    val dv2 = Vectors.dense(1.0, -2.0)
    val result = Vectors.dense(0.5, -0.8)
    val vectorsRDD = spark.sparkContext.parallelize(Seq(Row(dv1), Row(dv2)))

    val df = spark.createDataFrame(vectorsRDD, vStruct)
    val vSum = new VectorElementWiseSum()
    val actualRes = df.agg(vSum(df("v"))).first.getAs[Vector](0)

    assert(result.equals(actualRes))
  }

}
