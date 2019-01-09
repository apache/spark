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

import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkFunSuite

class JsonMatrixConverterSuite extends SparkFunSuite {

  test("toJson/fromJson") {
    val denseMatrices = Seq(
      Matrices.dense(0, 0, Array.empty[Double]),
      Matrices.dense(1, 1, Array(0.1)),
      new DenseMatrix(3, 2, Array(0.0, 1.21, 2.3, 9.8, 9.0, 0.0), true)
    )

    val sparseMatrices = denseMatrices.map(_.toSparse) ++ Seq(
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(3.1, 3.5))
    )

    for (m <- sparseMatrices ++ denseMatrices) {
      val json = JsonMatrixConverter.toJson(m)
      parse(json) // `json` should be a valid JSON string
      val u = JsonMatrixConverter.fromJson(json)
      assert(u.getClass === m.getClass, "toJson/fromJson should preserve Matrix types.")
      assert(u === m, "toJson/fromJson should preserve Matrix values.")
    }
  }
}
