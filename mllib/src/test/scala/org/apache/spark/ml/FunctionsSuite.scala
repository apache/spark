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

package org.apache.spark.ml

import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.MLTest
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}

class FunctionsSuite extends MLTest {

  import testImplicits._

  test("test vector_to_array") {
    val df1 = Seq(
      (Vectors.dense(1.0, 2.0, 3.0), OldVectors.dense(10.0, 20.0, 30.0)),
      (Vectors.sparse(3, Seq((0, 2.0), (2, 3.0))), OldVectors.sparse(3, Seq((0, 20.0), (2, 30.0))))
    ).toDF("vec", "oldVec")

    val result = df1.select(vector_to_array('vec), vector_to_array('oldVec))
      .as[(List[Double], List[Double])]
      .collect()

    val expected = Array(
      (List(1.0, 2.0, 3.0), List(10.0, 20.0, 30.0)),
      (List(2.0, 0.0, 3.0), List(20.0, 0.0, 30.0))
    )
    assert(result === expected)
  }
}
