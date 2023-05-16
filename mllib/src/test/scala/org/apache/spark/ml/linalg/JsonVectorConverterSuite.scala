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

class JsonVectorConverterSuite extends SparkFunSuite {

  test("toJson/fromJson") {
    val sv0 = Vectors.sparse(0, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(1, Array.empty, Array.empty)
    val sv2 = Vectors.sparse(2, Array(1), Array(2.0))
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0)
    val dv2 = Vectors.dense(0.0, 2.0)
    for (v <- Seq(sv0, sv1, sv2, dv0, dv1, dv2)) {
      val json = JsonVectorConverter.toJson(v)
      parse(json) // `json` should be a valid JSON string
      val u = JsonVectorConverter.fromJson(json)
      assert(u.getClass === v.getClass, "toJson/fromJson should preserve vector types.")
      assert(u === v, "toJson/fromJson should preserve vector values.")
    }
  }
}
