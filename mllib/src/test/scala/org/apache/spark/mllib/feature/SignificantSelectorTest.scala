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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

class SignificantSelectorTest extends FunSuite with MLlibTestSparkContext {
  val dv = Vectors.dense(1, 2, 3, 4, 5)
  val sv1 = Vectors.sparse(5, Seq((0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0)))
  val sv2 = Vectors.sparse(5, Seq((2, 3.0)))

  test("same result vector") {
    val vectors = sc.parallelize(List(
      Vectors.dense(0.0, 1.0, 2.0, 3.0, 4.0),
      Vectors.dense(4.0, 5.0, 6.0, 7.0, 8.0)
    ))

    val significant = new SignificantSelector().fit(vectors)
    assert(significant.transform(dv).toString == dv.toString)
    assert(significant.transform(sv1).toString == sv1.toString)
    assert(significant.transform(sv2).toString == sv2.toString)
  }
  
  
  test("shortest result vector") {
    val vectors = sc.parallelize(List(
      Vectors.dense(0.0, 2.0, 3.0, 4.0),
      Vectors.dense(0.0, 2.0, 3.0, 4.0),
      Vectors.dense(0.0, 2.0, 3.0, 4.0),
      Vectors.sparse(4, Seq((1, 3.0), (2, 4.0))),
      Vectors.dense(0.0, 3.0, 5.0, 4.0),
      Vectors.dense(0.0, 3.0, 7.0, 4.0)
    ))

    val significant = new SignificantSelector().fit(vectors)
    assert(significant.transform(dv).toString == "[2.0,3.0,4.0]")
    assert(significant.transform(sv1).toString == "(3,[0,1,2],[2.0,3.0,4.0])")
    assert(significant.transform(sv2).toString == "(3,[1],[3.0])")
  }
  
  test("empty result vector") {
    val vectors = sc.parallelize(List(
      Vectors.dense(0.0, 2.0, 3.0, 4.0),
      Vectors.dense(0.0, 2.0, 3.0, 4.0)
    ))

    val significant = new SignificantSelector().fit(vectors)
    assert(significant.transform(dv).toString == "[]")
    assert(significant.transform(sv1).toString == "[]")
    assert(significant.transform(sv2).toString == "[]")
  }
}