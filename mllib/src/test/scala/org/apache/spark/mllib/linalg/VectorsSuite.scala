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

package org.apache.spark.mllib.linalg

import scala.collection.JavaConverters._

import org.scalatest.FunSuite

class VectorsSuite extends FunSuite {

  val arr = Array(0.1, 0.2, 0.3, 0.4)
  val n = 20
  val indices = Array(0, 3, 5, 10, 13)
  val values = Array(0.1, 0.5, 0.3, -0.8, -1.0)

  test("dense vector construction with varargs") {
    val vec = Vectors.dense(arr: _*).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("dense vector construction from a double array") {
   val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("sparse vector construction") {
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices))
    assert(vec.values.eq(values))
  }

  test("sparse vector construction with unordered elements") {
    val vec = Vectors.sparse(n, indices.zip(values).reverse).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices)
    assert(vec.values === values)
  }

  test("sparse vector construction with unordered elements stored as Java Iterable") {
    val vec = Vectors.sparse(n, indices.toSeq.zip(values).reverse.asJava).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices)
    assert(vec.values === values)
  }

  test("vector equals") {
    val dv1 = Vectors.dense(1.0, 0.0, 2.0)
    val dv2 = Vectors.dense(1.0, 0.0, 2.0)
    val sv1 = Vectors.sparse(3, Seq((0, 1.0), (2, 2.0)))
    val sv2 = Vectors.sparse(3, Seq((0, 1.0), (2, 2.0)))

    val vectors = Seq(dv1, dv2, sv1, sv2)

    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.dense(1.0, 1.0, 2.0)

    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }
}
