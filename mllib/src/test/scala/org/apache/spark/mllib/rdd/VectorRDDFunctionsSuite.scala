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

package org.apache.spark.mllib.rdd

import org.apache.spark.mllib.linalg.Vector
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils._
import VectorRDDFunctionsSuite._
import org.apache.spark.mllib.util.LocalSparkContext

class VectorRDDFunctionsSuite extends FunSuite with LocalSparkContext {

  val localData = Array(
      Vectors.dense(1.0, 2.0, 3.0),
      Vectors.dense(4.0, 5.0, 6.0),
      Vectors.dense(7.0, 8.0, 9.0)
    )

  val rowMeans = Array(2.0, 5.0, 8.0)
  val rowNorm2 = Array(math.sqrt(14.0), math.sqrt(77.0), math.sqrt(194.0))
  val rowSDs = Array(math.sqrt(2.0 / 3.0), math.sqrt(2.0 / 3.0), math.sqrt(2.0 / 3.0))

  val colMeans = Array(4.0, 5.0, 6.0)
  val colNorm2 = Array(math.sqrt(66.0), math.sqrt(93.0), math.sqrt(126.0))
  val colSDs = Array(math.sqrt(6.0), math.sqrt(6.0), math.sqrt(6.0))

  val maxVec = Array(7.0, 8.0, 9.0)
  val minVec = Array(1.0, 2.0, 3.0)

  test("rowMeans") {
    val data = sc.parallelize(localData, 2)
    assert(equivVector(Vectors.dense(data.rowMeans().collect()), Vectors.dense(rowMeans)), "Row means do not match.")
  }

  test("rowNorm2") {
    val data = sc.parallelize(localData, 2)
    assert(equivVector(Vectors.dense(data.rowNorm2().collect()), Vectors.dense(rowNorm2)), "Row norm2s do not match.")
  }

  test("rowSDs") {
    val data = sc.parallelize(localData, 2)
    assert(equivVector(Vectors.dense(data.rowSDs().collect()), Vectors.dense(rowSDs)), "Row SDs do not match.")
  }

  test("colMeans") {
    val data = sc.parallelize(localData, 2)
    assert(equivVector(data.colMeans(), Vectors.dense(colMeans)), "Column means do not match.")
  }

  test("colNorm2") {
    val data = sc.parallelize(localData, 2)
    assert(equivVector(data.colNorm2(), Vectors.dense(colNorm2)), "Column norm2s do not match.")
  }

  test("colSDs") {
    val data = sc.parallelize(localData, 2)
    assert(equivVector(data.colSDs(), Vectors.dense(colSDs)), "Column SDs do not match.")
  }

  test("maxOption") {
    val data = sc.parallelize(localData, 2)
    assert(equivVectorOption(
      data.maxOption((lhs: Vector, rhs: Vector) => lhs.toBreeze.norm(2) >= rhs.toBreeze.norm(2)),
      Some(Vectors.dense(maxVec))),
      "Optional maximum does not match."
    )
  }

  test("minOption") {
    val data = sc.parallelize(localData, 2)
    assert(equivVectorOption(
      data.minOption((lhs: Vector, rhs: Vector) => lhs.toBreeze.norm(2) >= rhs.toBreeze.norm(2)),
      Some(Vectors.dense(minVec))),
      "Optional minimum does not match."
    )
  }
}

object VectorRDDFunctionsSuite {
  def equivVector(lhs: Vector, rhs: Vector): Boolean = {
    (lhs.toBreeze - rhs.toBreeze).norm(2) < 1e-9
  }

  def equivVectorOption(lhs: Option[Vector], rhs: Option[Vector]): Boolean = {
    (lhs, rhs) match {
      case (Some(a), Some(b)) => (a.toBreeze - a.toBreeze).norm(2) < 1e-9
      case (None, None) => true
      case _ => false
    }
  }
}

