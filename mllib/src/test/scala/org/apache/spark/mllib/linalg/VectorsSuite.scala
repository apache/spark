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

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag
import scala.util.Random

import breeze.linalg.{DenseMatrix => BDM}
import org.json4s.jackson.JsonMethods.{parse => parseJson}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.internal.LogKeys.MALFORMATTED_STIRNG
import org.apache.spark.internal.MDC
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.ml.{linalg => newlinalg}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.ArrayImplicits._

class VectorsSuite extends SparkFunSuite {

  val arr = Array(0.1, 0.0, 0.3, 0.4)
  val n = 4
  val indices = Array(0, 2, 3)
  val values = Array(0.1, 0.3, 0.4)

  test("kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()
    def check[T: ClassTag](t: T): Unit = {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }

    val desVec = Vectors.dense(arr).asInstanceOf[DenseVector]
    val sparVec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    check(desVec)
    check(sparVec)
  }

  test("dense vector construction with varargs") {
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
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
    val vec = Vectors.sparse(n, indices.zip(values).reverse.toImmutableArraySeq)
      .asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices)
    assert(vec.values === values)
  }

  test("sparse vector construction with mismatched indices/values array") {
    intercept[IllegalArgumentException] {
      Vectors.sparse(4, Array(1, 2, 3), Array(3.0, 5.0, 7.0, 9.0))
    }
    intercept[IllegalArgumentException] {
      Vectors.sparse(4, Array(1, 2, 3), Array(3.0, 5.0))
    }
  }

  test("sparse vector construction with too many indices vs size") {
    intercept[IllegalArgumentException] {
      Vectors.sparse(3, Array(1, 2, 3, 4), Array(3.0, 5.0, 7.0, 9.0))
    }
  }

  test("dense to array") {
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.toArray.eq(arr))
  }

  test("dense argmax") {
    val vec = Vectors.dense(Array.empty[Double]).asInstanceOf[DenseVector]
    assert(vec.argmax === -1)

    val vec2 = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec2.argmax === 3)

    val vec3 = Vectors.dense(Array(-1.0, 0.0, -2.0, 1.0)).asInstanceOf[DenseVector]
    assert(vec3.argmax === 3)
  }

  test("sparse to array") {
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.toArray === arr)
  }

  test("sparse argmax") {
    val vec = Vectors.sparse(0, Array.empty[Int], Array.empty[Double]).asInstanceOf[SparseVector]
    assert(vec.argmax === -1)

    val vec2 = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec2.argmax === 3)

    val vec3 = Vectors.sparse(5, Array(2, 3, 4), Array(1.0, 0.0, -.7))
    assert(vec3.argmax === 2)

    // check for case that sparse vector is created with
    // only negative values {0.0, 0.0,-1.0, -0.7, 0.0}
    val vec4 = Vectors.sparse(5, Array(2, 3), Array(-1.0, -.7))
    assert(vec4.argmax === 0)

    val vec5 = Vectors.sparse(11, Array(0, 3, 10), Array(-1.0, -.7, 0.0))
    assert(vec5.argmax === 1)

    val vec6 = Vectors.sparse(11, Array(0, 1, 2), Array(-1.0, -.7, 0.0))
    assert(vec6.argmax === 2)

    val vec7 = Vectors.sparse(5, Array(0, 1, 3), Array(-1.0, 0.0, -.7))
    assert(vec7.argmax === 1)

    val vec8 = Vectors.sparse(5, Array(1, 2), Array(0.0, -1.0))
    assert(vec8.argmax === 0)

    // Check for case when sparse vector is non-empty but the values are empty
    val vec9 = Vectors.sparse(100, Array.empty[Int], Array.empty[Double]).asInstanceOf[SparseVector]
    assert(vec9.argmax === 0)

    val vec10 = Vectors.sparse(1, Array.empty[Int], Array.empty[Double]).asInstanceOf[SparseVector]
    assert(vec10.argmax === 0)
  }

  test("vector equals") {
    val dv1 = Vectors.dense(arr.clone())
    val dv2 = Vectors.dense(arr.clone())
    val sv1 = Vectors.sparse(n, indices.clone(), values.clone())
    val sv2 = Vectors.sparse(n, indices.clone(), values.clone())

    val vectors = Seq(dv1, dv2, sv1, sv2)

    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.dense(0.1, 0.2, 0.3, 0.4)

    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }

  test("vectors equals with explicit 0") {
    val dv1 = Vectors.dense(Array(0, 0.9, 0, 0.8, 0))
    val sv1 = Vectors.sparse(5, Array(1, 3), Array(0.9, 0.8))
    val sv2 = Vectors.sparse(5, Array(0, 1, 2, 3, 4), Array(0, 0.9, 0, 0.8, 0))

    val vectors = Seq(dv1, sv1, sv2)
    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.sparse(5, Array(0, 1, 3), Array(0, 0.9, 0.2))
    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }

  test("indexing dense vectors") {
    val vec = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    assert(vec(0) === 1.0)
    assert(vec(3) === 4.0)
  }

  test("indexing sparse vectors") {
    val vec = Vectors.sparse(7, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec(0) === 1.0)
    assert(vec(1) === 0.0)
    assert(vec(2) === 2.0)
    assert(vec(3) === 0.0)
    assert(vec(6) === 4.0)
    val vec2 = Vectors.sparse(8, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec2(6) === 4.0)
    assert(vec2(7) === 0.0)
  }

  test("parse vectors") {
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(1.0),
      Vectors.dense(1.0E6, 0.0, -2.0e-7),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array(0), Array(1.0)),
      Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0)))
    vectors.foreach { v =>
      val v1 = Vectors.parse(v.toString)
      assert(v.getClass === v1.getClass)
      assert(v === v1)
    }

    val malformatted = Seq("1", "[1,,]", "[1,2b]", "(1,[1,2])", "([1],[2.0,1.0])")
    malformatted.foreach { s =>
      intercept[SparkException] {
        Vectors.parse(s)
        logInfo(log"Didn't detect malformatted string ${MDC(MALFORMATTED_STIRNG, s)}.")
      }
    }
  }

  test("zeros") {
    assert(Vectors.zeros(3) === Vectors.dense(0.0, 0.0, 0.0))
  }

  test("Vector.copy") {
    val sv = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0))
    val svCopy = sv.copy
    (sv, svCopy) match {
      case (sv: SparseVector, svCopy: SparseVector) =>
        assert(sv.size === svCopy.size)
        assert(sv.indices === svCopy.indices)
        assert(sv.values === svCopy.values)
        assert(!sv.indices.eq(svCopy.indices))
        assert(!sv.values.eq(svCopy.values))
      case _ =>
        throw new RuntimeException(s"copy returned ${svCopy.getClass} on ${sv.getClass}.")
    }

    val dv = Vectors.dense(1.0, 0.0, 2.0)
    val dvCopy = dv.copy
    (dv, dvCopy) match {
      case (dv: DenseVector, dvCopy: DenseVector) =>
        assert(dv.size === dvCopy.size)
        assert(dv.values === dvCopy.values)
        assert(!dv.values.eq(dvCopy.values))
      case _ =>
        throw new RuntimeException(s"copy returned ${dvCopy.getClass} on ${dv.getClass}.")
    }
  }

  test("VectorUDT") {
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0, 2.0)
    val sv0 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(2, Array(1), Array(2.0))
    val udt = new VectorUDT()
    for (v <- Seq(dv0, dv1, sv0, sv1)) {
      assert(v === udt.deserialize(udt.serialize(v)))
    }
    assert(udt.typeName == "vector")
    assert(udt.simpleString == "vector")
  }

  test("fromBreeze") {
    val x = BDM.zeros[Double](10, 10)
    val v = Vectors.fromBreeze(x(::, 0))
    assert(v.size === x.rows)
  }

  test("sqdist") {
    val random = new Random()
    for (m <- 1 until 1000 by 100) {
      val nnz = random.nextInt(m)

      val indices1 = random.shuffle(0 to m - 1).slice(0, nnz).sorted.toArray
      val values1 = Array.fill(nnz)(random.nextDouble())
      val sparseVector1 = Vectors.sparse(m, indices1, values1)

      val indices2 = random.shuffle(0 to m - 1).slice(0, nnz).sorted.toArray
      val values2 = Array.fill(nnz)(random.nextDouble())
      val sparseVector2 = Vectors.sparse(m, indices2, values2)

      val denseVector1 = Vectors.dense(sparseVector1.toArray)
      val denseVector2 = Vectors.dense(sparseVector2.toArray)

      val squaredDist = sparseVector1.toArray.zip(sparseVector2.toArray).map {
        case (a, b) => (a - b) * (a - b)
      }.sum

      // SparseVector vs. SparseVector
      assert(Vectors.sqdist(sparseVector1, sparseVector2) ~== squaredDist relTol 1E-8)
      // DenseVector  vs. SparseVector
      assert(Vectors.sqdist(denseVector1, sparseVector2) ~== squaredDist relTol 1E-8)
      // DenseVector  vs. DenseVector
      assert(Vectors.sqdist(denseVector1, denseVector2) ~== squaredDist relTol 1E-8)
    }
  }

  test("foreach") {
    val dv = Vectors.dense(0.0, 1.2, 3.1, 0.0)
    val sv = Vectors.sparse(4, Seq((1, 1.2), (2, 3.1), (3, 0.0)))

    val dvMap = scala.collection.mutable.Map[Int, Double]()
    dv.foreach { (index, value) => dvMap.put(index, value) }
    assert(dvMap === Map(0 -> 0.0, 1 -> 1.2, 2 -> 3.1, 3 -> 0.0))

    val svMap = scala.collection.mutable.Map[Int, Double]()
    sv.foreach { (index, value) => svMap.put(index, value) }
    assert(svMap === Map(0 -> 0.0, 1 -> 1.2, 2 -> 3.1, 3 -> 0.0))
  }

  test("foreachActive") {
    val dv = Vectors.dense(0.0, 1.2, 3.1, 0.0)
    val sv = Vectors.sparse(4, Seq((1, 1.2), (2, 3.1), (3, 0.0)))

    val dvMap = scala.collection.mutable.Map[Int, Double]()
    dv.foreachActive { (index, value) => dvMap.put(index, value) }
    assert(dvMap === Map(0 -> 0.0, 1 -> 1.2, 2 -> 3.1, 3 -> 0.0))

    val svMap = scala.collection.mutable.Map[Int, Double]()
    sv.foreachActive { (index, value) => svMap.put(index, value) }
    assert(svMap === Map(1 -> 1.2, 2 -> 3.1, 3 -> 0.0))
  }

  test("foreachNonZero") {
    val dv = Vectors.dense(0.0, 1.2, 3.1, 0.0)
    val sv = Vectors.sparse(4, Seq((1, 1.2), (2, 3.1), (3, 0.0)))

    val dvMap = scala.collection.mutable.Map[Int, Double]()
    dv.foreachNonZero { (index, value) => dvMap.put(index, value) }
    assert(dvMap === Map(1 -> 1.2, 2 -> 3.1))

    val svMap = scala.collection.mutable.Map[Int, Double]()
    sv.foreachNonZero { (index, value) => svMap.put(index, value) }
    assert(dvMap === Map(1 -> 1.2, 2 -> 3.1))
  }

  test("vector p-norm") {
    val dv = Vectors.dense(0.0, -1.2, 3.1, 0.0, -4.5, 1.9)
    val sv = Vectors.sparse(6, Seq((1, -1.2), (2, 3.1), (3, 0.0), (4, -4.5), (5, 1.9)))

    assert(Vectors.norm(dv, 1.0) ~== dv.toArray.foldLeft(0.0)((a, v) =>
      a + math.abs(v)) relTol 1E-8)
    assert(Vectors.norm(sv, 1.0) ~== sv.toArray.foldLeft(0.0)((a, v) =>
      a + math.abs(v)) relTol 1E-8)

    assert(Vectors.norm(dv, 2.0) ~== math.sqrt(dv.toArray.foldLeft(0.0)((a, v) =>
      a + v * v)) relTol 1E-8)
    assert(Vectors.norm(sv, 2.0) ~== math.sqrt(sv.toArray.foldLeft(0.0)((a, v) =>
      a + v * v)) relTol 1E-8)

    assert(Vectors.norm(dv, Double.PositiveInfinity) ~== dv.toArray.map(math.abs).max relTol 1E-8)
    assert(Vectors.norm(sv, Double.PositiveInfinity) ~== sv.toArray.map(math.abs).max relTol 1E-8)

    assert(Vectors.norm(dv, 3.7) ~== math.pow(dv.toArray.foldLeft(0.0)((a, v) =>
      a + math.pow(math.abs(v), 3.7)), 1.0 / 3.7) relTol 1E-8)
    assert(Vectors.norm(sv, 3.7) ~== math.pow(sv.toArray.foldLeft(0.0)((a, v) =>
      a + math.pow(math.abs(v), 3.7)), 1.0 / 3.7) relTol 1E-8)
  }

  test("Vector numActive and numNonzeros") {
    val dv = Vectors.dense(0.0, 2.0, 3.0, 0.0)
    assert(dv.numActives === 4)
    assert(dv.numNonzeros === 2)

    val sv = Vectors.sparse(4, Array(0, 1, 2), Array(0.0, 2.0, 3.0))
    assert(sv.numActives === 3)
    assert(sv.numNonzeros === 2)
  }

  test("Vector toSparse and toDense") {
    val dv0 = Vectors.dense(0.0, 2.0, 3.0, 0.0)
    assert(dv0.toDense === dv0)
    val dv0s = dv0.toSparse
    assert(dv0s.numActives === 2)
    assert(dv0s === dv0)

    assert(dv0.toSparseWithSize(dv0.numNonzeros) === dv0)
    val dv0s2 = dv0.toSparseWithSize(dv0.numNonzeros)
    assert(dv0s2.numActives === 2)
    assert(dv0s2 === dv0s)

    val sv0 = Vectors.sparse(4, Array(0, 1, 2), Array(0.0, 2.0, 3.0))
    assert(sv0.toDense === sv0)
    val sv0s = sv0.toSparse
    assert(sv0s.numActives === 2)
    assert(sv0s === sv0)

    assert(sv0.toSparseWithSize(sv0.numNonzeros) === sv0)
    val sv0s2 = sv0.toSparseWithSize(sv0.numNonzeros)
    assert(sv0s2.numActives === 2)
    assert(sv0s2 === sv0s)
  }

  test("Vector.compressed") {
    val dv0 = Vectors.dense(1.0, 2.0, 3.0, 0.0)
    val dv0c = dv0.compressed.asInstanceOf[DenseVector]
    assert(dv0c === dv0)

    val dv1 = Vectors.dense(0.0, 2.0, 0.0, 0.0)
    val dv1c = dv1.compressed.asInstanceOf[SparseVector]
    assert(dv1 === dv1c)
    assert(dv1c.numActives === 1)

    val sv0 = Vectors.sparse(4, Array(1, 2), Array(2.0, 0.0))
    val sv0c = sv0.compressed.asInstanceOf[SparseVector]
    assert(sv0 === sv0c)
    assert(sv0c.numActives === 1)

    val sv1 = Vectors.sparse(4, Array(0, 1, 2), Array(1.0, 2.0, 3.0))
    val sv1c = sv1.compressed.asInstanceOf[DenseVector]
    assert(sv1 === sv1c)
  }

  test("SparseVector.slice") {
    val v = new SparseVector(5, Array(1, 2, 4), Array(1.1, 2.2, 4.4))
    assert(v.slice(Array(0, 2)) === new SparseVector(2, Array(1), Array(2.2)))
    assert(v.slice(Array(2, 0)) === new SparseVector(2, Array(0), Array(2.2)))
    assert(v.slice(Array(2, 0, 3, 4)) === new SparseVector(4, Array(0, 3), Array(2.2, 4.4)))
  }

  test("toJson/fromJson") {
    val sv0 = Vectors.sparse(0, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(1, Array.empty, Array.empty)
    val sv2 = Vectors.sparse(2, Array(1), Array(2.0))
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0)
    val dv2 = Vectors.dense(0.0, 2.0)
    for (v <- Seq(sv0, sv1, sv2, dv0, dv1, dv2)) {
      val json = v.toJson
      parseJson(json) // `json` should be a valid JSON string
      val u = Vectors.fromJson(json)
      assert(u.getClass === v.getClass, "toJson/fromJson should preserve vector types.")
      assert(u === v, "toJson/fromJson should preserve vector values.")
    }
  }

  test("conversions between new local linalg and mllib linalg") {
    val dv: DenseVector = new DenseVector(Array(1.0, 2.0, 3.5))
    val sv: SparseVector = new SparseVector(5, Array(1, 2, 4), Array(1.1, 2.2, 4.4))
    val sv0: Vector = sv.asInstanceOf[Vector]
    val dv0: Vector = dv.asInstanceOf[Vector]

    val newSV: newlinalg.SparseVector = sv.asML
    val newDV: newlinalg.DenseVector = dv.asML
    val newSV0: newlinalg.Vector = sv0.asML
    val newDV0: newlinalg.Vector = dv0.asML
    assert(newSV0.isInstanceOf[newlinalg.SparseVector])
    assert(newDV0.isInstanceOf[newlinalg.DenseVector])
    assert(sv.toArray === newSV.toArray)
    assert(dv.toArray === newDV.toArray)
    assert(sv0.toArray === newSV0.toArray)
    assert(dv0.toArray === newDV0.toArray)

    val oldSV: SparseVector = SparseVector.fromML(newSV)
    val oldDV: DenseVector = DenseVector.fromML(newDV)
    val oldSV0: Vector = Vectors.fromML(newSV0)
    val oldDV0: Vector = Vectors.fromML(newDV0)
    assert(oldSV0.isInstanceOf[SparseVector])
    assert(oldDV0.isInstanceOf[DenseVector])
    assert(oldSV.toArray === newSV.toArray)
    assert(oldDV.toArray === newDV.toArray)
    assert(oldSV0.toArray === newSV0.toArray)
    assert(oldDV0.toArray === newDV0.toArray)
  }

  test("implicit conversions between new local linalg and mllib linalg") {

    def mllibVectorToArray(v: Vector): Array[Double] = v.toArray

    def mllibDenseVectorToArray(v: DenseVector): Array[Double] = v.toArray

    def mllibSparseVectorToArray(v: SparseVector): Array[Double] = v.toArray

    def mlVectorToArray(v: newlinalg.Vector): Array[Double] = v.toArray

    def mlDenseVectorToArray(v: newlinalg.DenseVector): Array[Double] = v.toArray

    def mlSparseVectorToArray(v: newlinalg.SparseVector): Array[Double] = v.toArray

    val dv: DenseVector = new DenseVector(Array(1.0, 2.0, 3.5))
    val sv: SparseVector = new SparseVector(5, Array(1, 2, 4), Array(1.1, 2.2, 4.4))
    val sv0: Vector = sv.asInstanceOf[Vector]
    val dv0: Vector = dv.asInstanceOf[Vector]

    val newSV: newlinalg.SparseVector = sv.asML
    val newDV: newlinalg.DenseVector = dv.asML
    val newSV0: newlinalg.Vector = sv0.asML
    val newDV0: newlinalg.Vector = dv0.asML

    import org.apache.spark.mllib.linalg.VectorImplicits._

    assert(mllibVectorToArray(dv0) === mllibVectorToArray(newDV0))
    assert(mllibVectorToArray(sv0) === mllibVectorToArray(newSV0))

    assert(mllibDenseVectorToArray(dv) === mllibDenseVectorToArray(newDV))
    assert(mllibSparseVectorToArray(sv) === mllibSparseVectorToArray(newSV))

    assert(mlVectorToArray(dv0) === mlVectorToArray(newDV0))
    assert(mlVectorToArray(sv0) === mlVectorToArray(newSV0))

    assert(mlDenseVectorToArray(dv) === mlDenseVectorToArray(newDV))
    assert(mlSparseVectorToArray(sv) === mlSparseVectorToArray(newSV))
  }

  test("sparse vector only support non-negative length") {
    val v1 = Vectors.sparse(0, Array.emptyIntArray, Array.emptyDoubleArray)
    val v2 = Vectors.sparse(0, Array.empty[(Int, Double)].toImmutableArraySeq)
    assert(v1.size === 0)
    assert(v2.size === 0)

    intercept[IllegalArgumentException] {
      Vectors.sparse(-1, Array(1), Array(2.0))
    }
    intercept[IllegalArgumentException] {
      Vectors.sparse(-1, Array((1, 2.0)).toImmutableArraySeq)
    }
  }

  test("dot product only supports vectors of same size") {
    val vSize4 = Vectors.dense(arr)
    val vSize1 = Vectors.zeros(1)
    intercept[IllegalArgumentException]{ vSize1.dot(vSize4) }
  }

  test("dense vector dot product") {
    val dv = Vectors.dense(arr)
    assert(dv.dot(dv) === 0.26)
  }

  test("sparse vector dot product") {
    val sv = Vectors.sparse(n, indices, values)
    assert(sv.dot(sv) === 0.26)
  }

  test("mixed sparse and dense vector dot product") {
    val sv = Vectors.sparse(n, indices, values)
    val dv = Vectors.dense(arr)
    assert(sv.dot(dv) === 0.26)
    assert(dv.dot(sv) === 0.26)
  }

  test("iterator") {
    Seq(
      Vectors.dense(arr),
      Vectors.zeros(n),
      Vectors.sparse(n, indices, values),
      Vectors.sparse(n, Array.emptyIntArray, Array.emptyDoubleArray)
    ).foreach { vec =>
      val (indices, values) = vec.iterator.toArray.unzip
      assert(Array.range(0, vec.size) === indices)
      assert(vec.toArray === values)
    }
  }

  test("activeIterator") {
    Seq(
      Vectors.dense(arr),
      Vectors.zeros(n),
      Vectors.sparse(n, indices, values),
      Vectors.sparse(n, Array.emptyIntArray, Array.emptyDoubleArray)
    ).foreach { vec =>
      val indicesBuilder = ArrayBuilder.make[Int]
      val valuesBuilder = ArrayBuilder.make[Double]
      vec.foreachActive { case (i, v) =>
        indicesBuilder += i
        valuesBuilder += v
      }
      val (indices, values) = vec.activeIterator.toArray.unzip
      assert(indicesBuilder.result() === indices)
      assert(valuesBuilder.result() === values)
    }
  }

  test("nonZeroIterator") {
    Seq(
      Vectors.dense(arr),
      Vectors.zeros(n),
      Vectors.sparse(n, indices, values),
      Vectors.sparse(n, Array.emptyIntArray, Array.emptyDoubleArray)
    ).foreach { vec =>
      val indicesBuilder = ArrayBuilder.make[Int]
      val valuesBuilder = ArrayBuilder.make[Double]
      vec.foreachActive { case (i, v) =>
        if (v != 0) {
          indicesBuilder += i
          valuesBuilder += v
        }
      }
      val (indices, values) = vec.nonZeroIterator.toArray.unzip
      assert(indicesBuilder.result() === indices)
      assert(valuesBuilder.result() === values)
    }
  }
}
