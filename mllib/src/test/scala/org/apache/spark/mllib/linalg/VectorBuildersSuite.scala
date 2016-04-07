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

import java.util.NoSuchElementException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.util.TestingUtils._

class VectorBuildersSuite extends SparkFunSuite with Logging {
  
  test("dense builder fetch of a value not explicitly stored") {
    val av = new DenseVectorBuilder(10)
    assert(av(5) === 0.0)
  }
  
  test("dense builder fetch of an explicitly stored value") {
    val av = new DenseVectorBuilder(10)
    av.set(5, 4.2)
    assert(av(5) === 4.2)
  }
  
  test("dense builder fetch at negative index") {
    intercept[IndexOutOfBoundsException] {
      val av = new DenseVectorBuilder(10)
      av(-1)
    }
  }
  
  test("dense builder fetch at index exceeding the size") {
    intercept[IndexOutOfBoundsException] {
      val size = 10
      val av = new DenseVectorBuilder(size)
      av(size)
    }
  }
  
  test("dense builder with default value") {
    val va = new DenseVectorBuilder(10, 42)
    assert(va(0) === 42)
  }
  
  test("dense builder last value") {
    val va = new DenseVectorBuilder(3)
    va.set(0, 1.0)
    va.set(1, 2.0)

    assert(va.last === 0.0)
  }
  
  test("dense builder size zero has no last value") {
    intercept[NoSuchElementException] {
      new DenseVectorBuilder(0).last
    }
  }
  
  test("dense builder set value") {
    val av = new DenseVectorBuilder(10)
    assert(av(5) === 0.0)

    av.set(5, 2.0)
    assert(av(5) === 2.0)

    av.set(5, 4.0)
    assert(av(5) === 4.0)
  }
  
  test("dense builder add value") {
    val av = new DenseVectorBuilder(10)
    av.add(5, 4.2)
    av.add(5, -4.2)
    av.add(5, 6.3)
    av.add(5, 2.2)

    assert(av(5) === 8.5)
  }
  
  test("dense builder drop right") {
    val av1 = new DenseVectorBuilder(3)
    av1.set(0, 1.0)
    av1.set(1, 2.0)
    av1.set(2, 4.0)

    val av2 = av1.dropRight(2)

    assert(av2.size === 1)
    assert(av2(0) === 1.0)
  }
  
  test("dense builder drop all") {
    val av = new DenseVectorBuilder(3)
    assert(av.dropRight(4).size === 0)
  }
  
  test("dense builder for each active") {
    val av = new DenseVectorBuilder(5)
    av.add(4, 0.0)
    av.add(2, 4.0)
    av.add(1, 2.0)

    val actual = new ArrayBuffer[(Int, Double)]()
    av.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 0.0), (1, 2.0), (2, 4.0), (3, 0.0), (4, 0.0))
    assert(actual === expected)
  }
  
  test("dense builder map active") {
    val av = new DenseVectorBuilder(3)
    av.set(0, 1.0)
    av.set(1, 2.0)
    av.set(2, 3.0)

    val avUpdated = av.mapActive((i, v) => v * v)

    val actual = new ArrayBuffer[(Int, Double)]()
    avUpdated.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 1.0), (1, 4.0), (2, 9.0))
    assert(actual === expected)
  }
  
  test("dense builder add all") {
    val av1 = new DenseVectorBuilder(4)
    av1.set(2, 1.0)
    av1.set(1, 1.0)

    val av2 = new DenseVectorBuilder(4)
    av2.set(3, 1.0)
    av2.set(2, 1.0)

    av1.addAll(av2)

    val actual = new ArrayBuffer[(Int, Double)]()
    av1.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 0.0), (1, 1.0), (2, 2.0), (3, 1.0))
    assert(actual === expected)
  }
  
  test("dense builder to vector") {
    val av = new DenseVectorBuilder(4)
    av.set(1, 4.0)
    av.set(3, 2.0)

    val vector = av.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 0.0), (1, 4.0), (2, 0.0), (3, 2.0))
    assert(actual === expected)
  }
  
  test("dense builder with default value to vector") {
    val av = new DenseVectorBuilder(4, Double.MaxValue)
    av.set(1, 4.0)
    av.set(3, 2.0)

    val vector = av.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, Double.MaxValue), (1, 4.0), (2, Double.MaxValue), (3, 2.0))
    assert(actual === expected)
  }
  
  test("dense builder from vector") {
    val v = new DenseVector(Array(3.0, 2.0, 1.0))

    val av = VectorBuilders.fromVector(v)

    assert(av.isInstanceOf[DenseVectorBuilder])

    val actual = new ArrayBuffer[(Int, Double)]()
    av.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 3.0), (1, 2.0), (2, 1.0))
    assert(actual === expected)
  }
  
  test("dense builder creation") {
    val av = VectorBuilders.create(3, false, 42)

    assert(av.isInstanceOf[DenseVectorBuilder])
    assert(av(0) === 42.0)
  }
  
  test("dense builder clone") {
    val av = new DenseVectorBuilder(3)
    av.set(0, 2.0)
    av.set(2, 4.0)

    val clone = av.clone()

    assert(clone.isInstanceOf[DenseVectorBuilder])

    val actual = new ArrayBuffer[(Int, Double)]()
    clone.foreachActive((i, v) => actual += ((i, v)))
    val expected = Seq((0, 2.0), (1, 0.0), (2, 4.0))
    assert(actual === expected)
  }
  
  test("sparse builder fetch of a value not explicitly stored") {
    val av = new SparseVectorBuilder(10)

    assert(av(5) === 0.0)
  }
  
  test("sparse builder fetch of an explicitly stored value") {
    val av = new SparseVectorBuilder(10)
    av.add(5, 4.2)

    assert(av(5) === 4.2)
  }
  
  test("sparse builder fetch at negative index") {
    intercept[IndexOutOfBoundsException] {
      val av = new SparseVectorBuilder(10)
      av(-1)
    }
  }
  
  test("sparse builder fetch at index exceeding the size") {
    intercept[IndexOutOfBoundsException] {
      val size = 10
      val av = new SparseVectorBuilder(size)
      av(size)
    }
  }
  
  test("sparse builder with default value") {
    val va = new SparseVectorBuilder(10, 42)
    assert(va(0) === 42)
  }
  
  test("sparse builder last value") {
    val va = new SparseVectorBuilder(3)
    va.set(0, 1.0)
    va.set(1, 2.0)

    assert(va.last === 0.0)
  }
  
  test("sparse builder size zero has no last value") {
    intercept[NoSuchElementException] {
      new SparseVectorBuilder(0).last
    }
  }
  
  test("sparse builder set value") {
    val av = new SparseVectorBuilder(10)
    assert(av(5) === 0.0)

    av.set(5, 2.0)
    assert(av(5) === 2.0)

    av.set(5, 4.0)
    assert(av(5) === 4.0)
  }
  
  test("sparse builder add value") {
    val av = new SparseVectorBuilder(10)
    av.add(5, 4.2)
    av.add(5, -4.2)
    av.add(5, 6.3)
    av.add(5, 2.2)

    assert(av(5) === 8.5)
  }
  
  test("sparse builder drop right") {
    val av1 = new SparseVectorBuilder(3)
    av1.set(0, 1.0)
    av1.set(1, 2.0)
    av1.set(2, 4.0)

    val av2 = av1.dropRight(2)

    assert(av2.size === 1)
    assert(av2(0) === 1.0)
  }
  
  test("sparse builder drop all") {
    val av = new SparseVectorBuilder(3)
    assert(av.dropRight(4).size === 0)
  }
  
  test("sparse builder for each active") {
    val av = new SparseVectorBuilder(5)
    av.set(4, 0.0)
    av.set(2, 4.0)
    av.set(1, 2.0)

    val actual = new ArrayBuffer[(Int, Double)]()
    av.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 2.0), (2, 4.0), (4, 0.0))
    assert(actual.sortBy(_._1) === expected)
  }
  
  test("sparse builder map active") {
    val av = new SparseVectorBuilder(5)
    av.set(0, 1.0)
    av.set(2, 2.0)
    av.set(4, 3.0)

    val avUpdated = av.mapActive((i, v) => v * v)

    val actual = new ArrayBuffer[(Int, Double)]()
    avUpdated.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 1.0), (2, 4.0), (4, 9.0))
    assert(actual.sortBy(_._1) === expected)
  }
  
  test("sparse builder add all") {
    val av1 = new SparseVectorBuilder(5)
    av1.add(2, 1.0)
    av1.add(1, 1.0)

    val av2 = new SparseVectorBuilder(5)
    av2.add(3, 1.0)
    av2.add(2, 1.0)

    av1.addAll(av2)

    val actual = new ArrayBuffer[(Int, Double)]()
    av1.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 1.0), (2, 2.0), (3, 1.0))
    assert(actual.sortBy(_._1) === expected)
  }
  
  test("sparse builder to vector") {
    val av = new SparseVectorBuilder(4)
    av.add(1, 4.0)
    av.add(3, 2.0)

    val vector = av.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 4.0), (3, 2.0))
    assert(actual === expected)
  }
  
  test("sparse builder with default value to vector") {
    val av = new SparseVectorBuilder(4, Double.MaxValue)
    av.set(1, 4.0)
    av.set(3, 2.0)

    val vector = av.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 4.0), (3, 2.0))
    assert(actual === expected)
  }
  
  test("sparse builder from vector") {
    val v = new SparseVector(6, Array(1, 2, 5), Array(3.0, 2.0, 1.0))

    val av = VectorBuilders.fromVector(v)

    assert(av.isInstanceOf[SparseVectorBuilder])

    val actual = new mutable.HashSet[(Int, Double)]()
    av.foreachActive((i, v) => actual += Pair(i, v))

    val expected = Set((1, 3.0), (2, 2.0), (5, 1.0))
    assert(actual === expected)
  }
  
  test("sparse builder creation") {
    val av = new SparseVectorBuilder(3)
    av.add(0, 2.0)
    av.add(2, 4.0)

    val clone = av.clone()

    assert(clone.isInstanceOf[SparseVectorBuilder])

    val actual = new mutable.HashSet[(Int, Double)]()
    clone.foreachActive((i, v) => actual += ((i, v)))
    val expected = Set((0, 2.0), (2, 4.0))
    assert(actual === expected)
  }
  
  test("sparse builder clone") {
    val av = VectorBuilders.create(3, true, 42)

    assert(av.isInstanceOf[SparseVectorBuilder])
    assert(av(0) === 42.0)
  }
}