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
    val builder = new DenseVectorBuilder(10)
    assert(builder(5) === 0.0)
  }

  test("dense builder fetch of an explicitly stored value") {
    val builder = new DenseVectorBuilder(10)
    builder.set(5, 4.2)
    assert(builder(5) === 4.2)
  }

  test("dense builder fetch at negative index") {
    intercept[IndexOutOfBoundsException] {
      val builder = new DenseVectorBuilder(10)
      builder(-1)
    }
  }

  test("dense builder fetch at index exceeding the size") {
    intercept[IndexOutOfBoundsException] {
      val size = 10
      val builder = new DenseVectorBuilder(size)
      builder(size)
    }
  }

  test("dense builder with default value") {
    val builder = new DenseVectorBuilder(10, 42)
    assert(builder(0) === 42)
  }

  test("dense builder last value") {
    val builder = new DenseVectorBuilder(3)
    builder.set(0, 1.0)
    builder.set(1, 2.0)

    assert(builder.last === 0.0)
  }

  test("dense builder size zero has no last value") {
    intercept[NoSuchElementException] {
      new DenseVectorBuilder(0).last
    }
  }

  test("dense builder set value") {
    val builder = new DenseVectorBuilder(10)
    assert(builder(5) === 0.0)

    builder.set(5, 2.0)
    assert(builder(5) === 2.0)

    builder.set(5, 4.0)
    assert(builder(5) === 4.0)
  }

  test("dense builder add value") {
    val builder = new DenseVectorBuilder(10)
    builder.add(5, 4.2)
    builder.add(5, -4.2)
    builder.add(5, 6.3)
    builder.add(5, 2.2)

    assert(builder(5) === 8.5)
  }

  test("dense builder drop right") {
    val builder1 = new DenseVectorBuilder(3)
    builder1.set(0, 1.0)
    builder1.set(1, 2.0)
    builder1.set(2, 4.0)

    val builder2 = builder1.dropRight(2)

    assert(builder2.size === 1)
    assert(builder2(0) === 1.0)
  }

  test("dense builder drop all") {
    val builder = new DenseVectorBuilder(3)
    assert(builder.dropRight(4).size === 0)
  }

  test("dense builder for each active") {
    val builder = new DenseVectorBuilder(5)
    builder.add(4, 0.0)
    builder.add(2, 4.0)
    builder.add(1, 2.0)

    val actual = new ArrayBuffer[(Int, Double)]()
    builder.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 0.0), (1, 2.0), (2, 4.0), (3, 0.0), (4, 0.0))
    assert(actual === expected)
  }

  test("dense builder map active") {
    val builder = new DenseVectorBuilder(3)
    builder.set(0, 1.0)
    builder.set(1, 2.0)
    builder.set(2, 3.0)

    val builderUpdated = builder.mapActive((i, v) => v * v)

    val actual = new ArrayBuffer[(Int, Double)]()
    builderUpdated.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 1.0), (1, 4.0), (2, 9.0))
    assert(actual === expected)
  }

  test("dense builder add all") {
    val builder1 = new DenseVectorBuilder(4)
    builder1.set(2, 1.0)
    builder1.set(1, 1.0)

    val builder2 = new DenseVectorBuilder(4)
    builder2.set(3, 1.0)
    builder2.set(2, 1.0)

    builder1.addAll(builder2)

    val actual = new ArrayBuffer[(Int, Double)]()
    builder1.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 0.0), (1, 1.0), (2, 2.0), (3, 1.0))
    assert(actual === expected)
  }

  test("dense builder to vector") {
    val builder = new DenseVectorBuilder(4)
    builder.set(1, 4.0)
    builder.set(3, 2.0)

    val vector = builder.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 0.0), (1, 4.0), (2, 0.0), (3, 2.0))
    assert(actual === expected)
  }

  test("dense builder with default value to vector") {
    val builder = new DenseVectorBuilder(4, Double.MaxValue)
    builder.set(1, 4.0)
    builder.set(3, 2.0)

    val vector = builder.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, Double.MaxValue), (1, 4.0), (2, Double.MaxValue), (3, 2.0))
    assert(actual === expected)
  }

  test("dense builder from vector") {
    val vector = new DenseVector(Array(3.0, 2.0, 1.0))

    val builder = VectorBuilders.fromVector(vector)

    assert(builder.isInstanceOf[DenseVectorBuilder])

    val actual = new ArrayBuffer[(Int, Double)]()
    builder.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 3.0), (1, 2.0), (2, 1.0))
    assert(actual === expected)
  }

  test("dense builder creation") {
    val builder = VectorBuilders.create(3, false, 42)

    assert(builder.isInstanceOf[DenseVectorBuilder])
    assert(builder(0) === 42.0)
  }

  test("dense builder clone") {
    val builder = new DenseVectorBuilder(3)
    builder.set(0, 2.0)
    builder.set(2, 4.0)

    val clone = builder.clone()

    assert(clone.isInstanceOf[DenseVectorBuilder])

    val actual = new ArrayBuffer[(Int, Double)]()
    clone.foreachActive((i, v) => actual += ((i, v)))
    val expected = Seq((0, 2.0), (1, 0.0), (2, 4.0))
    assert(actual === expected)
  }

  test("dense builder clone with default") {
    val builder = new DenseVectorBuilder(3, 1.0)
    builder.set(0, 2.0)
    builder.set(2, 4.0)

    val clone = builder.clone()

    assert(clone.isInstanceOf[DenseVectorBuilder])

    val actual = new ArrayBuffer[(Int, Double)]()
    clone.foreachActive((i, v) => actual += ((i, v)))
    val expected = Seq((0, 2.0), (1, 1.0), (2, 4.0))
    assert(actual === expected)
  }

  test("sparse builder fetch of a value not explicitly stored") {
    val builder = new SparseVectorBuilder(10)

    assert(builder(5) === 0.0)
  }

  test("sparse builder fetch of an explicitly stored value") {
    val builder = new SparseVectorBuilder(10)
    builder.add(5, 4.2)

    assert(builder(5) === 4.2)
  }

  test("sparse builder fetch at negative index") {
    intercept[IndexOutOfBoundsException] {
      val builder = new SparseVectorBuilder(10)
      builder(-1)
    }
  }

  test("sparse builder fetch at index exceeding the size") {
    intercept[IndexOutOfBoundsException] {
      val size = 10
      val builder = new SparseVectorBuilder(size)
      builder(size)
    }
  }

  test("sparse builder with default value") {
    val builder = new SparseVectorBuilder(10, 42)
    assert(builder(0) === 42)
  }

  test("sparse builder last value") {
    val builder = new SparseVectorBuilder(3)
    builder.set(0, 1.0)
    builder.set(1, 2.0)

    assert(builder.last === 0.0)
  }

  test("sparse builder size zero has no last value") {
    intercept[NoSuchElementException] {
      new SparseVectorBuilder(0).last
    }
  }

  test("sparse builder set value") {
    val builder = new SparseVectorBuilder(10)
    assert(builder(5) === 0.0)

    builder.set(5, 2.0)
    assert(builder(5) === 2.0)

    builder.set(5, 4.0)
    assert(builder(5) === 4.0)
  }

  test("sparse builder add value") {
    val builder = new SparseVectorBuilder(10)
    builder.add(5, 4.2)
    builder.add(5, -4.2)
    builder.add(5, 6.3)
    builder.add(5, 2.2)

    assert(builder(5) === 8.5)
  }

  test("sparse builder drop right") {
    val builder1 = new SparseVectorBuilder(3)
    builder1.set(0, 1.0)
    builder1.set(1, 2.0)
    builder1.set(2, 4.0)

    val builder2 = builder1.dropRight(2)

    assert(builder2.size === 1)
    assert(builder2(0) === 1.0)
  }

  test("sparse builder drop all") {
    val builder = new SparseVectorBuilder(3)
    assert(builder.dropRight(4).size === 0)
  }

  test("sparse builder for each active") {
    val builder = new SparseVectorBuilder(5)
    builder.set(4, 0.0)
    builder.set(2, 4.0)
    builder.set(1, 2.0)

    val actual = new ArrayBuffer[(Int, Double)]()
    builder.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 2.0), (2, 4.0), (4, 0.0))
    assert(actual.sortBy(_._1) === expected)
  }

  test("sparse builder map active") {
    val builder = new SparseVectorBuilder(5)
    builder.set(0, 1.0)
    builder.set(2, 2.0)
    builder.set(4, 3.0)

    val builderUpdated = builder.mapActive((i, v) => v * v)

    val actual = new ArrayBuffer[(Int, Double)]()
    builderUpdated.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((0, 1.0), (2, 4.0), (4, 9.0))
    assert(actual.sortBy(_._1) === expected)
  }

  test("sparse builder add all") {
    val builder1 = new SparseVectorBuilder(5)
    builder1.add(2, 1.0)
    builder1.add(1, 1.0)

    val builder2 = new SparseVectorBuilder(5)
    builder2.add(3, 1.0)
    builder2.add(2, 1.0)

    builder1.addAll(builder2)

    val actual = new ArrayBuffer[(Int, Double)]()
    builder1.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 1.0), (2, 2.0), (3, 1.0))
    assert(actual.sortBy(_._1) === expected)
  }

  test("sparse builder to vector") {
    val builder = new SparseVectorBuilder(4)
    builder.add(1, 4.0)
    builder.add(3, 2.0)

    val vector = builder.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 4.0), (3, 2.0))
    assert(actual === expected)
  }

  test("sparse builder with default value to vector") {
    val builder = new SparseVectorBuilder(4, Double.MaxValue)
    builder.set(1, 4.0)
    builder.set(3, 2.0)

    val vector = builder.toVector

    assert(vector.size === 4)

    val actual = new ArrayBuffer[(Int, Double)]()
    vector.foreachActive((i, v) => actual += ((i, v)))

    val expected = Seq((1, 4.0), (3, 2.0))
    assert(actual === expected)
  }

  test("sparse builder from vector") {
    val vector = new SparseVector(6, Array(1, 2, 5), Array(3.0, 2.0, 1.0))

    val builder = VectorBuilders.fromVector(vector)

    assert(builder.isInstanceOf[SparseVectorBuilder])

    val actual = new mutable.HashSet[(Int, Double)]()
    builder.foreachActive((i, v) => actual += Pair(i, v))

    val expected = Set((1, 3.0), (2, 2.0), (5, 1.0))
    assert(actual === expected)
  }

  test("sparse builder creation") {
    val builder = VectorBuilders.create(3, true, 42)

    assert(builder.isInstanceOf[SparseVectorBuilder])
    assert(builder(0) === 42.0)
  }

  test("sparse builder clone") {
    val builder = new SparseVectorBuilder(3)
    builder.set(0, 2.0)
    builder.set(2, 4.0)

    val clone = builder.clone()

    assert(clone.isInstanceOf[SparseVectorBuilder])

    val actual = new mutable.HashSet[(Int, Double)]()
    clone.foreachActive((i, v) => actual += ((i, v)))
    val expected = Set((0, 2.0), (2, 4.0))
    assert(actual === expected)
  }

  test("sparse builder clone with default") {
    val builder = new SparseVectorBuilder(3, 1.0)
    builder.set(0, 2.0)
    builder.set(2, 4.0)

    val clone = builder.clone()

    assert(clone.isInstanceOf[SparseVectorBuilder])

    val actual = new mutable.HashSet[(Int, Double)]()
    clone.foreachActive((i, v) => actual += ((i, v)))
    val expected = Set((0, 2.0), (2, 4.0))
    assert(actual === expected)
  }
}
