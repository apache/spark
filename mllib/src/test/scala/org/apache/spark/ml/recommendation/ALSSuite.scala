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

package org.apache.spark.ml.recommendation

import java.util.Random

import org.scalatest.FunSuite

import org.apache.spark.ml.recommendation.ALS._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

import scala.collection.mutable.ArrayBuffer

class ALSSuite extends FunSuite with MLlibTestSparkContext {

  test("LocalIndexEncoder") {
    val random = new Random
    for (numBlocks <- Seq(1, 2, 5, 10, 20, 50, 100)) {
      val encoder = new LocalIndexEncoder(numBlocks)
      val maxLocalIndex = Int.MaxValue / numBlocks
      val tests = Seq.fill(5)((random.nextInt(numBlocks), random.nextInt(maxLocalIndex))) ++
        Seq((0, 0), (numBlocks - 1, maxLocalIndex))
      tests.foreach { case (blockId, localIndex) =>
        val err = s"Failed with numBlocks=$numBlocks, blockId=$blockId, and localIndex=$localIndex."
        val encoded = encoder.encode(blockId, localIndex)
        assert(encoder.blockId(encoded) === blockId, err)
        assert(encoder.localIndex(encoded) === localIndex, err)
      }
    }
  }

  test("normal equation construction with explict feedback") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 3.0f)
      .add(Array(4.0f, 5.0f), 6.0f)
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    assert(ne0.n === 2)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5")
    // b = np.matrix("3; 6")
    // ata = A.transpose() * A
    // atb = A.transpose() * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(17.0, 22.0, 29.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(27.0, 36.0) relTol 1e-8)

    val ne1 = new NormalEquation(2)
      .add(Array(7.0f, 8.0f), 9.0f)
    ne0.merge(ne1)
    assert(ne0.n === 3)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5; 7 8")
    // b = np.matrix("3; 6; 9")
    // ata = A.transpose() * A
    // atb = A.transpose() * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(66.0, 78.0, 93.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(90.0, 108.0) relTol 1e-8)

    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f), 2.0f)
    }
    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f, 2.0f, 3.0f), 4.0f)
    }
    intercept[IllegalArgumentException] {
      val ne2 = new NormalEquation(3)
      ne0.merge(ne2)
    }

    ne0.reset()
    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))
  }

  test("normal equation construction with implicit feedback") {
    val k = 2
    val alpha = 0.5
    val ne0 = new NormalEquation(k)
      .addImplicit(Array(-5.0f, -4.0f), -3.0f, alpha)
      .addImplicit(Array(-2.0f, -1.0f), 0.0f, alpha)
      .addImplicit(Array(1.0f, 2.0f), 3.0f, alpha)
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    assert(ne0.n === 0) // addImplicit doesn't increase the count.
    // NumPy code that computes the expected values:
    // alpha = 0.5
    // A = np.matrix("-5 -4; -2 -1; 1 2")
    // b = np.matrix("-3; 0; 3")
    // b1 = b > 0
    // c = 1.0 + alpha * np.abs(b)
    // C = np.diag(c.A1)
    // I = np.eye(3)
    // ata = A.transpose() * (C - I) * A
    // atb = A.transpose() * C * b1
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(39.0, 33.0, 30.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(2.5, 5.0) relTol 1e-8)
  }

  test("CholeskySolver") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 4.0f)
      .add(Array(1.0f, 3.0f), 9.0f)
      .add(Array(1.0f, 4.0f), 16.0f)
    val ne1 = new NormalEquation(k)
      .merge(ne0)

    val chol = new CholeskySolver
    val x0 = chol.solve(ne0, 0.0).map(_.toDouble)
    // NumPy code that computes the expected solution:
    // A = np.matrix("1 2; 1 3; 1 4")
    // b = b = np.matrix("3; 6")
    // x0 = np.linalg.lstsq(A, b)[0]
    assert(Vectors.dense(x0) ~== Vectors.dense(-8.333333, 6.0) relTol 1e-6)

    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))

    val x1 = chol.solve(ne1, 0.5).map(_.toDouble)
    // NumPy code that computes the expected solution, where lambda is scaled by n:
    // x0 = np.linalg.solve(A.transpose() * A + 0.5 * 3 * np.eye(2), A.transpose() * b)
    assert(Vectors.dense(x1) ~== Vectors.dense(-0.1155556, 3.28) relTol 1e-6)
  }

  test("RatingBlockBuilder") {
    val emptyBuilder = new RatingBlockBuilder()
    assert(emptyBuilder.size === 0)
    val emptyBlock = emptyBuilder.build()
    assert(emptyBlock.srcIds.isEmpty)
    assert(emptyBlock.dstIds.isEmpty)
    assert(emptyBlock.ratings.isEmpty)

    val builder0 = new RatingBlockBuilder()
      .add(Rating(0, 1, 2.0f))
      .add(Rating(3, 4, 5.0f))
    assert(builder0.size === 2)
    val builder1 = new RatingBlockBuilder()
      .add(Rating(6, 7, 8.0f))
      .merge(builder0.build())
    assert(builder1.size === 3)
    val block = builder1.build()
    val ratings = Seq.tabulate(block.size) { i =>
      (block.srcIds(i), block.dstIds(i), block.ratings(i))
    }.toSet
    assert(ratings === Set((0, 1, 2.0f), (3, 4, 5.0f), (6, 7, 8.0f)))
  }

  test("UncompressedInBlock") {
    val encoder = new LocalIndexEncoder(10)
    val uncompressed = new UncompressedInBlockBuilder(encoder)
      .add(0, Array(1, 0, 2), Array(0, 1, 4), Array(1.0f, 2.0f, 3.0f))
      .add(1, Array(3, 0), Array(2, 5), Array(4.0f, 5.0f))
      .build()
    assert(uncompressed.size === 5)
    val records = Seq.tabulate(uncompressed.size) { i =>
      val dstEncodedIndex = uncompressed.dstEncodedIndices(i)
      val dstBlockId = encoder.blockId(dstEncodedIndex)
      val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
      (uncompressed.srcIds(i), dstBlockId, dstLocalIndex, uncompressed.ratings(i))
    }.toSet
    val expected =
      Set((1, 0, 0, 1.0f), (0, 0, 1, 2.0f), (2, 0, 4, 3.0f), (3, 1, 2, 4.0f), (0, 1, 5, 5.0f))
    assert(records === expected)

    val compressed = uncompressed.compress()
    assert(compressed.size === 5)
    assert(compressed.srcIds.toSeq === Seq(0, 1, 2, 3))
    assert(compressed.dstPtrs.toSeq === Seq(0, 2, 3, 4, 5))
    var decompressed = ArrayBuffer.empty[(Int, Int, Int, Float)]
    var i = 0
    while (i < compressed.srcIds.size) {
      var j = compressed.dstPtrs(i)
      while (j < compressed.dstPtrs(i + 1)) {
        val dstEncodedIndex = compressed.dstEncodedIndices(j)
        val dstBlockId = encoder.blockId(dstEncodedIndex)
        val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
        decompressed += ((compressed.srcIds(i), dstBlockId, dstLocalIndex, compressed.ratings(j)))
        j += 1
      }
      i += 1
    }
    assert(decompressed.toSet === expected)
  }
}
