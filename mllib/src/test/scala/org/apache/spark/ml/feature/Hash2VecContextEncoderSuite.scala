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

package org.apache.spark.ml.feature

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

/**
 * Tests of the per-sequence context encoding of [[Hash2Vec]], without a SparkSession. Tokens are
 * Ints, and the hashes are given explicitly: by default token t has index hash t and sign hash 1,
 * so its context entry is at index t with sign +1.
 */
class Hash2VecContextEncoderSuite extends SparkFunSuite {

  private val vectorSize = 8

  private val decayFunctions = Seq("constant", "gaussian", "exponential", "linear", "harmonic")

  private def constant(windowSize: Int): Array[Double] =
    Hash2Vec.decayWeights("constant", 1.0, windowSize)

  private def decode(packed: Array[Byte]): Map[Int, Double] = {
    val buffer = ByteBuffer.wrap(packed)
    val entries = mutable.ArrayBuffer.empty[(Int, Double)]
    while (buffer.hasRemaining) {
      entries += buffer.getInt() -> buffer.getDouble()
    }
    assert(entries.map(_._1).distinct.size === entries.size, "an index is packed twice")
    entries.toMap
  }

  /** Encodes one sequence and returns the context of each token, in order of first occurrence. */
  private def encode(
      tokens: Seq[Int],
      weights: Array[Double],
      indexHash: Int => Int = identity,
      signHash: Int => Int = _ => 1,
      encoder: Option[Hash2Vec.ContextEncoder] = None): Seq[(Int, Map[Int, Double])] = {
    val enc = encoder.getOrElse(new Hash2Vec.ContextEncoder(vectorSize, weights))
    enc.reset(tokens.length)
    val lastPositions = mutable.Map.empty[Int, Int]
    tokens.zipWithIndex.foreach { case (token, i) =>
      enc.add(i, indexHash(token), signHash(token), lastPositions.getOrElse(token, -1))
      lastPositions(token) = i
    }
    (0 until enc.numHeads).map { h =>
      val first = enc.head(h)
      tokens(first) -> decode(enc.encode(first))
    }
  }

  /** A direct computation of the same contexts, used as the reference. */
  private def reference(
      tokens: Seq[Int],
      weights: Array[Double],
      indexHash: Int => Int = identity,
      signHash: Int => Int = _ => 1): Seq[(Int, Map[Int, Double])] = {
    val windowSize = weights.length - 1
    tokens.distinct.map { token =>
      val sums = mutable.Map.empty[Int, Double].withDefaultValue(0.0)
      for {
        i <- tokens.indices if tokens(i) == token
        c <- math.max(0, i - windowSize) to math.min(tokens.length - 1, i + windowSize) if c != i
      } {
        val index = Utils.nonNegativeMod(indexHash(tokens(c)), vectorSize)
        val sign = if ((signHash(tokens(c)) & 1) == 1) 1.0 else -1.0
        sums(index) += sign * weights(math.abs(c - i))
      }
      token -> sums.filter(_._2 != 0.0).toMap
    }
  }

  test("decay weights") {
    def weights(name: String, scale: Double = 2.0, windowSize: Int = 4): Seq[Double] =
      Hash2Vec.decayWeights(name, scale, windowSize).toSeq.drop(1)

    assert(weights("constant") === Seq(1.0, 1.0, 1.0, 1.0))
    assert(weights("linear") === Seq(1.0, 0.75, 0.5, 0.25))
    assert(weights("harmonic") === Seq(1.0, 0.5, 1.0 / 3, 0.25))
    assert(weights("exponential") === Seq(1, 2, 3, 4).map(d => math.exp(-d / 2.0)))
    assert(weights("gaussian") === Seq(1, 2, 3, 4).map(d => math.exp(-(d * d) / 8.0)))

    decayFunctions.foreach { name =>
      val all = Hash2Vec.decayWeights(name, 2.0, 4)
      assert(all.length === 5)
      assert(all(0) === 0.0, s"$name: distance 0 must be unused")
      assert(weights(name, windowSize = 1).size === 1)
    }
    assert(weights("linear", windowSize = 1) === Seq(1.0))
  }

  test("decay scale only affects gaussian and exponential") {
    Seq("gaussian", "exponential").foreach { name =>
      val narrow = Hash2Vec.decayWeights(name, 1.0, 4)
      val wide = Hash2Vec.decayWeights(name, 3.0, 4)
      (1 to 4).foreach(d => assert(wide(d) > narrow(d), s"$name at distance $d"))
    }
    Seq("constant", "linear", "harmonic").foreach { name =>
      assert(Hash2Vec.decayWeights(name, 1.0, 4) === Hash2Vec.decayWeights(name, 3.0, 4))
    }
  }

  test("empty sequence has no tokens") {
    assert(encode(Seq.empty, constant(2)) === Seq.empty)
  }

  test("single token has an empty context") {
    assert(encode(Seq(3), constant(2)) === Seq(3 -> Map.empty[Int, Double]))
  }

  test("repeats of one token are each other's context") {
    assert(encode(Seq(3, 3), constant(2)) === Seq(3 -> Map(3 -> 2.0)))
    // Linear weights for window 2 are 1.0 and 0.5. Position 0 sees 1.0 + 0.5, position 1 sees
    // 1.0 + 1.0 and position 2 sees 0.5 + 1.0.
    val linear = Hash2Vec.decayWeights("linear", 1.0, 2)
    assert(encode(Seq(3, 3, 3), linear) === Seq(3 -> Map(3 -> 5.0)))
  }

  test("two tokens") {
    val signs = (t: Int) => t // odd tokens are positive, even tokens are negative
    assert(
      encode(Seq(1, 2), constant(2), signHash = signs) ===
        Seq(1 -> Map(2 -> -1.0), 2 -> Map(1 -> 1.0)))
  }

  test("context is limited by the window and the sequence edges") {
    assert(
      encode(Seq(1, 2, 3, 4), constant(1)) === Seq(
        1 -> Map(2 -> 1.0),
        2 -> Map(1 -> 1.0, 3 -> 1.0),
        3 -> Map(2 -> 1.0, 4 -> 1.0),
        4 -> Map(3 -> 1.0)))
    // A window longer than the sequence covers the whole sequence.
    assert(
      encode(Seq(1, 2, 3), constant(10)) === Seq(
        1 -> Map(2 -> 1.0, 3 -> 1.0),
        2 -> Map(1 -> 1.0, 3 -> 1.0),
        3 -> Map(1 -> 1.0, 2 -> 1.0)))
  }

  test("tokens are returned in order of first occurrence") {
    assert(encode(Seq(5, 1, 5, 2, 1), constant(1)).map(_._1) === Seq(5, 1, 2))
  }

  test("colliding contexts add up, and cancelled entries are dropped") {
    // Tokens 1 and 2 share index 5. Around token 0 they have the same weight.
    val indices = (t: Int) => if (t == 0) 0 else 5
    val sameSign = encode(Seq(1, 0, 2), constant(1), indexHash = indices)
    assert(sameSign.toMap.apply(0) === Map(5 -> 2.0))

    val oppositeSigns = encode(Seq(1, 0, 2), constant(1), indexHash = indices, signHash = t => t)
    assert(oppositeSigns.toMap.apply(0) === Map.empty[Int, Double])
  }

  test("negative index hashes map to non-negative indices") {
    val contexts = encode(Seq(1, 2), constant(1), indexHash = t => -t)
    assert(contexts === Seq(1 -> Map(6 -> 1.0), 2 -> Map(7 -> 1.0)))
  }

  test("sign comes from the lowest bit of the sign hash") {
    Seq(1 -> 1.0, -1 -> 1.0, Int.MaxValue -> 1.0, 2 -> -1.0, 0 -> -1.0, Int.MinValue -> -1.0)
      .foreach { case (hash, sign) =>
        val contexts = encode(Seq(1, 2), constant(1), signHash = _ => hash)
        assert(contexts.toMap.apply(1) === Map(2 -> sign), s"sign hash $hash")
      }
  }

  test("decay functions weight contexts by distance") {
    decayFunctions.foreach { name =>
      val weights = Hash2Vec.decayWeights(name, 2.0, 4)
      // The context of token 0 holds tokens 1 to 4 at distances 1 to 4; token 5 is too far.
      val contexts = encode(Seq(0, 1, 2, 3, 4, 5), weights)
      assert(contexts.toMap.apply(0) === (1 to 4).map(d => d -> weights(d)).toMap, name)
      assert(contexts === reference(Seq(0, 1, 2, 3, 4, 5), weights), name)
    }
  }

  test("encoder can be reused and grows past its initial capacity") {
    val weights = Hash2Vec.decayWeights("gaussian", 2.0, 3)
    val encoder = new Hash2Vec.ContextEncoder(vectorSize, weights)
    val long = Seq.tabulate(200)(i => (i * 7) % 13)
    val short = Seq(4, 2, 4)
    Seq(short, long, short, Seq.empty[Int], long).foreach { tokens =>
      assert(encode(tokens, weights, encoder = Some(encoder)) === reference(tokens, weights))
    }
  }

  test("random sequences match the reference") {
    val random = new Random(42)
    for {
      name <- decayFunctions
      windowSize <- 1 to 4
      _ <- 1 to 20
    } {
      val weights = Hash2Vec.decayWeights(name, 1.5, windowSize)
      val tokens = Seq.fill(random.nextInt(100))(random.nextInt(20))
      val indexHash = (t: Int) => t * 31 - 7
      val signHash = (t: Int) => t / 3
      assert(
        encode(tokens, weights, indexHash, signHash) ===
          reference(tokens, weights, indexHash, signHash),
        s"$name, window $windowSize")
    }
  }
}
